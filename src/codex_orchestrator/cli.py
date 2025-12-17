from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from codex_orchestrator import __version__
from codex_orchestrator.ai_policy import AiPolicyError, AiSettings, enforce_unattended_ai_policy, load_ai_settings
from codex_orchestrator.beads_subprocess import BdCliError, bd_init, bd_list_ids, bd_ready
from codex_orchestrator.contract_overlays import (
    ContractOverlay,
    ContractOverlayError,
    ContractOverlayPatch,
    load_contract_overlay,
)
from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir
from codex_orchestrator.planner import plan_deck_items
from codex_orchestrator.repo_execution import (
    DiffCaps,
    RepoExecutionConfig,
    TickBudget,
    execute_repo_tick,
)
from codex_orchestrator.repo_inventory import RepoConfigError, load_repo_inventory
from codex_orchestrator.orchestrator_cycle import OrchestratorCycleError, run_orchestrator_cycle
from codex_orchestrator.run_closure_review import (
    RunClosureReviewError,
    run_review_only_codex_pass,
    write_final_review,
)
from codex_orchestrator.run_lifecycle import RunLifecycleError, tick_run


def _load_enforced_ai_settings() -> AiSettings:
    config_path = Path("config/orchestrator.toml")
    try:
        settings = load_ai_settings(config_path)
        enforce_unattended_ai_policy(settings, config_path=config_path)
    except AiPolicyError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e
    return settings


def _cmd_tick(args: argparse.Namespace) -> int:
    ai_settings = _load_enforced_ai_settings()
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    try:
        result = tick_run(
            paths=paths,
            mode=args.mode,
            actionable_work_found=bool(args.actionable_work_found),
            idle_ticks_to_end=int(args.idle_ticks_to_end),
            manual_ttl=timedelta(hours=float(args.manual_ttl_hours)),
        )
    except RunLifecycleError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e

    if result.ended:
        if result.run_id is not None:
            try:
                write_final_review(paths, run_id=result.run_id, ai_settings=ai_settings)
                if bool(args.final_review_codex):
                    run_review_only_codex_pass(paths, run_id=result.run_id, ai_settings=ai_settings)
            except RunClosureReviewError as e:
                raise SystemExit(f"codex-orchestrator: {e}") from e
        if result.run_id is None:
            print(f"status=skipped reason={result.end_reason}")
        else:
            print(f"RUN_ID={result.run_id} status=ended reason={result.end_reason}")
    else:
        tick_count = result.state.tick_count if result.state is not None else "?"
        print(
            f"RUN_ID={result.run_id} status=active tick={tick_count} "
            f"started_new={result.started_new}"
        )
    return 0


def _load_current_run_id(paths: OrchestratorPaths) -> str:
    try:
        data = json.loads(paths.current_run_path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise SystemExit(
            f"codex-orchestrator: no active run found at {paths.current_run_path}; "
            "run `codex-orchestrator tick --mode manual` first, or pass --run-id"
        ) from e
    except json.JSONDecodeError as e:
        raise SystemExit(
            f"codex-orchestrator: failed to parse {paths.current_run_path}: {e}"
        ) from e
    run_id = data.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise SystemExit(f"codex-orchestrator: {paths.current_run_path} missing run_id")
    return run_id


def _format_bool(v: bool) -> str:
    return "true" if v else "false"


def _list_planning_audit_repo_ids(run_dir: Path) -> list[str]:
    repo_ids: set[str] = set()
    for p in sorted(run_dir.glob("*.planning_audit.*")):
        name = p.name
        if ".planning_audit." not in name:
            continue
        repo_id = name.split(".planning_audit.", 1)[0].strip()
        if repo_id:
            repo_ids.add(repo_id)
    return sorted(repo_ids)


def _cmd_planning_audit(args: argparse.Namespace) -> int:
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = str(args.run_id) if args.run_id else _load_current_run_id(paths)
    repo_id = str(args.repo_id)

    if bool(args.no_meta) and args.dump is None:
        raise SystemExit("codex-orchestrator: --no-meta requires --dump md|json")

    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise SystemExit(
            f"codex-orchestrator: run dir not found: {run_dir} "
            "(check --run-id and --cache-dir)"
        )

    json_path = paths.repo_planning_audit_json_path(run_id, repo_id)
    md_path = paths.repo_planning_audit_md_path(run_id, repo_id)
    json_exists = json_path.exists()
    md_exists = md_path.exists()

    status: str
    if json_exists and md_exists:
        status = "ok"
    elif json_exists or md_exists:
        status = "partial"
    else:
        status = "missing"

    if not bool(args.no_meta):
        print(f"RUN_ID={run_id} repo_id={repo_id} status={status}")
        print(f"json_path={json_path.as_posix()} json_exists={_format_bool(json_exists)}")
        print(f"md_path={md_path.as_posix()} md_exists={_format_bool(md_exists)}")

    def emit_missing_error(*, missing: list[str]) -> int:
        missing_str = ",".join(missing) if missing else "<unknown>"
        known_repos = _list_planning_audit_repo_ids(run_dir)
        known_suffix = f" (available repo_ids: {', '.join(known_repos)})" if known_repos else ""

        out = sys.stderr if bool(args.no_meta) else sys.stdout
        print(f"error=planning_audit_missing missing={missing_str}{known_suffix}", file=out)
        print(
            "next_action="
            f"rerun planning for this repo (regenerates audit): "
            f"codex-orchestrator exec-repo --repo-id {repo_id} --run-id {run_id} --replan",
            file=out,
        )
        return 2

    if args.dump == "md":
        if not md_exists:
            return emit_missing_error(missing=["md"])
        if not bool(args.no_meta):
            print("")
        print(md_path.read_text(encoding="utf-8").rstrip("\n"))
        return 0

    if args.dump == "json":
        if not json_exists:
            return emit_missing_error(missing=["json"])
        if not bool(args.no_meta):
            print("")
        print(json_path.read_text(encoding="utf-8").rstrip("\n"))
        return 0

    if status != "ok" and not bool(args.allow_missing):
        missing: list[str] = []
        if not json_exists:
            missing.append("json")
        if not md_exists:
            missing.append("md")
        return emit_missing_error(missing=missing)

    return 0


def _cmd_exec_repo(args: argparse.Namespace) -> int:
    ai_settings = _load_enforced_ai_settings()
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = str(args.run_id) if args.run_id else _load_current_run_id(paths)

    try:
        inventory = load_repo_inventory(Path("config/repos.toml"))
    except RepoConfigError as e:
        raise SystemExit(f"codex-orchestrator: invalid config/repos.toml: {e}") from e

    repo_id = str(args.repo_id)
    policy = inventory.repos.get(repo_id)
    if policy is None:
        known = ", ".join(sorted(inventory.repos)) or "<none>"
        raise SystemExit(f"codex-orchestrator: unknown repo_id {repo_id!r} (known: {known})")

    overlay_path = Path("config/bead_contracts") / f"{repo_id}.toml"
    if not overlay_path.exists():
        raise SystemExit(f"codex-orchestrator: missing overlay {overlay_path}")

    tick_minutes = float(args.tick_minutes)
    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(minutes=tick_minutes))
    config = RepoExecutionConfig(
        tick_budget=timedelta(minutes=tick_minutes),
        min_minutes_to_start_new_bead=int(args.min_minutes_to_start_new_bead),
        max_beads_per_tick=int(args.max_beads_per_tick),
        diff_caps=DiffCaps(
            max_files_changed=int(args.diff_cap_files),
            max_lines_added=int(args.diff_cap_lines),
        ),
        replan=bool(args.replan),
        ai_settings=ai_settings,
    )

    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        tick=tick,
        config=config,
    )

    if result.skipped:
        print(f"repo_id={repo_id} status=skipped reason={result.skip_reason}")
    else:
        print(
            f"repo_id={repo_id} status=ok closed={result.beads_closed} "
            f"attempted={result.beads_attempted} stop_reason={result.stop_reason}"
        )
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    ai_settings = _load_enforced_ai_settings()
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()

    max_parallel: int
    if args.max_parallel is not None:
        max_parallel = int(args.max_parallel)
    else:
        raw = os.environ.get("MAX_PARALLEL", "1")
        try:
            max_parallel = int(raw)
        except ValueError as e:
            raise SystemExit(f"codex-orchestrator: invalid MAX_PARALLEL={raw!r} (expected int)") from e

    try:
        result = run_orchestrator_cycle(
            cache_dir=cache_dir,
            mode=args.mode,
            ai_settings=ai_settings,
            repo_config_path=Path("config/repos.toml"),
            overlays_dir=Path("config/bead_contracts"),
            repo_ids=args.repo_id,
            repo_groups=args.repo_group,
            max_parallel=max_parallel,
            tick_minutes=float(args.tick_minutes),
            idle_ticks_to_end=int(args.idle_ticks_to_end),
            manual_ttl_hours=float(args.manual_ttl_hours),
            min_minutes_to_start_new_bead=int(args.min_minutes_to_start_new_bead),
            max_beads_per_tick=int(args.max_beads_per_tick),
            diff_cap_files=int(args.diff_cap_files),
            diff_cap_lines=int(args.diff_cap_lines),
            replan=bool(args.replan),
            final_review_codex_review=bool(args.final_review_codex),
        )
    except (OrchestratorCycleError, RunLifecycleError) as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e

    ensure = result.ensure_result
    if ensure.ended:
        if ensure.run_id is None:
            print(f"status=skipped reason={ensure.end_reason}")
        else:
            print(f"RUN_ID={ensure.run_id} status=ended reason={ensure.end_reason}")
        return 0

    run_id = ensure.run_id
    assert run_id is not None
    tick_result = result.tick_result
    assert tick_result is not None

    if tick_result.ended:
        print(f"RUN_ID={run_id} status=ended reason={tick_result.end_reason}")
    else:
        tick_count = tick_result.state.tick_count if tick_result.state is not None else "?"
        print(f"RUN_ID={run_id} status=active tick={tick_count} started_new={ensure.started_new}")

    for repo_result in result.repo_results:
        if repo_result.skipped:
            print(f"repo_id={repo_result.repo_id} status=skipped reason={repo_result.skip_reason}")
        else:
            print(
                f"repo_id={repo_result.repo_id} status=ok attempted={repo_result.beads_attempted} "
                f"closed={repo_result.beads_closed} stop_reason={repo_result.stop_reason}"
            )
    return 0


def _toml_quote_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{escaped}"'


def _toml_quote_str_list(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(_toml_quote_string(v) for v in values) + "]"


def _toml_quote_path_list(values: tuple[Path, ...]) -> str:
    return _toml_quote_str_list(tuple(p.as_posix() for p in values))


def _render_overlay_patch_lines(patch: ContractOverlayPatch) -> list[str]:
    lines: list[str] = []
    if patch.time_budget_minutes is not None:
        lines.append(f"time_budget_minutes = {patch.time_budget_minutes}")
    if patch.validation_commands is not None:
        lines.append(f"validation_commands = {_toml_quote_str_list(patch.validation_commands)}")
    if patch.env is not None:
        lines.append(f"env = {_toml_quote_string(patch.env)}")
    if patch.allow_env_creation is not None:
        lines.append(f"allow_env_creation = {'true' if patch.allow_env_creation else 'false'}")
    if patch.requires_notebook_execution is not None:
        lines.append(
            "requires_notebook_execution = "
            f"{'true' if patch.requires_notebook_execution else 'false'}"
        )
    if patch.enable_planning_audit_issue_creation is not None:
        lines.append(
            "enable_planning_audit_issue_creation = "
            f"{'true' if patch.enable_planning_audit_issue_creation else 'false'}"
        )
    if patch.planning_audit_issue_limit is not None:
        lines.append(f"planning_audit_issue_limit = {patch.planning_audit_issue_limit}")
    if patch.allowed_roots is not None:
        lines.append(f"allowed_roots = {_toml_quote_path_list(patch.allowed_roots)}")
    if patch.deny_roots is not None:
        lines.append(f"deny_roots = {_toml_quote_path_list(patch.deny_roots)}")
    return lines


def _render_contract_overlay_toml(overlay: ContractOverlay) -> str:
    lines: list[str] = []
    lines.extend(
        [
            f"# Contract overlay for `{overlay.repo_id}`.",
            "#",
            "# `[defaults]` applies to all beads unless overridden.",
            "# `[beads.\"<BEAD_ID>\"]` defines per-bead overrides (keyed by Beads issue id).",
            "",
            "[defaults]",
        ]
    )
    defaults_lines = _render_overlay_patch_lines(overlay.defaults)
    if defaults_lines:
        lines.extend(defaults_lines)
    lines.append("")

    for bead_id in sorted(overlay.beads):
        patch = overlay.beads[bead_id]
        patch_lines = _render_overlay_patch_lines(patch)
        lines.append(f'[beads.{_toml_quote_string(bead_id)}]')
        lines.extend(patch_lines)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _cmd_overlay_dry_run(args: argparse.Namespace) -> int:
    try:
        inventory = load_repo_inventory(Path("config/repos.toml"))
    except RepoConfigError as e:
        raise SystemExit(f"codex-orchestrator: invalid config/repos.toml: {e}") from e

    repo_id = str(args.repo_id)
    repo_policy = inventory.repos.get(repo_id)
    if repo_policy is None:
        known = ", ".join(sorted(inventory.repos)) or "<none>"
        raise SystemExit(f"codex-orchestrator: unknown repo_id {repo_id!r} (known: {known})")

    overlay_path = Path("config/bead_contracts") / f"{repo_id}.toml"
    if not overlay_path.exists():
        print(f"repo_id={repo_id} status=missing_overlay overlay={overlay_path.as_posix()}")
        print(f"next_action=run `codex-orchestrator overlay apply --repo-id {repo_id}`")
        return 1

    try:
        bd_init(repo_root=repo_policy.path)
        known_bead_ids = bd_list_ids(repo_root=repo_policy.path)
        ready_beads = bd_ready(repo_root=repo_policy.path)
    except BdCliError as e:
        raise SystemExit(f"codex-orchestrator: bd error for repo_id={repo_id!r}: {e}") from e

    try:
        planning = plan_deck_items(
            repo_policy=repo_policy,
            overlay_path=overlay_path,
            ready_beads=ready_beads,
            known_bead_ids=known_bead_ids,
        )
    except ContractOverlayError as e:
        print(str(e).rstrip("\n"))
        print(f"next_action=fix {overlay_path.as_posix()} (then re-run dry-run)")
        return 1

    if planning.skipped_beads:
        print(
            f"repo_id={repo_id} status=missing_contract_fields "
            f"ready={len(ready_beads)} queued={len(planning.deck_items)} "
            f"skipped={len(planning.skipped_beads)}"
        )
        for bead in planning.skipped_beads:
            print(f"bead_id={bead.bead_id} title={bead.title!r}")
            print(f"next_action={bead.next_action}")
        return 1

    print(
        f"repo_id={repo_id} status=ok overlay={overlay_path.as_posix()} "
        f"ready={len(ready_beads)} queued={len(planning.deck_items)}"
    )
    return 0


def _cmd_overlay_apply(args: argparse.Namespace) -> int:
    try:
        inventory = load_repo_inventory(Path("config/repos.toml"))
    except RepoConfigError as e:
        raise SystemExit(f"codex-orchestrator: invalid config/repos.toml: {e}") from e

    repo_id = str(args.repo_id)
    repo_policy = inventory.repos.get(repo_id)
    if repo_policy is None:
        known = ", ".join(sorted(inventory.repos)) or "<none>"
        raise SystemExit(f"codex-orchestrator: unknown repo_id {repo_id!r} (known: {known})")

    overlay_path = Path("config/bead_contracts") / f"{repo_id}.toml"
    overlay_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        bd_init(repo_root=repo_policy.path)
        known_bead_ids = bd_list_ids(repo_root=repo_policy.path)
        ready_beads = bd_ready(repo_root=repo_policy.path)
    except BdCliError as e:
        raise SystemExit(f"codex-orchestrator: bd error for repo_id={repo_id!r}: {e}") from e

    overlay: ContractOverlay
    if overlay_path.exists():
        try:
            overlay = load_contract_overlay(
                overlay_path,
                repo_policy=repo_policy,
                known_bead_ids=known_bead_ids,
            )
        except ContractOverlayError as e:
            raise SystemExit(
                f"codex-orchestrator: cannot apply defaults to invalid overlay {overlay_path}: {e}"
            ) from e
    else:
        overlay = ContractOverlay(repo_id=repo_id, defaults=ContractOverlayPatch(), beads={})

    seed_env = str(args.env).strip() if args.env is not None else None
    env_to_write = overlay.defaults.env or seed_env or repo_policy.env
    if env_to_write is None:
        raise SystemExit(
            "codex-orchestrator: env is required for execution contract resolution. "
            f"Set repos.{repo_id}.env in config/repos.toml, or run "
            f"`codex-orchestrator overlay apply --repo-id {repo_id} --env <ENV_NAME>`"
        )

    time_budget_minutes = int(args.time_budget_minutes)
    if time_budget_minutes <= 0:
        raise SystemExit("codex-orchestrator: --time-budget-minutes must be > 0")

    defaults = overlay.defaults
    if defaults.time_budget_minutes is None:
        defaults = replace(defaults, time_budget_minutes=time_budget_minutes)
    if defaults.env is None:
        defaults = replace(defaults, env=env_to_write)
    if defaults.allow_env_creation is None:
        defaults = replace(defaults, allow_env_creation=bool(args.allow_env_creation))
    if defaults.requires_notebook_execution is None:
        defaults = replace(
            defaults, requires_notebook_execution=bool(args.requires_notebook_execution)
        )

    validation_commands: tuple[str, ...] | None = None
    if defaults.validation_commands is None:
        if args.validation_command:
            validation_commands = tuple(
                c for c in (str(item).strip() for item in args.validation_command) if c
            )
        elif repo_policy.validation_commands:
            validation_commands = repo_policy.validation_commands
        if validation_commands:
            defaults = replace(defaults, validation_commands=validation_commands)

    updated = ContractOverlay(repo_id=overlay.repo_id, defaults=defaults, beads=dict(overlay.beads))
    new_text = _render_contract_overlay_toml(updated)
    old_text = overlay_path.read_text(encoding="utf-8") if overlay_path.exists() else None
    if old_text != new_text:
        overlay_path.write_text(new_text, encoding="utf-8")

    # Confirm that ready beads have resolvable required fields.
    try:
        planning = plan_deck_items(
            repo_policy=repo_policy,
            overlay_path=overlay_path,
            ready_beads=ready_beads,
            known_bead_ids=known_bead_ids,
        )
    except ContractOverlayError as e:
        raise SystemExit(f"codex-orchestrator: wrote invalid overlay {overlay_path}: {e}") from e

    if planning.skipped_beads:
        print(
            f"repo_id={repo_id} status=written_but_incomplete overlay={overlay_path.as_posix()} "
            f"ready={len(ready_beads)} queued={len(planning.deck_items)} skipped={len(planning.skipped_beads)}"
        )
        for bead in planning.skipped_beads:
            print(f"bead_id={bead.bead_id} title={bead.title!r}")
            print(f"next_action={bead.next_action}")
        return 1

    print(
        f"repo_id={repo_id} status=ok overlay={overlay_path.as_posix()} "
        f"ready={len(ready_beads)} queued={len(planning.deck_items)}"
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="codex-orchestrator",
        description="Global Codex Orchestrator (work-in-progress).",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command")

    tick_parser = subparsers.add_parser("tick", help="Run one orchestrator tick.")
    tick_parser.add_argument(
        "--mode",
        choices=("automated", "manual"),
        default="automated",
        help="Run mode (scheduler=automated, roadtrip=manual).",
    )
    tick_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    tick_parser.add_argument(
        "--idle-ticks-to-end",
        type=int,
        default=3,
        help="End the run after N consecutive idle ticks.",
    )
    tick_parser.add_argument(
        "--manual-ttl-hours",
        type=float,
        default=12.0,
        help="Expiry TTL for manual runs (hours).",
    )
    tick_parser.add_argument(
        "--actionable-work-found",
        action="store_true",
        help="Record that actionable work was found this tick (resets idle counter).",
    )
    tick_parser.add_argument(
        "--final-review-codex",
        action="store_true",
        help="After ending a run, optionally run a review-only Codex pass (must produce zero diffs).",
    )
    tick_parser.set_defaults(func=_cmd_tick)

    run_parser = subparsers.add_parser("run", help="Execute one orchestrator cycle (all repos).")
    run_parser.add_argument(
        "--mode",
        choices=("automated", "manual"),
        default="automated",
        help="Run mode (scheduler=automated, roadtrip=manual).",
    )
    run_parser.add_argument("--cache-dir", default=None, help="Override orchestrator cache directory.")
    run_parser.add_argument(
        "--repo-id",
        action="append",
        default=None,
        help="Restrict to a specific repo_id from config/repos.toml (repeatable).",
    )
    run_parser.add_argument(
        "--repo-group",
        action="append",
        default=None,
        help="Restrict to a repo_group from config/repos.toml (repeatable).",
    )
    run_parser.add_argument(
        "--max-parallel",
        type=int,
        default=None,
        help="Max repos to run in parallel (defaults to $MAX_PARALLEL or 1).",
    )
    run_parser.add_argument("--tick-minutes", type=float, default=45.0, help="Tick budget in minutes.")
    run_parser.add_argument(
        "--idle-ticks-to-end",
        type=int,
        default=3,
        help="End the run after N consecutive idle ticks.",
    )
    run_parser.add_argument(
        "--manual-ttl-hours",
        type=float,
        default=12.0,
        help="Expiry TTL for manual runs (hours).",
    )
    run_parser.add_argument(
        "--min-minutes-to-start-new-bead",
        type=int,
        default=15,
        help="Do not start new beads if less than this remains.",
    )
    run_parser.add_argument(
        "--max-beads-per-tick",
        type=int,
        default=3,
        help="Cap beads attempted per repo per tick.",
    )
    run_parser.add_argument(
        "--diff-cap-files",
        type=int,
        default=25,
        help="Per-tick max files changed (sum across beads).",
    )
    run_parser.add_argument(
        "--diff-cap-lines",
        type=int,
        default=1500,
        help="Per-tick max lines added (sum across beads).",
    )
    run_parser.add_argument(
        "--replan",
        action="store_true",
        help="Recompute each repo run deck even if one already exists for this RUN_ID+repo_id.",
    )
    run_parser.add_argument(
        "--final-review-codex",
        action="store_true",
        help="After ending a run, optionally run a review-only Codex pass (must produce zero diffs).",
    )
    run_parser.set_defaults(func=_cmd_run)

    exec_repo_parser = subparsers.add_parser("exec-repo", help="Execute one repo deck tick.")
    exec_repo_parser.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    exec_repo_parser.add_argument("--run-id", default=None, help="Override RUN_ID (defaults to current_run.json)")
    exec_repo_parser.add_argument("--cache-dir", default=None, help="Override orchestrator cache directory.")
    exec_repo_parser.add_argument("--tick-minutes", type=float, default=45.0, help="Tick time budget in minutes.")
    exec_repo_parser.add_argument(
        "--min-minutes-to-start-new-bead",
        type=int,
        default=15,
        help="Do not start new beads if less than this remains.",
    )
    exec_repo_parser.add_argument(
        "--max-beads-per-tick",
        type=int,
        default=3,
        help="Cap beads attempted per repo per tick.",
    )
    exec_repo_parser.add_argument(
        "--diff-cap-files",
        type=int,
        default=25,
        help="Per-tick max files changed (sum across beads).",
    )
    exec_repo_parser.add_argument(
        "--diff-cap-lines",
        type=int,
        default=1500,
        help="Per-tick max lines added (sum across beads).",
    )
    exec_repo_parser.add_argument(
        "--replan",
        action="store_true",
        help="Recompute the run deck even if one already exists for this RUN_ID+repo_id.",
    )
    exec_repo_parser.set_defaults(func=_cmd_exec_repo)

    overlay_parser = subparsers.add_parser("overlay", help="Validate/generate contract overlays.")

    def _cmd_overlay_help(_: argparse.Namespace) -> int:
        overlay_parser.print_help()
        return 0

    overlay_parser.set_defaults(func=_cmd_overlay_help)

    overlay_subparsers = overlay_parser.add_subparsers(dest="overlay_command")

    overlay_dry_run = overlay_subparsers.add_parser(
        "dry-run",
        help="Validate overlay + report missing contract fields for ready beads.",
    )
    overlay_dry_run.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    overlay_dry_run.set_defaults(func=_cmd_overlay_dry_run)

    overlay_apply = overlay_subparsers.add_parser(
        "apply",
        help="Create/update config/bead_contracts/<repo_id>.toml with safe defaults.",
    )
    overlay_apply.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    overlay_apply.add_argument(
        "--time-budget-minutes",
        type=int,
        default=45,
        help="Default per-bead time budget (minutes).",
    )
    overlay_apply.add_argument(
        "--env",
        default=None,
        help="Override default env written to the overlay (defaults to repos.<repo_id>.env).",
    )
    overlay_apply.add_argument(
        "--allow-env-creation",
        action="store_true",
        help="Set allow_env_creation=true in [defaults] if missing.",
    )
    overlay_apply.add_argument(
        "--requires-notebook-execution",
        action="store_true",
        help="Set requires_notebook_execution=true in [defaults] if missing.",
    )
    overlay_apply.add_argument(
        "--validation-command",
        action="append",
        default=None,
        help="Set defaults.validation_commands if missing (repeatable).",
    )
    overlay_apply.set_defaults(func=_cmd_overlay_apply)

    planning_audit_parser = subparsers.add_parser(
        "planning-audit",
        help="Inspect planning audit artifacts for a RUN_ID + repo_id.",
    )
    planning_audit_parser.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    planning_audit_parser.add_argument(
        "--run-id",
        default=None,
        help="RUN_ID to inspect (defaults to current run).",
    )
    planning_audit_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    planning_audit_parser.add_argument(
        "--dump",
        choices=("md", "json"),
        default=None,
        help="Print the selected artifact contents to stdout.",
    )
    planning_audit_parser.add_argument(
        "--no-meta",
        action="store_true",
        help="Suppress path/existence lines (requires --dump).",
    )
    planning_audit_parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Exit 0 even if one/both artifacts are missing (metadata-only mode).",
    )
    planning_audit_parser.set_defaults(func=_cmd_planning_audit)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return int(args.func(args))
