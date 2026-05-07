from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

from codex_orchestrator import __version__
from codex_orchestrator.cli_common import format_bool as _format_bool
from codex_orchestrator.cli_common import load_current_run_id as _load_current_run_id
from codex_orchestrator.cli_common import load_enforced_ai_settings as _load_enforced_ai_settings
from codex_orchestrator.cli_exec_repo import cmd_exec_repo as _cmd_exec_repo
from codex_orchestrator.cli_init_repo import cmd_init_repo as _cmd_init_repo
from codex_orchestrator.cli_overlay import cmd_overlay_apply as _cmd_overlay_apply
from codex_orchestrator.cli_overlay import cmd_overlay_dry_run as _cmd_overlay_dry_run
from codex_orchestrator.cli_run import cmd_run as _cmd_run
from codex_orchestrator.cli_run_info import cmd_run_info as _cmd_run_info
from codex_orchestrator.cli_signoff import cmd_signoff as _cmd_signoff
from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir
from codex_orchestrator.run_closure_review import (
    RunClosureReviewError,
    run_review_only_codex_pass,
    write_final_review,
)
from codex_orchestrator.run_lifecycle import RunLifecycleError, tick_run


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
                    run_review_only_codex_pass(
                        paths,
                        run_id=result.run_id,
                        ai_settings=ai_settings,
                        repo_config_path=Path("config/repos.toml"),
                    )
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
        help="Max repos to run in parallel (defaults to $MAX_PARALLEL or auto).",
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
        "--review-every-beads",
        type=int,
        default=None,
        help="Run a review-only Codex pass after N beads are attempted (does not end the run).",
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
    run_parser.add_argument(
        "--focus",
        default=None,
        help="Natural language focus area for the run (filters planned beads and guides Codex execution).",
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
    exec_repo_parser.add_argument(
        "--focus",
        default=None,
        help="Natural language focus area for the run (filters planned beads and guides Codex execution).",
    )
    exec_repo_parser.set_defaults(func=_cmd_exec_repo)

    init_repo_parser = subparsers.add_parser(
        "init-repo",
        help="Initialize a new repository so it can participate in orchestrator runs.",
    )
    init_repo_parser.add_argument("--repo-id", required=True, help="Repo ID to create in config/repos.toml")
    init_repo_parser.add_argument(
        "--path",
        required=True,
        help="Path to the target repository (absolute or relative to current working directory).",
    )
    init_repo_parser.add_argument(
        "--env",
        required=True,
        help="Default conda env name for this repo (written to repos.toml and overlay defaults).",
    )
    init_repo_parser.add_argument(
        "--base-branch",
        default=None,
        help="Base branch for run/<RUN_ID> branches (defaults to current git branch).",
    )
    init_repo_parser.add_argument(
        "--validation-command",
        action="append",
        default=None,
        help="Validation command to add (repeatable).",
    )
    init_repo_parser.add_argument(
        "--time-budget-minutes",
        type=int,
        default=45,
        help="Default per-bead time budget (minutes).",
    )
    init_repo_parser.add_argument(
        "--allow-env-creation",
        action="store_true",
        help="Write allow_env_creation=true in overlay defaults when missing.",
    )
    init_repo_parser.add_argument(
        "--requires-notebook-execution",
        action="store_true",
        help="Write requires_notebook_execution=true in overlay defaults when missing.",
    )
    init_repo_parser.add_argument(
        "--allow-existing",
        action="store_true",
        help="Keep an existing repos.<repo_id> entry and only bootstrap overlay/beads.",
    )
    init_repo_parser.set_defaults(func=_cmd_init_repo)

    signoff_parser = subparsers.add_parser(
        "signoff",
        help="Create run signoff artifacts (run_signoff.json/md) for an ended run.",
    )
    signoff_parser.add_argument(
        "--run-id",
        default=None,
        help="RUN_ID to sign off (defaults to latest ended run).",
    )
    signoff_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    signoff_parser.add_argument(
        "--reviewer",
        default=None,
        help="Reviewer identity (required; or set $CODEX_ORCHESTRATOR_REVIEWER).",
    )
    signoff_parser.add_argument(
        "--notes",
        default=None,
        help="Optional notes to include in run_signoff.md.",
    )
    signoff_parser.set_defaults(func=_cmd_signoff)

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

    run_info_parser = subparsers.add_parser(
        "run-info",
        help="List recent runs or inspect one run's debugging artifacts.",
    )
    run_info_parser.add_argument(
        "--run-id",
        default=None,
        help="Inspect a specific RUN_ID (if omitted, lists recent runs).",
    )
    run_info_parser.add_argument(
        "--latest",
        action="store_true",
        help="Inspect the most recent RUN_ID under cache/runs.",
    )
    run_info_parser.add_argument(
        "--repo-id",
        action="append",
        default=None,
        help="Filter inspected repo summaries by repo_id (repeatable; requires --run-id/--latest).",
    )
    run_info_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum runs to list when no --run-id/--latest is provided.",
    )
    run_info_parser.add_argument(
        "--tail-lines",
        type=int,
        default=0,
        help="Include trailing log lines for run/repo logs (requires --run-id/--latest).",
    )
    run_info_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    run_info_parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON output.",
    )
    run_info_parser.set_defaults(func=_cmd_run_info)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
