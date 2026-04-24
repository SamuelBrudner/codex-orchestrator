from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from codex_orchestrator.beads_subprocess import BdCliError, bd_init, bd_list_ids, bd_ready
from codex_orchestrator.contract_overlays import (
    ContractOverlay,
    ContractOverlayError,
    ContractOverlayPatch,
    load_contract_overlay,
)
from codex_orchestrator.planner import plan_deck_items
from codex_orchestrator.repo_inventory import RepoConfigError, load_repo_inventory


def _toml_quote_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{escaped}"'


def _toml_quote_str_list(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(_toml_quote_string(v) for v in values) + "]"


def _toml_quote_path_list(values: tuple[Path, ...]) -> str:
    return _toml_quote_str_list(tuple(path.as_posix() for path in values))


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
    if patch.enforce_given_when_then is not None:
        lines.append(
            "enforce_given_when_then = "
            f"{'true' if patch.enforce_given_when_then else 'false'}"
        )
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
            defaults,
            requires_notebook_execution=bool(args.requires_notebook_execution),
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

    updated = ContractOverlay(
        repo_id=overlay.repo_id,
        defaults=defaults,
        beads=dict(overlay.beads),
    )
    new_text = _render_contract_overlay_toml(updated)
    old_text = overlay_path.read_text(encoding="utf-8") if overlay_path.exists() else None
    if old_text != new_text:
        overlay_path.write_text(new_text, encoding="utf-8")

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


def register_overlay_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
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
