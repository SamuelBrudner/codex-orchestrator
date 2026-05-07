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
from codex_orchestrator.toml_writers import render_contract_overlay_toml


def cmd_overlay_dry_run(args: argparse.Namespace) -> int:
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


def cmd_overlay_apply(args: argparse.Namespace) -> int:
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
    new_text = render_contract_overlay_toml(updated)
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
