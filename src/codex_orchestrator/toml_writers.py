from __future__ import annotations

from pathlib import Path
from typing import Any

import tomlkit

from codex_orchestrator.contract_overlays import ContractOverlay, ContractOverlayPatch


def _add_patch_fields(table: Any, patch: ContractOverlayPatch) -> None:
    if patch.time_budget_minutes is not None:
        table.add("time_budget_minutes", patch.time_budget_minutes)
    if patch.validation_commands is not None:
        table.add("validation_commands", list(patch.validation_commands))
    if patch.env is not None:
        table.add("env", patch.env)
    if patch.allow_env_creation is not None:
        table.add("allow_env_creation", patch.allow_env_creation)
    if patch.requires_notebook_execution is not None:
        table.add("requires_notebook_execution", patch.requires_notebook_execution)
    if patch.enforce_given_when_then is not None:
        table.add("enforce_given_when_then", patch.enforce_given_when_then)
    if patch.enable_planning_audit_issue_creation is not None:
        table.add("enable_planning_audit_issue_creation", patch.enable_planning_audit_issue_creation)
    if patch.planning_audit_issue_limit is not None:
        table.add("planning_audit_issue_limit", patch.planning_audit_issue_limit)
    if patch.enable_notebook_refactor_issue_creation is not None:
        table.add("enable_notebook_refactor_issue_creation", patch.enable_notebook_refactor_issue_creation)
    if patch.notebook_refactor_issue_limit is not None:
        table.add("notebook_refactor_issue_limit", patch.notebook_refactor_issue_limit)
    if patch.allowed_roots is not None:
        table.add("allowed_roots", [p.as_posix() for p in patch.allowed_roots])
    if patch.deny_roots is not None:
        table.add("deny_roots", [p.as_posix() for p in patch.deny_roots])


def render_contract_overlay_toml(overlay: ContractOverlay) -> str:
    doc = tomlkit.document()
    doc.add(tomlkit.comment(f"Contract overlay for `{overlay.repo_id}`."))
    doc.add(tomlkit.comment(""))
    doc.add(tomlkit.comment("`[defaults]` applies to all beads unless overridden."))
    doc.add(tomlkit.comment('`[beads."<BEAD_ID>"]` defines per-bead overrides (keyed by Beads issue id).'))
    doc.add(tomlkit.nl())

    defaults = tomlkit.table()
    _add_patch_fields(defaults, overlay.defaults)
    doc.add("defaults", defaults)

    if overlay.beads:
        beads = tomlkit.table()
        for bead_id in sorted(overlay.beads):
            patch_table = tomlkit.table()
            _add_patch_fields(patch_table, overlay.beads[bead_id])
            beads.add(bead_id, patch_table)
        doc.add("beads", beads)

    return tomlkit.dumps(doc).rstrip() + "\n"


def load_toml_document(path: Path) -> Any:
    try:
        return tomlkit.parse(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return tomlkit.document()
    except OSError as e:
        raise SystemExit(f"codex-orchestrator: failed to read {path}: {e}") from e
    except Exception as e:
        raise SystemExit(f"codex-orchestrator: failed to parse TOML in {path}: {e}") from e


def render_repo_inventory_entry_toml(
    *,
    repo_id: str,
    repo_path: Path,
    base_branch: str,
    env_name: str,
    validation_commands: tuple[str, ...],
    deny_roots: tuple[str, ...],
) -> str:
    doc = tomlkit.document()
    repos = tomlkit.table()
    entry = repo_inventory_entry_table(
        repo_path=repo_path,
        base_branch=base_branch,
        env_name=env_name,
        validation_commands=validation_commands,
        deny_roots=deny_roots,
    )
    repos.add(repo_id, entry)
    doc.add("repos", repos)
    return tomlkit.dumps(doc).rstrip()


def repo_inventory_entry_table(
    *,
    repo_path: Path,
    base_branch: str,
    env_name: str,
    validation_commands: tuple[str, ...],
    deny_roots: tuple[str, ...],
) -> Any:
    entry = tomlkit.table()
    entry.add("path", repo_path.as_posix())
    entry.add("base_branch", base_branch)
    entry.add("env", env_name)
    entry.add("notebook_roots", ["."])
    entry.add("allowed_roots", ["."])
    entry.add("deny_roots", list(deny_roots))
    entry.add("notebook_output_policy", "strip")
    if validation_commands:
        entry.add("validation_commands", list(validation_commands))
    return entry
