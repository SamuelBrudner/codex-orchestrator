from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codex_orchestrator.config_parsing import (
    as_bool,
    as_int,
    as_rel_paths,
    as_str,
    as_str_list,
    load_toml_table,
)
from codex_orchestrator.repo_inventory import RepoPolicy


class ContractOverlayError(ValueError):
    pass


def _toml_load(path: Path) -> dict[str, Any]:
    return load_toml_table(
        path,
        error_type=ContractOverlayError,
        missing_message="Contract overlay not found: {path}",
        read_message="Failed to read contract overlay: {path}",
        parse_message="Failed to parse TOML in {path}: {error}",
        table_message="Expected TOML document to be a table in {path}",
    )


def _path_is_within(child: Path, parent: Path) -> bool:
    if parent == Path("."):
        return True
    return child == parent or child.is_relative_to(parent)


def _deny_root_covers(policy_root: Path, overlay_root: Path) -> bool:
    if overlay_root == Path("."):
        return True
    return policy_root == overlay_root or policy_root.is_relative_to(overlay_root)


@dataclass(frozen=True, slots=True)
class ContractOverlayPatch:
    time_budget_minutes: int | None = None
    validation_commands: tuple[str, ...] | None = None
    env: str | None = None
    allow_env_creation: bool | None = None
    requires_notebook_execution: bool | None = None
    enforce_given_when_then: bool | None = None
    enable_planning_audit_issue_creation: bool | None = None
    planning_audit_issue_limit: int | None = None
    enable_notebook_refactor_issue_creation: bool | None = None
    notebook_refactor_issue_limit: int | None = None
    allowed_roots: tuple[Path, ...] | None = None
    deny_roots: tuple[Path, ...] | None = None


@dataclass(frozen=True, slots=True)
class ContractOverlay:
    repo_id: str
    defaults: ContractOverlayPatch
    beads: dict[str, ContractOverlayPatch]


def _parse_patch(table: dict[str, Any], *, prefix: str, errors: list[str]) -> ContractOverlayPatch:
    known_fields = {
        "time_budget_minutes",
        "validation_commands",
        "env",
        "allow_env_creation",
        "requires_notebook_execution",
        "enforce_given_when_then",
        "enable_planning_audit_issue_creation",
        "planning_audit_issue_limit",
        "enable_notebook_refactor_issue_creation",
        "notebook_refactor_issue_limit",
        "allowed_roots",
        "deny_roots",
    }
    unknown_fields = set(table) - known_fields
    if unknown_fields:
        errors.append(
            f"{prefix}: unknown keys {sorted(unknown_fields)} (allowed: {sorted(known_fields)})"
        )

    time_budget = as_int(
        table.get("time_budget_minutes"),
        field=f"{prefix}.time_budget_minutes",
        errors=errors,
    )
    if time_budget is not None and time_budget <= 0:
        errors.append(f"{prefix}.time_budget_minutes: must be > 0, got {time_budget}")
        time_budget = None

    validation_commands_raw = as_str_list(
        table.get("validation_commands"),
        field=f"{prefix}.validation_commands",
        errors=errors,
    )
    validation_commands: tuple[str, ...] | None = (
        tuple(validation_commands_raw) if validation_commands_raw is not None else None
    )

    env = as_str(table.get("env"), field=f"{prefix}.env", errors=errors)
    allow_env_creation = as_bool(
        table.get("allow_env_creation"), field=f"{prefix}.allow_env_creation", errors=errors
    )
    requires_notebook_execution = as_bool(
        table.get("requires_notebook_execution"),
        field=f"{prefix}.requires_notebook_execution",
        errors=errors,
    )
    enforce_given_when_then = as_bool(
        table.get("enforce_given_when_then"),
        field=f"{prefix}.enforce_given_when_then",
        errors=errors,
    )
    enable_planning_audit_issue_creation = as_bool(
        table.get("enable_planning_audit_issue_creation"),
        field=f"{prefix}.enable_planning_audit_issue_creation",
        errors=errors,
    )
    planning_audit_issue_limit = as_int(
        table.get("planning_audit_issue_limit"),
        field=f"{prefix}.planning_audit_issue_limit",
        errors=errors,
    )
    if planning_audit_issue_limit is not None and planning_audit_issue_limit < 0:
        errors.append(
            f"{prefix}.planning_audit_issue_limit: must be >= 0, got {planning_audit_issue_limit}"
        )
        planning_audit_issue_limit = None
    enable_notebook_refactor_issue_creation = as_bool(
        table.get("enable_notebook_refactor_issue_creation"),
        field=f"{prefix}.enable_notebook_refactor_issue_creation",
        errors=errors,
    )
    notebook_refactor_issue_limit = as_int(
        table.get("notebook_refactor_issue_limit"),
        field=f"{prefix}.notebook_refactor_issue_limit",
        errors=errors,
    )
    if notebook_refactor_issue_limit is not None and notebook_refactor_issue_limit < 0:
        errors.append(
            f"{prefix}.notebook_refactor_issue_limit: must be >= 0, got {notebook_refactor_issue_limit}"
        )
        notebook_refactor_issue_limit = None
    allowed_roots = as_rel_paths(
        table.get("allowed_roots"),
        field=f"{prefix}.allowed_roots",
        errors=errors,
    )
    deny_roots = as_rel_paths(
        table.get("deny_roots"),
        field=f"{prefix}.deny_roots",
        errors=errors,
    )

    return ContractOverlayPatch(
        time_budget_minutes=time_budget,
        validation_commands=validation_commands,
        env=env,
        allow_env_creation=allow_env_creation,
        requires_notebook_execution=requires_notebook_execution,
        enforce_given_when_then=enforce_given_when_then,
        enable_planning_audit_issue_creation=enable_planning_audit_issue_creation,
        planning_audit_issue_limit=planning_audit_issue_limit,
        enable_notebook_refactor_issue_creation=enable_notebook_refactor_issue_creation,
        notebook_refactor_issue_limit=notebook_refactor_issue_limit,
        allowed_roots=allowed_roots,
        deny_roots=deny_roots,
    )


def load_contract_overlay(
    overlay_path: Path,
    *,
    repo_policy: RepoPolicy,
    known_bead_ids: set[str],
) -> ContractOverlay:
    data = _toml_load(overlay_path)

    allowed_top_level = {"defaults", "beads"}
    unknown_top_level = set(data) - allowed_top_level
    errors: list[str] = []
    if unknown_top_level:
        errors.append(
            f"Top-level: unknown keys {sorted(unknown_top_level)} "
            f"(allowed: {sorted(allowed_top_level)})"
        )

    defaults_table = data.get("defaults", {})
    if defaults_table is None:
        defaults_table = {}
    if not isinstance(defaults_table, dict):
        errors.append(
            f"defaults: expected table ([defaults]) got {type(defaults_table).__name__}"
        )
        defaults_table = {}

    beads_table = data.get("beads", {})
    if beads_table is None:
        beads_table = {}
    if not isinstance(beads_table, dict):
        errors.append(f"beads: expected table ([beads.<id>]) got {type(beads_table).__name__}")
        beads_table = {}

    defaults_patch = _parse_patch(defaults_table, prefix="defaults", errors=errors)
    if defaults_patch.allowed_roots is not None:
        for idx, root in enumerate(defaults_patch.allowed_roots):
            if not any(
                _path_is_within(root, policy_root) for policy_root in repo_policy.allowed_roots
            ):
                errors.append(
                    "defaults.allowed_roots: may only narrow repo policy "
                    f"(item {idx}={root.as_posix()!r} not within repo allowed_roots)"
                )
    if defaults_patch.deny_roots is not None:
        for policy_root in repo_policy.deny_roots:
            if not any(
                _deny_root_covers(policy_root, overlay_root)
                for overlay_root in defaults_patch.deny_roots
            ):
                errors.append(
                    "defaults.deny_roots: may not relax repo policy "
                    f"(missing coverage for {policy_root.as_posix()!r})"
                )

    bead_patches: dict[str, ContractOverlayPatch] = {}
    for bead_id in sorted(beads_table):
        if bead_id not in known_bead_ids:
            known_preview = ", ".join(sorted(known_bead_ids)[:10])
            suffix = "" if len(known_bead_ids) <= 10 else ", ..."
            errors.append(
                f'beads."{bead_id}": unknown bead id (known: {known_preview or "<none>"}{suffix})'
            )
            continue

        bead_table = beads_table.get(bead_id)
        if not isinstance(bead_table, dict):
            errors.append(
                f'beads."{bead_id}": expected table, got {type(bead_table).__name__}'
            )
            continue

        patch = _parse_patch(bead_table, prefix=f'beads."{bead_id}"', errors=errors)
        if patch.allowed_roots is not None:
            for idx, root in enumerate(patch.allowed_roots):
                if not any(
                    _path_is_within(root, policy_root) for policy_root in repo_policy.allowed_roots
                ):
                    errors.append(
                        f'beads."{bead_id}".allowed_roots: may only narrow repo policy '
                        f"(item {idx}={root.as_posix()!r} not within repo allowed_roots)"
                    )
        if patch.deny_roots is not None:
            for policy_root in repo_policy.deny_roots:
                if not any(
                    _deny_root_covers(policy_root, overlay_root)
                    for overlay_root in patch.deny_roots
                ):
                    errors.append(
                        f'beads."{bead_id}".deny_roots: may not relax repo policy '
                        f"(missing coverage for {policy_root.as_posix()!r})"
                    )

        bead_patches[bead_id] = patch

    if errors:
        raise ContractOverlayError("Invalid contract overlay:\n- " + "\n- ".join(errors))

    return ContractOverlay(repo_id=repo_policy.repo_id, defaults=defaults_patch, beads=bead_patches)
