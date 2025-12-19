from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codex_orchestrator.repo_inventory import RepoPolicy


class ContractOverlayError(ValueError):
    pass


def _toml_load(path: Path) -> dict[str, Any]:
    try:
        import tomllib  # pyright: ignore[reportMissingImports]
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib  # type: ignore[no-redef]

    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError as e:
        raise ContractOverlayError(f"Contract overlay not found: {path}") from e
    except OSError as e:
        raise ContractOverlayError(f"Failed to read contract overlay: {path}") from e
    except Exception as e:  # tomllib.TOMLDecodeError is not public across tomli/tomllib
        raise ContractOverlayError(f"Failed to parse TOML in {path}: {e}") from e

    if not isinstance(data, dict):
        raise ContractOverlayError(f"Expected TOML document to be a table in {path}")
    return data


def _as_str(value: Any, *, field: str, errors: list[str]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        errors.append(f"{field}: expected string, got {type(value).__name__}")
        return None
    if not value.strip():
        errors.append(f"{field}: must be non-empty")
        return None
    return value


def _as_bool(value: Any, *, field: str, errors: list[str]) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        errors.append(f"{field}: expected bool, got {type(value).__name__}")
        return None
    return value


def _as_int(value: Any, *, field: str, errors: list[str]) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        errors.append(f"{field}: expected int, got {type(value).__name__}")
        return None
    return value


def _as_str_list(value: Any, *, field: str, errors: list[str]) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        errors.append(f"{field}: expected list[str], got {type(value).__name__}")
        return None
    out: list[str] = []
    for idx, item in enumerate(value):
        if not isinstance(item, str):
            errors.append(f"{field}[{idx}]: expected string, got {type(item).__name__}")
            continue
        if not item.strip():
            errors.append(f"{field}[{idx}]: must be non-empty")
            continue
        out.append(item)
    return out


def _as_rel_paths(value: Any, *, field: str, errors: list[str]) -> tuple[Path, ...] | None:
    items = _as_str_list(value, field=field, errors=errors)
    if items is None:
        return None
    out: list[Path] = []
    for idx, item in enumerate(items):
        p = Path(item)
        if p.is_absolute():
            errors.append(f"{field}[{idx}]: must be a relative path, got {item!r}")
            continue
        if ".." in p.parts:
            errors.append(f"{field}[{idx}]: must not contain '..', got {item!r}")
            continue
        out.append(p)
    return tuple(out)


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

    time_budget = _as_int(
        table.get("time_budget_minutes"),
        field=f"{prefix}.time_budget_minutes",
        errors=errors,
    )
    if time_budget is not None and time_budget <= 0:
        errors.append(f"{prefix}.time_budget_minutes: must be > 0, got {time_budget}")
        time_budget = None

    validation_commands_raw = _as_str_list(
        table.get("validation_commands"),
        field=f"{prefix}.validation_commands",
        errors=errors,
    )
    validation_commands: tuple[str, ...] | None = (
        tuple(validation_commands_raw) if validation_commands_raw is not None else None
    )

    env = _as_str(table.get("env"), field=f"{prefix}.env", errors=errors)
    allow_env_creation = _as_bool(
        table.get("allow_env_creation"), field=f"{prefix}.allow_env_creation", errors=errors
    )
    requires_notebook_execution = _as_bool(
        table.get("requires_notebook_execution"),
        field=f"{prefix}.requires_notebook_execution",
        errors=errors,
    )
    enforce_given_when_then = _as_bool(
        table.get("enforce_given_when_then"),
        field=f"{prefix}.enforce_given_when_then",
        errors=errors,
    )
    enable_planning_audit_issue_creation = _as_bool(
        table.get("enable_planning_audit_issue_creation"),
        field=f"{prefix}.enable_planning_audit_issue_creation",
        errors=errors,
    )
    planning_audit_issue_limit = _as_int(
        table.get("planning_audit_issue_limit"),
        field=f"{prefix}.planning_audit_issue_limit",
        errors=errors,
    )
    if planning_audit_issue_limit is not None and planning_audit_issue_limit < 0:
        errors.append(
            f"{prefix}.planning_audit_issue_limit: must be >= 0, got {planning_audit_issue_limit}"
        )
        planning_audit_issue_limit = None
    enable_notebook_refactor_issue_creation = _as_bool(
        table.get("enable_notebook_refactor_issue_creation"),
        field=f"{prefix}.enable_notebook_refactor_issue_creation",
        errors=errors,
    )
    notebook_refactor_issue_limit = _as_int(
        table.get("notebook_refactor_issue_limit"),
        field=f"{prefix}.notebook_refactor_issue_limit",
        errors=errors,
    )
    if notebook_refactor_issue_limit is not None and notebook_refactor_issue_limit < 0:
        errors.append(
            f"{prefix}.notebook_refactor_issue_limit: must be >= 0, got {notebook_refactor_issue_limit}"
        )
        notebook_refactor_issue_limit = None
    allowed_roots = _as_rel_paths(
        table.get("allowed_roots"),
        field=f"{prefix}.allowed_roots",
        errors=errors,
    )
    deny_roots = _as_rel_paths(
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
