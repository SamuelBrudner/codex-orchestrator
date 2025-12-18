from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

NotebookOutputPolicy = Literal["keep", "strip"]


class RepoConfigError(ValueError):
    pass


_REQUIRED_ORCHESTRATOR_OUTPUT_ROOTS: tuple[Path, ...] = (
    Path(".beads"),
    Path("docs/runs"),
)


def _within(path: Path, root: Path) -> bool:
    if root == Path(".") or root == Path():
        return True
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _first_covering_root(target: Path, roots: tuple[Path, ...]) -> Path | None:
    for root in roots:
        if _within(target, root):
            return root
    return None


def _validate_required_output_root(
    *,
    repo_id: str,
    required_root: Path,
    allowed_roots: tuple[Path, ...],
    deny_roots: tuple[Path, ...],
    errors: list[str],
) -> None:
    deny_match = _first_covering_root(required_root, deny_roots)
    if deny_match is not None:
        errors.append(
            f"repos.{repo_id}.deny_roots: must not cover orchestrator output "
            f"{required_root.as_posix()!r} (denied by {deny_match.as_posix()!r})"
        )

    allow_match = _first_covering_root(required_root, allowed_roots)
    if allow_match is None:
        errors.append(
            f"repos.{repo_id}.allowed_roots: must include orchestrator output "
            f"{required_root.as_posix()!r} (or a parent like '.' or {required_root.parent.as_posix()!r})"
        )


def _validate_orchestrator_outputs_policy(
    *,
    repo_id: str,
    allowed_roots: tuple[Path, ...],
    deny_roots: tuple[Path, ...],
    errors: list[str],
) -> None:
    for required_root in _REQUIRED_ORCHESTRATOR_OUTPUT_ROOTS:
        _validate_required_output_root(
            repo_id=repo_id,
            required_root=required_root,
            allowed_roots=allowed_roots,
            deny_roots=deny_roots,
            errors=errors,
        )


def _toml_load(path: Path) -> dict[str, Any]:
    try:
        import tomllib  # pyright: ignore[reportMissingImports]
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib  # type: ignore[no-redef]

    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError as e:
        raise RepoConfigError(f"Config file not found: {path}") from e
    except OSError as e:
        raise RepoConfigError(f"Failed to read config file: {path}") from e
    except Exception as e:  # tomllib.TOMLDecodeError is not public across tomli/tomllib
        raise RepoConfigError(f"Failed to parse TOML in {path}: {e}") from e

    if not isinstance(data, dict):
        raise RepoConfigError(f"Expected TOML document to be a table in {path}")
    return data


def _as_str(
    value: Any,
    *,
    field: str,
    errors: list[str],
    required: bool = False,
) -> str | None:
    if value is None:
        if required:
            errors.append(f"{field}: required field missing")
        return None
    if not isinstance(value, str):
        errors.append(f"{field}: expected string, got {type(value).__name__}")
        return None
    if not value.strip():
        errors.append(f"{field}: must be non-empty")
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


def _as_rel_paths(
    value: Any,
    *,
    field: str,
    default: tuple[Path, ...],
    errors: list[str],
) -> tuple[Path, ...]:
    items = _as_str_list(value, field=field, errors=errors)
    if items is None:
        return default

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


def _validate_repo_groups(
    repo_ids: set[str],
    repo_groups: dict[str, tuple[str, ...]],
    *,
    errors: list[str],
) -> None:
    for group_name, members in sorted(repo_groups.items()):
        for member in members:
            if member not in repo_ids:
                errors.append(
                    f"repo_groups.{group_name}: unknown repo_id {member!r} "
                    f"(known: {', '.join(sorted(repo_ids)) or '<none>'})"
                )


@dataclass(frozen=True, slots=True)
class RepoPolicy:
    repo_id: str
    path: Path
    base_branch: str
    env: str | None
    notebook_roots: tuple[Path, ...]
    allowed_roots: tuple[Path, ...]
    deny_roots: tuple[Path, ...]
    validation_commands: tuple[str, ...]
    notebook_output_policy: NotebookOutputPolicy


@dataclass(frozen=True, slots=True)
class RepoInventory:
    repos: dict[str, RepoPolicy]
    repo_groups: dict[str, tuple[str, ...]]

    def list_repos(self) -> list[RepoPolicy]:
        return [self.repos[repo_id] for repo_id in sorted(self.repos)]

    def select_repo_ids(
        self,
        *,
        repo_ids: Sequence[str] | None = None,
        repo_groups: Sequence[str] | None = None,
    ) -> list[str]:
        repo_ids = [r for r in (repo_ids or ()) if r]
        repo_groups = [g for g in (repo_groups or ()) if g]

        if not repo_ids and not repo_groups:
            return sorted(self.repos)

        unknown_repo_ids = sorted({r for r in repo_ids if r not in self.repos})
        if unknown_repo_ids:
            raise RepoConfigError(
                "Unknown repo_id(s): "
                + ", ".join(repr(r) for r in unknown_repo_ids)
                + " (known: "
                + ", ".join(sorted(self.repos))
                + ")"
            )

        unknown_group_names = sorted({g for g in repo_groups if g not in self.repo_groups})
        if unknown_group_names:
            raise RepoConfigError(
                "Unknown repo_group(s): "
                + ", ".join(repr(g) for g in unknown_group_names)
                + " (known: "
                + ", ".join(sorted(self.repo_groups) or ["<none>"])
                + ")"
            )

        selected: set[str] = set(repo_ids)
        for group_name in repo_groups:
            selected.update(self.repo_groups.get(group_name, ()))
        return sorted(selected)

    def select_repos(
        self,
        *,
        repo_ids: Sequence[str] | None = None,
        repo_groups: Sequence[str] | None = None,
    ) -> list[RepoPolicy]:
        selected_repo_ids = self.select_repo_ids(repo_ids=repo_ids, repo_groups=repo_groups)
        return [self.repos[repo_id] for repo_id in selected_repo_ids]


def load_repo_inventory(config_path: Path) -> RepoInventory:
    data = _toml_load(config_path)

    allowed_top_level = {"repos", "repo_groups"}
    unknown_top_level = set(data) - allowed_top_level
    errors: list[str] = []
    if unknown_top_level:
        errors.append(
            f"Top-level: unknown keys {sorted(unknown_top_level)} "
            f"(allowed: {sorted(allowed_top_level)})"
        )

    repos_table = data.get("repos")
    if not isinstance(repos_table, dict):
        errors.append("repos: required table missing or malformed (expected [repos.<repo_id>])")
        raise RepoConfigError("Invalid config:\n- " + "\n- ".join(errors))

    repos: dict[str, RepoPolicy] = {}
    for repo_id in sorted(repos_table):
        repo_data = repos_table.get(repo_id)
        if not isinstance(repo_data, dict):
            errors.append(f"repos.{repo_id}: expected table, got {type(repo_data).__name__}")
            continue

        known_fields = {
            "path",
            "base_branch",
            "env",
            "notebook_roots",
            "allowed_roots",
            "deny_roots",
            "validation_commands",
            "notebook_output_policy",
        }
        unknown_fields = set(repo_data) - known_fields
        if unknown_fields:
            errors.append(
                f"repos.{repo_id}: unknown keys {sorted(unknown_fields)} "
                f"(allowed: {sorted(known_fields)})"
            )

        path_str = _as_str(
            repo_data.get("path"),
            field=f"repos.{repo_id}.path",
            errors=errors,
            required=True,
        )
        base_branch = _as_str(
            repo_data.get("base_branch"),
            field=f"repos.{repo_id}.base_branch",
            errors=errors,
            required=True,
        )
        env = _as_str(repo_data.get("env"), field=f"repos.{repo_id}.env", errors=errors)

        notebook_roots = _as_rel_paths(
            repo_data.get("notebook_roots"),
            field=f"repos.{repo_id}.notebook_roots",
            default=(Path("."),),
            errors=errors,
        )
        allowed_roots = _as_rel_paths(
            repo_data.get("allowed_roots"),
            field=f"repos.{repo_id}.allowed_roots",
            default=(Path("."),),
            errors=errors,
        )
        deny_roots = _as_rel_paths(
            repo_data.get("deny_roots"),
            field=f"repos.{repo_id}.deny_roots",
            default=(),
            errors=errors,
        )

        validation_commands_raw = _as_str_list(
            repo_data.get("validation_commands"),
            field=f"repos.{repo_id}.validation_commands",
            errors=errors,
        )
        validation_commands: tuple[str, ...] = tuple(validation_commands_raw or ())

        policy_raw = _as_str(
            repo_data.get("notebook_output_policy"),
            field=f"repos.{repo_id}.notebook_output_policy",
            errors=errors,
        )
        notebook_output_policy: NotebookOutputPolicy = "strip"
        if policy_raw is not None:
            if policy_raw not in ("strip", "keep"):
                errors.append(
                    f"repos.{repo_id}.notebook_output_policy: expected 'strip' or 'keep', "
                    f"got {policy_raw!r}"
                )
            else:
                notebook_output_policy = policy_raw  # type: ignore[assignment]

        if path_str is None or base_branch is None:
            continue

        repo_path = Path(path_str).expanduser()
        if not repo_path.is_absolute():
            errors.append(f"repos.{repo_id}.path: must be an absolute path, got {path_str!r}")
            continue
        if not repo_path.exists():
            errors.append(f"repos.{repo_id}.path: does not exist: {path_str!r}")
            continue
        if not repo_path.is_dir():
            errors.append(f"repos.{repo_id}.path: must be a directory, got {path_str!r}")
            continue

        repos[repo_id] = RepoPolicy(
            repo_id=repo_id,
            path=repo_path,
            base_branch=base_branch,
            env=env,
            notebook_roots=notebook_roots,
            allowed_roots=allowed_roots,
            deny_roots=deny_roots,
            validation_commands=validation_commands,
            notebook_output_policy=notebook_output_policy,
        )
        _validate_orchestrator_outputs_policy(
            repo_id=repo_id,
            allowed_roots=allowed_roots,
            deny_roots=deny_roots,
            errors=errors,
        )

    repo_groups_table = data.get("repo_groups", {})
    repo_groups: dict[str, tuple[str, ...]] = {}
    if repo_groups_table is not None and not isinstance(repo_groups_table, dict):
        errors.append(
            f"repo_groups: expected table ([repo_groups]) got {type(repo_groups_table).__name__}"
        )
    elif isinstance(repo_groups_table, dict):
        for group_name in sorted(repo_groups_table):
            members = _as_str_list(
                repo_groups_table.get(group_name),
                field=f"repo_groups.{group_name}",
                errors=errors,
            )
            repo_groups[group_name] = tuple(members or ())

    _validate_repo_groups(set(repos), repo_groups, errors=errors)

    if errors:
        raise RepoConfigError("Invalid config:\n- " + "\n- ".join(errors))

    return RepoInventory(repos=repos, repo_groups=repo_groups)
