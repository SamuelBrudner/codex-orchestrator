from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from codex_orchestrator.contract_overlays import ContractOverlay
from codex_orchestrator.repo_inventory import NotebookOutputPolicy, RepoPolicy


class ContractResolutionError(ValueError):
    pass


class ContractParseError(ValueError):
    pass


def _dedupe_preserve_order(items: Iterable[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        out.append(item)
        seen.add(item)
    return tuple(out)


@dataclass(frozen=True, slots=True)
class ResolvedExecutionContract:
    time_budget_minutes: int
    validation_commands: tuple[str, ...]
    env: str
    allow_env_creation: bool
    requires_notebook_execution: bool
    allowed_roots: tuple[Path, ...]
    deny_roots: tuple[Path, ...]
    notebook_roots: tuple[Path, ...]
    notebook_output_policy: NotebookOutputPolicy

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "time_budget_minutes": self.time_budget_minutes,
            "validation_commands": list(self.validation_commands),
            "env": self.env,
            "allow_env_creation": self.allow_env_creation,
            "requires_notebook_execution": self.requires_notebook_execution,
            "allowed_roots": [p.as_posix() for p in self.allowed_roots],
            "deny_roots": [p.as_posix() for p in self.deny_roots],
            "notebook_roots": [p.as_posix() for p in self.notebook_roots],
            "notebook_output_policy": self.notebook_output_policy,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> ResolvedExecutionContract:
        def _expect_str(value: Any, *, field: str) -> str:
            if not isinstance(value, str):
                raise ContractParseError(f"{field}: expected string, got {type(value).__name__}")
            if not value.strip():
                raise ContractParseError(f"{field}: must be non-empty")
            return value

        def _expect_bool(value: Any, *, field: str) -> bool:
            if not isinstance(value, bool):
                raise ContractParseError(f"{field}: expected bool, got {type(value).__name__}")
            return value

        def _expect_int(value: Any, *, field: str) -> int:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ContractParseError(f"{field}: expected int, got {type(value).__name__}")
            return value

        def _expect_str_list(value: Any, *, field: str) -> tuple[str, ...]:
            if not isinstance(value, list):
                raise ContractParseError(f"{field}: expected list[str], got {type(value).__name__}")
            out: list[str] = []
            for idx, item in enumerate(value):
                if not isinstance(item, str):
                    raise ContractParseError(
                        f"{field}[{idx}]: expected string, got {type(item).__name__}"
                    )
                out.append(item)
            return tuple(out)

        def _expect_path_list(value: Any, *, field: str) -> tuple[Path, ...]:
            raw = _expect_str_list(value, field=field)
            out: list[Path] = []
            for idx, item in enumerate(raw):
                p = Path(item)
                if p.is_absolute():
                    raise ContractParseError(f"{field}[{idx}]: must be relative, got {item!r}")
                if ".." in p.parts:
                    raise ContractParseError(f"{field}[{idx}]: must not contain '..', got {item!r}")
                out.append(p)
            return tuple(out)

        time_budget_minutes = _expect_int(data.get("time_budget_minutes"), field="time_budget_minutes")
        if time_budget_minutes <= 0:
            raise ContractParseError(
                f"time_budget_minutes: must be > 0, got {time_budget_minutes}"
            )

        validation_commands = _expect_str_list(data.get("validation_commands"), field="validation_commands")
        env = _expect_str(data.get("env"), field="env")
        allow_env_creation = _expect_bool(data.get("allow_env_creation"), field="allow_env_creation")
        requires_notebook_execution = _expect_bool(
            data.get("requires_notebook_execution"), field="requires_notebook_execution"
        )
        allowed_roots = _expect_path_list(data.get("allowed_roots"), field="allowed_roots")
        deny_roots = _expect_path_list(data.get("deny_roots"), field="deny_roots")
        notebook_roots = _expect_path_list(data.get("notebook_roots"), field="notebook_roots")
        notebook_output_policy = _expect_str(
            data.get("notebook_output_policy"), field="notebook_output_policy"
        )
        if notebook_output_policy not in {"strip", "keep"}:
            raise ContractParseError(
                "notebook_output_policy: expected 'strip' or 'keep', got "
                f"{notebook_output_policy!r}"
            )

        return cls(
            time_budget_minutes=time_budget_minutes,
            validation_commands=validation_commands,
            env=env,
            allow_env_creation=allow_env_creation,
            requires_notebook_execution=requires_notebook_execution,
            allowed_roots=allowed_roots,
            deny_roots=deny_roots,
            notebook_roots=notebook_roots,
            notebook_output_policy=notebook_output_policy,  # type: ignore[arg-type]
        )


def resolve_execution_contract(
    *,
    repo_policy: RepoPolicy,
    overlay: ContractOverlay,
    bead_id: str,
    overlay_path: Path | None = None,
) -> ResolvedExecutionContract:
    if overlay.repo_id != repo_policy.repo_id:
        raise ContractResolutionError(
            "Contract overlay repo_id mismatch: "
            f"overlay={overlay.repo_id!r} policy={repo_policy.repo_id!r}"
        )

    defaults = overlay.defaults
    per_bead = overlay.beads.get(bead_id)

    def _pick(field: str) -> Any:
        if per_bead is not None:
            value = getattr(per_bead, field)
            if value is not None:
                return value
        value = getattr(defaults, field)
        if value is not None:
            return value
        return None

    missing: list[str] = []

    time_budget_minutes = _pick("time_budget_minutes")
    if time_budget_minutes is None:
        missing.append("time_budget_minutes")

    allow_env_creation = _pick("allow_env_creation")
    if allow_env_creation is None:
        missing.append("allow_env_creation")

    requires_notebook_execution = _pick("requires_notebook_execution")
    if requires_notebook_execution is None:
        missing.append("requires_notebook_execution")

    env = _pick("env")
    if env is None:
        env = repo_policy.env
    if env is None:
        missing.append("env")

    allowed_roots = _pick("allowed_roots")
    if allowed_roots is None:
        allowed_roots = repo_policy.allowed_roots

    deny_roots = _pick("deny_roots")
    if deny_roots is None:
        deny_roots = repo_policy.deny_roots

    per_bead_validation_commands: tuple[str, ...] = ()
    if per_bead is not None and per_bead.validation_commands is not None:
        per_bead_validation_commands = per_bead.validation_commands
    validation_commands = _dedupe_preserve_order(
        list(repo_policy.validation_commands)
        + list(defaults.validation_commands or ())
        + list(per_bead_validation_commands)
    )

    if missing:
        missing_sorted = ", ".join(sorted(missing))
        overlay_hint = (
            overlay_path.as_posix()
            if overlay_path is not None
            else (Path("config") / "bead_contracts" / f"{repo_policy.repo_id}.toml").as_posix()
        )
        raise ContractResolutionError(
            "Unresolvable execution contract for "
            f"repo_id={repo_policy.repo_id!r} bead_id={bead_id!r}: missing {missing_sorted}. "
            f"Set these in {overlay_hint} under [defaults] or [beads.\"{bead_id}\"]"
        )

    return ResolvedExecutionContract(
        time_budget_minutes=int(time_budget_minutes),
        validation_commands=validation_commands,
        env=str(env),
        allow_env_creation=bool(allow_env_creation),
        requires_notebook_execution=bool(requires_notebook_execution),
        allowed_roots=tuple(allowed_roots),
        deny_roots=tuple(deny_roots),
        notebook_roots=repo_policy.notebook_roots,
        notebook_output_policy=repo_policy.notebook_output_policy,
    )
