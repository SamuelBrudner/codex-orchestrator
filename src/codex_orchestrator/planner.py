from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from codex_orchestrator.contract_overlays import load_contract_overlay
from codex_orchestrator.contracts import ContractResolutionError, ResolvedExecutionContract
from codex_orchestrator.contracts import resolve_execution_contract
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.repo_inventory import RepoPolicy

logger = logging.getLogger(__name__)


class PlannerError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReadyBead:
    bead_id: str
    title: str


@dataclass(frozen=True, slots=True)
class PlannedDeckItem:
    bead_id: str
    title: str
    contract: ResolvedExecutionContract


@dataclass(frozen=True, slots=True)
class SkippedBead:
    bead_id: str
    title: str
    next_action: str


@dataclass(frozen=True, slots=True)
class ValidationResult:
    command: str
    exit_code: int
    started_at: datetime
    finished_at: datetime
    stdout: str = ""
    stderr: str = ""

    def to_json_dict(self) -> dict[str, Any]:
        if self.started_at.tzinfo is None or self.finished_at.tzinfo is None:
            raise PlannerError("ValidationResult requires timezone-aware datetimes.")
        duration_seconds = (self.finished_at - self.started_at).total_seconds()
        return {
            "command": self.command,
            "ok": self.exit_code == 0,
            "exit_code": self.exit_code,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": duration_seconds,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> ValidationResult:
        command = data.get("command")
        if not isinstance(command, str) or not command.strip():
            raise PlannerError("ValidationResult.command must be a non-empty string.")

        exit_code = data.get("exit_code")
        if isinstance(exit_code, bool) or not isinstance(exit_code, int):
            raise PlannerError(
                "ValidationResult.exit_code must be an int, got "
                f"{type(exit_code).__name__}."
            )

        started_at_raw = data.get("started_at")
        finished_at_raw = data.get("finished_at")
        if not isinstance(started_at_raw, str) or not isinstance(finished_at_raw, str):
            raise PlannerError("ValidationResult started_at/finished_at must be ISO8601 strings.")

        try:
            started_at = datetime.fromisoformat(started_at_raw)
            finished_at = datetime.fromisoformat(finished_at_raw)
        except ValueError as e:
            raise PlannerError(f"ValidationResult has invalid datetime: {e}") from e

        stdout = data.get("stdout", "")
        stderr = data.get("stderr", "")
        if stdout is None:
            stdout = ""
        if stderr is None:
            stderr = ""
        if not isinstance(stdout, str) or not isinstance(stderr, str):
            raise PlannerError("ValidationResult stdout/stderr must be strings.")

        return cls(
            command=command,
            exit_code=exit_code,
            started_at=started_at,
            finished_at=finished_at,
            stdout=stdout,
            stderr=stderr,
        )


@dataclass(frozen=True, slots=True)
class RunDeckItem:
    bead_id: str
    title: str
    contract: ResolvedExecutionContract
    baseline_validation: tuple[ValidationResult, ...]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "bead_id": self.bead_id,
            "title": self.title,
            "contract": self.contract.to_json_dict(),
            "baseline_validation": [r.to_json_dict() for r in self.baseline_validation],
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> RunDeckItem:
        bead_id = data.get("bead_id")
        title = data.get("title")
        if not isinstance(bead_id, str) or not bead_id.strip():
            raise PlannerError("RunDeckItem.bead_id must be a non-empty string.")
        if not isinstance(title, str) or not title.strip():
            raise PlannerError("RunDeckItem.title must be a non-empty string.")

        contract_raw = data.get("contract")
        if not isinstance(contract_raw, dict):
            raise PlannerError("RunDeckItem.contract must be an object.")
        contract = ResolvedExecutionContract.from_json_dict(contract_raw)

        baseline_raw = data.get("baseline_validation", [])
        if baseline_raw is None:
            baseline_raw = []
        if not isinstance(baseline_raw, list):
            raise PlannerError("RunDeckItem.baseline_validation must be a list.")
        baseline: list[ValidationResult] = []
        for idx, item in enumerate(baseline_raw):
            if not isinstance(item, dict):
                raise PlannerError(
                    "RunDeckItem.baseline_validation items must be objects; "
                    f"got {type(item).__name__} at index {idx}."
                )
            baseline.append(ValidationResult.from_json_dict(item))

        return cls(
            bead_id=bead_id,
            title=title,
            contract=contract,
            baseline_validation=tuple(baseline),
        )


@dataclass(frozen=True, slots=True)
class PlanningResult:
    deck_items: tuple[PlannedDeckItem, ...]
    skipped_beads: tuple[SkippedBead, ...]


def plan_deck_items(
    *,
    repo_policy: RepoPolicy,
    overlay_path: Path,
    ready_beads: Sequence[ReadyBead],
    known_bead_ids: set[str] | None = None,
) -> PlanningResult:
    if known_bead_ids is None:
        known_bead_ids = {bead.bead_id for bead in ready_beads}

    overlay = load_contract_overlay(
        overlay_path,
        repo_policy=repo_policy,
        known_bead_ids=known_bead_ids,
    )

    deck_items: list[PlannedDeckItem] = []
    skipped: list[SkippedBead] = []
    for bead in ready_beads:
        try:
            contract = resolve_execution_contract(
                repo_policy=repo_policy,
                overlay=overlay,
                bead_id=bead.bead_id,
                overlay_path=overlay_path,
            )
        except ContractResolutionError as e:
            logger.error("Skipping bead %s (%s): %s", bead.bead_id, bead.title, e)
            skipped.append(
                SkippedBead(
                    bead_id=bead.bead_id,
                    title=bead.title,
                    next_action=str(e),
                )
            )
            continue

        deck_items.append(
            PlannedDeckItem(bead_id=bead.bead_id, title=bead.title, contract=contract)
        )

    return PlanningResult(deck_items=tuple(deck_items), skipped_beads=tuple(skipped))


@dataclass(frozen=True, slots=True)
class RunDeck:
    schema_version: int
    run_id: str
    repo_id: str
    created_at: datetime
    items: tuple[RunDeckItem, ...]

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "repo_id": self.repo_id,
            "created_at": self.created_at.isoformat(),
            "items": [item.to_json_dict() for item in self.items],
        }

    @classmethod
    def from_json_dict(cls, data: Mapping[str, Any]) -> RunDeck:
        schema_version = data.get("schema_version")
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise PlannerError("RunDeck.schema_version must be an int.")
        if schema_version != 2:
            raise PlannerError(
                f"Unsupported run deck schema_version={schema_version} (expected 2)."
            )

        run_id = data.get("run_id")
        repo_id = data.get("repo_id")
        created_at_raw = data.get("created_at")
        if not isinstance(run_id, str) or not run_id.strip():
            raise PlannerError("RunDeck.run_id must be a non-empty string.")
        if not isinstance(repo_id, str) or not repo_id.strip():
            raise PlannerError("RunDeck.repo_id must be a non-empty string.")
        if not isinstance(created_at_raw, str):
            raise PlannerError("RunDeck.created_at must be an ISO8601 string.")

        try:
            created_at = datetime.fromisoformat(created_at_raw)
        except ValueError as e:
            raise PlannerError(f"RunDeck.created_at has invalid datetime: {e}") from e

        items_raw = data.get("items", [])
        if items_raw is None:
            items_raw = []
        if not isinstance(items_raw, list):
            raise PlannerError("RunDeck.items must be a list.")
        items: list[RunDeckItem] = []
        for idx, item in enumerate(items_raw):
            if not isinstance(item, dict):
                raise PlannerError(
                    f"RunDeck.items[{idx}] must be an object, got {type(item).__name__}."
                )
            items.append(RunDeckItem.from_json_dict(item))

        return cls(
            schema_version=schema_version,
            run_id=run_id,
            repo_id=repo_id,
            created_at=created_at,
            items=tuple(items),
        )


def build_run_deck(
    *,
    run_id: str,
    repo_policy: RepoPolicy,
    planning: PlanningResult,
    baseline_results_by_command: Mapping[str, ValidationResult],
    now: datetime | None = None,
) -> RunDeck:
    if now is None:
        now = datetime.now().astimezone()
    if now.tzinfo is None:
        raise PlannerError("build_run_deck requires a timezone-aware now datetime.")

    missing: set[str] = set()
    items: list[RunDeckItem] = []
    for planned in planning.deck_items:
        baseline: list[ValidationResult] = []
        for command in planned.contract.validation_commands:
            result = baseline_results_by_command.get(command)
            if result is None:
                missing.add(command)
                continue
            baseline.append(result)
        items.append(
            RunDeckItem(
                bead_id=planned.bead_id,
                title=planned.title,
                contract=planned.contract,
                baseline_validation=tuple(baseline),
            )
        )
    if missing:
        missing_sorted = ", ".join(repr(cmd) for cmd in sorted(missing))
        raise PlannerError(
            f"Missing baseline validation results for commands: {missing_sorted}. "
            "Run baseline validations before writing the deck."
        )

    return RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id=repo_policy.repo_id,
        created_at=now,
        items=tuple(items),
    )


def _write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
        tmp_name = f.name
    os.replace(tmp_name, path)


def write_run_deck(paths: OrchestratorPaths, *, deck: RunDeck) -> Path:
    out_path = paths.run_deck_path(
        deck.run_id,
        deck.repo_id,
        day=deck.created_at,
    )
    _write_json_atomic(out_path, deck.to_json_dict())
    return out_path


def read_run_deck(path: Path) -> RunDeck:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        raise PlannerError(f"Run deck not found: {path}") from e
    except json.JSONDecodeError as e:
        raise PlannerError(f"Failed to parse run deck JSON in {path}: {e}") from e
    except OSError as e:
        raise PlannerError(f"Failed to read run deck: {path}: {e}") from e

    if not isinstance(data, dict):
        raise PlannerError(f"Expected run deck JSON object in {path}")
    return RunDeck.from_json_dict(data)


def load_existing_run_deck(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    repo_id: str,
) -> RunDeck | None:
    deck_path = paths.find_existing_run_deck_path(run_id, repo_id)
    if deck_path is None:
        return None
    return read_run_deck(deck_path)
