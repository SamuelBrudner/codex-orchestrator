from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

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
class SkippedBead:
    bead_id: str
    title: str
    next_action: str


@dataclass(frozen=True, slots=True)
class RunDeckItem:
    bead_id: str
    title: str
    contract: ResolvedExecutionContract

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "bead_id": self.bead_id,
            "title": self.title,
            "contract": self.contract.to_json_dict(),
        }


@dataclass(frozen=True, slots=True)
class PlanningResult:
    deck_items: tuple[RunDeckItem, ...]
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

    deck_items: list[RunDeckItem] = []
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

        deck_items.append(RunDeckItem(bead_id=bead.bead_id, title=bead.title, contract=contract))

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


def build_run_deck(
    *,
    run_id: str,
    repo_policy: RepoPolicy,
    planning: PlanningResult,
    now: datetime | None = None,
) -> RunDeck:
    if now is None:
        now = datetime.now().astimezone()
    if now.tzinfo is None:
        raise PlannerError("build_run_deck requires a timezone-aware now datetime.")

    return RunDeck(
        schema_version=1,
        run_id=run_id,
        repo_id=repo_policy.repo_id,
        created_at=now,
        items=planning.deck_items,
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

