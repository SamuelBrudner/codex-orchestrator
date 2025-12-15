from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from codex_orchestrator.beads_subprocess import bd_init, bd_list_ids, bd_ready
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import (
    PlanningResult,
    RunDeck,
    build_run_deck,
    load_existing_run_deck,
    plan_deck_items,
    write_run_deck,
)
from codex_orchestrator.repo_inventory import RepoPolicy
from codex_orchestrator.validation_runner import run_validation_commands

logger = logging.getLogger(__name__)


class PlanningPassError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RepoDeckPlan:
    repo_id: str
    deck: RunDeck
    deck_path: Path
    reused_existing_deck: bool
    planning: PlanningResult | None


def _collect_validation_commands(planning: PlanningResult) -> list[str]:
    commands: list[str] = []
    for item in planning.deck_items:
        commands.extend(item.contract.validation_commands)
    return commands


def ensure_repo_run_deck(
    *,
    paths: OrchestratorPaths,
    run_id: str,
    repo_policy: RepoPolicy,
    overlay_path: Path,
    replan: bool = False,
    now: datetime | None = None,
) -> RepoDeckPlan:
    if not replan:
        existing = load_existing_run_deck(paths, run_id=run_id, repo_id=repo_policy.repo_id)
        if existing is not None:
            existing_path = paths.find_existing_run_deck_path(run_id, repo_policy.repo_id)
            if existing_path is None:
                raise PlanningPassError(
                    "Loaded an existing run deck but could not locate its path: "
                    f"run_id={run_id!r} repo_id={repo_policy.repo_id!r}"
                )
            return RepoDeckPlan(
                repo_id=repo_policy.repo_id,
                deck=existing,
                deck_path=existing_path,
                reused_existing_deck=True,
                planning=None,
            )

    logger.info("Planning run deck for repo_id=%s", repo_policy.repo_id)
    bd_init(repo_root=repo_policy.path)
    known_bead_ids = bd_list_ids(repo_root=repo_policy.path)
    ready_beads = bd_ready(repo_root=repo_policy.path)

    planning = plan_deck_items(
        repo_policy=repo_policy,
        overlay_path=overlay_path,
        ready_beads=ready_beads,
        known_bead_ids=known_bead_ids,
    )

    validation_commands = _collect_validation_commands(planning)
    baseline_results_by_command = run_validation_commands(
        validation_commands,
        cwd=repo_policy.path,
    )

    deck = build_run_deck(
        run_id=run_id,
        repo_policy=repo_policy,
        planning=planning,
        baseline_results_by_command=baseline_results_by_command,
        now=now,
    )
    deck_path = write_run_deck(paths, deck=deck)

    return RepoDeckPlan(
        repo_id=repo_policy.repo_id,
        deck=deck,
        deck_path=deck_path,
        reused_existing_deck=False,
        planning=planning,
    )

