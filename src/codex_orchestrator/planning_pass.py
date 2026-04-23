from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from codex_orchestrator.agent_guidance import inspect_commit_message_guidance
from codex_orchestrator.audit_trail import write_json_atomic, write_text_atomic
from codex_orchestrator.beads_subprocess import BdCliError, bd_init, bd_list_ids, bd_ready, bd_show
from codex_orchestrator.git_subprocess import GitError
from codex_orchestrator.notebook_changes import detect_changed_notebooks
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import (
    PlanningResult,
    ReadyBead,
    RunDeck,
    build_run_deck,
    load_existing_run_deck,
    plan_deck_items,
    write_run_deck,
)
from codex_orchestrator.planning_audit import build_planning_audit, format_planning_audit_md
from codex_orchestrator.repo_inventory import RepoPolicy

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


def _filter_ready_beads_by_live_status(
    *,
    repo_root: Path,
    ready_beads: Sequence[ReadyBead],
) -> list[ReadyBead]:
    out: list[ReadyBead] = []
    for bead in ready_beads:
        try:
            issue = bd_show(repo_root=repo_root, issue_id=bead.bead_id)
        except BdCliError as e:
            logger.warning(
                "Skipping live status filter for bead_id=%s due to bd show failure: %s",
                bead.bead_id,
                e,
            )
            out.append(bead)
            continue

        if issue.status in {"open", "in_progress"}:
            out.append(bead)
            continue

        logger.info(
            "Dropping bead_id=%s from planning because live status=%s",
            bead.bead_id,
            issue.status,
        )
    return out


def ensure_repo_run_deck(
    *,
    paths: OrchestratorPaths,
    run_id: str,
    repo_policy: RepoPolicy,
    overlay_path: Path,
    replan: bool = False,
    focus: str | None = None,
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
    commit_guidance = inspect_commit_message_guidance(repo_root=repo_policy.path)
    known_bead_ids = bd_list_ids(repo_root=repo_policy.path)
    ready_beads = _filter_ready_beads_by_live_status(
        repo_root=repo_policy.path,
        ready_beads=bd_ready(repo_root=repo_policy.path),
    )

    notebook_changes: tuple[str, ...] = ()
    try:
        notebook_changes = detect_changed_notebooks(
            repo_root=repo_policy.path,
            notebook_roots=repo_policy.notebook_roots,
        )
    except GitError as e:
        logger.warning(
            "Notebook change detection skipped for repo_id=%s: %s",
            repo_policy.repo_id,
            e,
        )

    planning = plan_deck_items(
        repo_policy=repo_policy,
        overlay_path=overlay_path,
        ready_beads=ready_beads,
        known_bead_ids=known_bead_ids,
        focus=focus,
    )

    deck = build_run_deck(
        run_id=run_id,
        repo_policy=repo_policy,
        planning=planning,
        now=now,
    )

    planning_audit_json_path = paths.repo_planning_audit_json_path(run_id, repo_policy.repo_id)
    planning_audit_md_path = paths.repo_planning_audit_md_path(run_id, repo_policy.repo_id)
    try:
        audit = build_planning_audit(run_id=run_id, repo_policy=repo_policy)
        audit["commit_guidance"] = {
            "agents_path": commit_guidance.agents_path.name,
            "guidance_present": commit_guidance.guidance_present,
            "note": commit_guidance.note,
            "next_action": commit_guidance.next_action,
        }
        audit_notes = audit.get("audit_notes")
        if not isinstance(audit_notes, list):
            audit_notes = []
            audit["audit_notes"] = audit_notes
        next_actions = audit.get("next_actions")
        if not isinstance(next_actions, list):
            next_actions = []
            audit["next_actions"] = next_actions
        if commit_guidance.note:
            audit_notes.append(commit_guidance.note)
        if commit_guidance.next_action:
            next_actions.append(commit_guidance.next_action)
        audit["notebook_refactor"] = {
            "changed_notebooks": list(notebook_changes),
        }
        audit["created_issues"] = []
        write_json_atomic(planning_audit_json_path, audit)
        write_text_atomic(planning_audit_md_path, format_planning_audit_md(audit))
    except Exception as e:
        raise PlanningPassError(f"Planning audit generation failed: {e}") from e

    deck_path = write_run_deck(paths, deck=deck)

    return RepoDeckPlan(
        repo_id=repo_policy.repo_id,
        deck=deck,
        deck_path=deck_path,
        reused_existing_deck=False,
        planning=planning,
    )
