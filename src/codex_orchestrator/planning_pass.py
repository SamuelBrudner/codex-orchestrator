from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from codex_orchestrator.agent_guidance import ensure_commit_message_guidance_issue
from codex_orchestrator.audit_trail import write_json_atomic, write_text_atomic
from codex_orchestrator.beads_subprocess import BdCliError, bd_init, bd_list_ids, bd_ready, bd_show
from codex_orchestrator.contract_overlays import load_contract_overlay
from codex_orchestrator.env_bootstrap import bootstrap_repo_env
from codex_orchestrator.git_subprocess import GitError
from codex_orchestrator.notebook_refactor_issues import detect_changed_notebooks
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


def _baseline_env(repo_policy: RepoPolicy, planning: PlanningResult) -> str | None:
    if repo_policy.env is not None and repo_policy.env.strip():
        return repo_policy.env
    if planning.deck_items:
        return planning.deck_items[0].contract.env
    return None


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
    commit_guidance = ensure_commit_message_guidance_issue(repo_root=repo_policy.path)
    known_bead_ids = bd_list_ids(repo_root=repo_policy.path)
    ready_beads = _filter_ready_beads_by_live_status(
        repo_root=repo_policy.path,
        ready_beads=bd_ready(repo_root=repo_policy.path),
    )

    overlay = load_contract_overlay(
        overlay_path,
        repo_policy=repo_policy,
        known_bead_ids=known_bead_ids,
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

    enable_notebook_refactors = bool(
        overlay.defaults.enable_notebook_refactor_issue_creation or False
    )
    notebook_refactor_limit = int(overlay.defaults.notebook_refactor_issue_limit or 0)

    planning = plan_deck_items(
        repo_policy=repo_policy,
        overlay_path=overlay_path,
        ready_beads=ready_beads,
        known_bead_ids=known_bead_ids,
        focus=focus,
    )

    baseline_env = _baseline_env(repo_policy, planning)
    if baseline_env is not None and planning.deck_items:
        first_contract = planning.deck_items[0].contract
        logger.info(
            "Bootstrapping repo env=%s allow_env_creation=%s for repo_id=%s",
            baseline_env,
            first_contract.allow_env_creation,
            repo_policy.repo_id,
        )
        bootstrap_result = bootstrap_repo_env(
            env_name=baseline_env,
            repo_root=repo_policy.path,
            allow_env_creation=first_contract.allow_env_creation,
        )
        if bootstrap_result.error is not None:
            raise PlanningPassError(
                f"Env bootstrap failed for repo_id={repo_policy.repo_id!r}: {bootstrap_result.error}"
            )
        logger.info(
            "Env bootstrap complete: env_existed=%s env_created=%s repo_installed=%s",
            bootstrap_result.env_existed,
            bootstrap_result.env_created,
            bootstrap_result.repo_installed,
        )

    validation_commands = _collect_validation_commands(planning)
    baseline_results_by_command = run_validation_commands(
        validation_commands,
        cwd=repo_policy.path,
        env=baseline_env,
    )

    deck = build_run_deck(
        run_id=run_id,
        repo_policy=repo_policy,
        planning=planning,
        baseline_results_by_command=baseline_results_by_command,
        now=now,
    )

    planning_audit_json_path = paths.repo_planning_audit_json_path(run_id, repo_policy.repo_id)
    planning_audit_md_path = paths.repo_planning_audit_md_path(run_id, repo_policy.repo_id)
    is_first_planning_pass = not planning_audit_json_path.exists()
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
            "created_issues": [],
            "enabled": enable_notebook_refactors,
            "limit": notebook_refactor_limit,
        }
        del is_first_planning_pass
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
