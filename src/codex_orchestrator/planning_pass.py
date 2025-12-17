from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from codex_orchestrator.audit_trail import write_json_atomic, write_text_atomic
from codex_orchestrator.beads_subprocess import bd_init, bd_list_ids, bd_ready
from codex_orchestrator.contract_overlays import load_contract_overlay
from codex_orchestrator.git_subprocess import GitError
from codex_orchestrator.notebook_refactor_issues import (
    NotebookRefactorResult,
    detect_changed_notebooks,
    ensure_notebook_refactor_issues,
)
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import (
    PlanningResult,
    RunDeck,
    build_run_deck,
    load_existing_run_deck,
    plan_deck_items,
    write_run_deck,
)
from codex_orchestrator.planning_audit import build_planning_audit, format_planning_audit_md
from codex_orchestrator.planning_audit_issues import create_planning_audit_issues
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


def _load_existing_created_issues(path: Path) -> list[dict[str, str]] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except OSError:
        return None
    except json.JSONDecodeError:
        return None

    if not isinstance(raw, dict):
        return None
    created = raw.get("created_issues")
    if not isinstance(created, list):
        return None

    out: list[dict[str, str]] = []
    for item in created:
        if not isinstance(item, dict):
            continue
        issue_id = item.get("id")
        title = item.get("title")
        if not isinstance(issue_id, str) or not issue_id.strip():
            continue
        if not isinstance(title, str) or not title.strip():
            continue
        out.append({"id": issue_id, "title": title})
    return out


def _collect_validation_commands(planning: PlanningResult) -> list[str]:
    commands: list[str] = []
    for item in planning.deck_items:
        commands.extend(item.contract.validation_commands)
    return commands


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

    overlay = load_contract_overlay(
        overlay_path,
        repo_policy=repo_policy,
        known_bead_ids=known_bead_ids,
    )

    notebook_changes: tuple[str, ...] = ()
    notebook_refactor: NotebookRefactorResult | None = None
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
    if enable_notebook_refactors and notebook_refactor_limit > 0 and notebook_changes:
        notebook_refactor = ensure_notebook_refactor_issues(
            repo_root=repo_policy.path,
            notebook_paths=notebook_changes,
            limit=notebook_refactor_limit,
            time_budget_minutes=overlay.defaults.time_budget_minutes,
            validation_commands=(
                tuple(repo_policy.validation_commands)
                + tuple(overlay.defaults.validation_commands or ())
            ),
            notebook_output_policy=repo_policy.notebook_output_policy,
            block_bead_ids=tuple(bead.bead_id for bead in ready_beads),
        )
        if notebook_refactor.issue_ids:
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
        env=_baseline_env(repo_policy, planning),
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
        audit["notebook_refactor"] = {
            "changed_notebooks": list(notebook_changes),
            "created_issues": [
                {"id": issue.issue_id, "title": issue.title}
                for issue in (notebook_refactor.created_issues if notebook_refactor else ())
            ],
            "enabled": enable_notebook_refactors,
            "limit": notebook_refactor_limit,
        }
        created_issues_payload: list[dict[str, str]] = []
        if is_first_planning_pass:
            enabled = bool(overlay.defaults.enable_planning_audit_issue_creation or False)
            limit = int(overlay.defaults.planning_audit_issue_limit or 0)
            if enabled and limit > 0:
                created = create_planning_audit_issues(
                    repo_root=repo_policy.path,
                    audit=audit,
                    limit=limit,
                )
                created_issues_payload = [
                    {"id": issue.issue_id, "title": issue.title} for issue in created
                ]
        else:
            existing = _load_existing_created_issues(planning_audit_json_path)
            if existing is not None:
                created_issues_payload = existing

        audit["created_issues"] = created_issues_payload
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
