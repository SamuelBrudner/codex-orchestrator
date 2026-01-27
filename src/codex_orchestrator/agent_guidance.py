from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from codex_orchestrator.beads_subprocess import BdIssue, bd_create, bd_list_open_titles

logger = logging.getLogger(__name__)

COMMIT_GUIDANCE_TITLE = "Add commit message guidance to AGENTS.md"
COMMIT_GUIDANCE_SNIPPET = "(feat): <description of the work that was done> (prescribed by bead <ID>)"


@dataclass(frozen=True, slots=True)
class CommitGuidanceResult:
    agents_path: Path
    guidance_present: bool
    issue_already_open: bool
    created_issue: BdIssue | None


def _find_agents_path(repo_root: Path) -> Path:
    for candidate in ("AGENTS.md", "AGENT.md"):
        path = repo_root / candidate
        if path.exists():
            return path
    return repo_root / "AGENTS.md"


def _guidance_present(text: str) -> bool:
    lowered = text.lower()
    if "prescribed by bead" in lowered:
        return True
    return "commit messages" in lowered and "commit message" in lowered


def ensure_commit_message_guidance_issue(*, repo_root: Path) -> CommitGuidanceResult:
    agents_path = _find_agents_path(repo_root)
    existing_text = ""
    if agents_path.exists():
        try:
            existing_text = agents_path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.warning("Failed to read %s: %s", agents_path, exc)

    if existing_text and _guidance_present(existing_text):
        return CommitGuidanceResult(
            agents_path=agents_path,
            guidance_present=True,
            issue_already_open=False,
            created_issue=None,
        )

    open_titles = bd_list_open_titles(repo_root=repo_root)
    if COMMIT_GUIDANCE_TITLE in open_titles:
        return CommitGuidanceResult(
            agents_path=agents_path,
            guidance_present=False,
            issue_already_open=True,
            created_issue=None,
        )

    description = (
        "Add commit message guidance to AGENTS.md (create the file if missing). "
        f"Required format: `{COMMIT_GUIDANCE_SNIPPET}`."
    )
    acceptance = (
        "- AGENTS.md includes a Commit Messages section.\n"
        f"- The section specifies `{COMMIT_GUIDANCE_SNIPPET}`.\n"
        "- The section includes a short example."
    )

    issue = bd_create(
        repo_root=repo_root,
        title=COMMIT_GUIDANCE_TITLE,
        issue_type="task",
        priority=2,
        description=description,
        acceptance_criteria=acceptance,
    )

    logger.info(
        "Created commit message guidance issue %s for repo_root=%s",
        issue.issue_id,
        repo_root,
    )

    return CommitGuidanceResult(
        agents_path=agents_path,
        guidance_present=False,
        issue_already_open=False,
        created_issue=issue,
    )
