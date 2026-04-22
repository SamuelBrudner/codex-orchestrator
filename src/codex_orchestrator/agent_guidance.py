from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

COMMIT_GUIDANCE_TITLE = "Add commit message guidance to AGENTS.md"
COMMIT_GUIDANCE_SNIPPET = "(feat): <description of the work that was done> (prescribed by bead <ID>)"


@dataclass(frozen=True, slots=True)
class CommitGuidanceResult:
    agents_path: Path
    guidance_present: bool
    note: str | None
    next_action: str | None


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
            note=None,
            next_action=None,
        )

    return CommitGuidanceResult(
        agents_path=agents_path,
        guidance_present=False,
        note=f"Commit message guidance missing from {agents_path.name}.",
        next_action=(
            f"Add a Commit Messages section to {agents_path.name} using "
            f"`{COMMIT_GUIDANCE_SNIPPET}`."
        ),
    )
