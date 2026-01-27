from __future__ import annotations

from pathlib import Path

import codex_orchestrator.agent_guidance as agent_guidance
from codex_orchestrator.beads_subprocess import BdIssue


def test_commit_guidance_skips_when_present(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "AGENTS.md").write_text(
        "## Commit Messages\nUse `(feat): example (prescribed by bead bd-1)`\n",
        encoding="utf-8",
    )

    def _bd_list_open_titles(*, repo_root: Path) -> set[str]:
        raise AssertionError("bd_list_open_titles should not be called when guidance exists")

    def _bd_create(**_kwargs) -> BdIssue:
        raise AssertionError("bd_create should not be called when guidance exists")

    monkeypatch.setattr(agent_guidance, "bd_list_open_titles", _bd_list_open_titles)
    monkeypatch.setattr(agent_guidance, "bd_create", _bd_create)

    result = agent_guidance.ensure_commit_message_guidance_issue(repo_root=tmp_path)

    assert result.guidance_present is True
    assert result.created_issue is None
    assert result.issue_already_open is False


def test_commit_guidance_creates_issue_when_missing(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "AGENTS.md").write_text("Placeholder", encoding="utf-8")
    created: dict[str, str] = {}

    def _bd_list_open_titles(*, repo_root: Path) -> set[str]:
        return set()

    def _bd_create(
        *,
        repo_root: Path,
        title: str,
        issue_type: str = "task",
        priority: int = 2,
        labels=None,
        description=None,
        acceptance_criteria=None,
        design=None,
        estimate_minutes=None,
        deps=None,
    ) -> BdIssue:
        created["title"] = title
        return BdIssue(
            issue_id="bd-1",
            title=title,
            status="open",
            notes="",
            dependencies=(),
            dependents=(),
        )

    monkeypatch.setattr(agent_guidance, "bd_list_open_titles", _bd_list_open_titles)
    monkeypatch.setattr(agent_guidance, "bd_create", _bd_create)

    result = agent_guidance.ensure_commit_message_guidance_issue(repo_root=tmp_path)

    assert created["title"] == agent_guidance.COMMIT_GUIDANCE_TITLE
    assert result.guidance_present is False
    assert result.created_issue is not None
    assert result.created_issue.issue_id == "bd-1"


def test_commit_guidance_skips_when_issue_open(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "AGENTS.md").write_text("Placeholder", encoding="utf-8")

    def _bd_list_open_titles(*, repo_root: Path) -> set[str]:
        return {agent_guidance.COMMIT_GUIDANCE_TITLE}

    def _bd_create(**_kwargs) -> BdIssue:
        raise AssertionError("bd_create should not be called when issue already open")

    monkeypatch.setattr(agent_guidance, "bd_list_open_titles", _bd_list_open_titles)
    monkeypatch.setattr(agent_guidance, "bd_create", _bd_create)

    result = agent_guidance.ensure_commit_message_guidance_issue(repo_root=tmp_path)

    assert result.guidance_present is False
    assert result.issue_already_open is True
    assert result.created_issue is None
