from __future__ import annotations

from pathlib import Path

import codex_orchestrator.agent_guidance as agent_guidance


def test_commit_guidance_skips_when_present(tmp_path: Path) -> None:
    (tmp_path / "AGENTS.md").write_text(
        "## Commit Messages\nUse `(feat): example (prescribed by bead bd-1)`\n",
        encoding="utf-8",
    )

    result = agent_guidance.inspect_commit_message_guidance(repo_root=tmp_path)

    assert result.guidance_present is True
    assert result.note is None
    assert result.next_action is None


def test_commit_guidance_reports_next_action_when_missing(tmp_path: Path) -> None:
    (tmp_path / "AGENTS.md").write_text("Placeholder", encoding="utf-8")

    result = agent_guidance.inspect_commit_message_guidance(repo_root=tmp_path)

    assert result.guidance_present is False
    assert result.note == "Commit message guidance missing from AGENTS.md."
    assert result.next_action is not None
    assert agent_guidance.COMMIT_GUIDANCE_SNIPPET in result.next_action


def test_commit_guidance_defaults_to_agents_md_when_missing(tmp_path: Path) -> None:
    result = agent_guidance.inspect_commit_message_guidance(repo_root=tmp_path)

    assert result.guidance_present is False
    assert result.agents_path == tmp_path / "AGENTS.md"
    assert result.note == "Commit message guidance missing from AGENTS.md."
