from __future__ import annotations

from pathlib import Path

import pytest

from codex_orchestrator.ai_policy import (
    REQUIRED_CODEX_MODEL,
    REQUIRED_REASONING_EFFORT,
    AiPolicyError,
    enforce_unattended_ai_policy,
    load_ai_settings,
)


def test_load_ai_settings_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "orchestrator.toml"
    path.write_text(
        "\n".join(
            [
                "[ai]",
                f"model = {REQUIRED_CODEX_MODEL!r}",
                f"reasoning_effort = {REQUIRED_REASONING_EFFORT!r}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    settings = load_ai_settings(path)
    assert settings.model == REQUIRED_CODEX_MODEL
    assert settings.reasoning_effort == REQUIRED_REASONING_EFFORT


def test_enforce_unattended_ai_policy_rejects_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "orchestrator.toml"
    path.write_text(
        "\n".join(
            [
                "[ai]",
                'model = "not-gpt-5.3-codex"',
                'reasoning_effort = "low"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    settings = load_ai_settings(path)
    with pytest.raises(AiPolicyError, match="refusing to start"):
        enforce_unattended_ai_policy(settings, config_path=path)
