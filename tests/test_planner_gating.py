from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import ReadyBead, build_run_deck, plan_deck_items, write_run_deck
from codex_orchestrator.repo_inventory import RepoPolicy


def _write_overlay(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _policy(*, tmp_path: Path) -> RepoPolicy:
    return RepoPolicy(
        repo_id="test_repo",
        path=tmp_path,
        base_branch="main",
        env="repo_env",
        notebook_roots=(Path("notebooks"),),
        allowed_roots=(Path("."),),
        deny_roots=(),
        validation_commands=("pytest -q",),
        notebook_output_policy="strip",
    )


def test_planner_skips_unresolvable_contract_and_logs_next_action(
    tmp_path: Path,
    caplog,
) -> None:
    overlay_path = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay_path,
        "\n".join(
            [
                "[defaults]",
                'env = "default_env"',
                "",
            ]
        ),
    )

    policy = _policy(tmp_path=tmp_path)
    beads = [ReadyBead(bead_id="bd-1", title="My bead")]

    with caplog.at_level(logging.ERROR):
        result = plan_deck_items(
            repo_policy=policy,
            overlay_path=overlay_path,
            ready_beads=beads,
            known_bead_ids={"bd-1"},
        )

    assert result.deck_items == ()
    assert len(result.skipped_beads) == 1
    assert result.skipped_beads[0].bead_id == "bd-1"
    assert overlay_path.as_posix() in result.skipped_beads[0].next_action
    assert "missing" in result.skipped_beads[0].next_action

    assert "Skipping bead bd-1" in caplog.text
    assert overlay_path.as_posix() in caplog.text


def test_planner_writes_deck_items_with_resolved_contract_snapshot(tmp_path: Path) -> None:
    overlay_path = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay_path,
        "\n".join(
            [
                "[defaults]",
                "time_budget_minutes = 45",
                "allow_env_creation = false",
                "requires_notebook_execution = false",
                'validation_commands = ["pytest -q"]',
                'env = "default_env"',
                "",
            ]
        ),
    )

    policy = _policy(tmp_path=tmp_path)
    planning = plan_deck_items(
        repo_policy=policy,
        overlay_path=overlay_path,
        ready_beads=[ReadyBead(bead_id="bd-1", title="My bead")],
        known_bead_ids={"bd-1"},
    )
    assert len(planning.deck_items) == 1

    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    deck = build_run_deck(run_id="run-123", repo_policy=policy, planning=planning, now=now)
    deck_json = deck.to_json_dict()

    assert deck_json["run_id"] == "run-123"
    assert deck_json["repo_id"] == "test_repo"
    assert len(deck_json["items"]) == 1
    assert deck_json["items"][0]["bead_id"] == "bd-1"
    assert deck_json["items"][0]["title"] == "My bead"
    assert deck_json["items"][0]["contract"]["env"] == "default_env"

    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    out_path = write_run_deck(paths, deck=deck)
    assert out_path.exists()

    written = json.loads(out_path.read_text(encoding="utf-8"))
    assert written["items"][0]["contract"]["env"] == "default_env"

