from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import (
    ReadyBead,
    ValidationResult,
    build_run_deck,
    plan_deck_items,
    read_run_deck,
    write_run_deck,
)
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
    baseline = ValidationResult(
        command="pytest -q",
        exit_code=0,
        started_at=now,
        finished_at=now,
        stdout="",
        stderr="",
    )
    deck = build_run_deck(
        run_id="run-123",
        repo_policy=policy,
        planning=planning,
        baseline_results_by_command={"pytest -q": baseline},
        now=now,
    )
    deck_json = deck.to_json_dict()

    assert deck_json["run_id"] == "run-123"
    assert deck_json["repo_id"] == "test_repo"
    assert len(deck_json["items"]) == 1
    assert deck_json["items"][0]["bead_id"] == "bd-1"
    assert deck_json["items"][0]["title"] == "My bead"
    assert deck_json["items"][0]["contract"]["env"] == "default_env"
    assert deck_json["items"][0]["baseline_validation"][0]["command"] == "pytest -q"
    assert deck_json["items"][0]["baseline_validation"][0]["ok"] is True

    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    out_path = write_run_deck(paths, deck=deck)
    assert out_path.exists()

    written = read_run_deck(out_path)
    assert written.items[0].contract.env == "default_env"
    assert written.items[0].baseline_validation[0].command == "pytest -q"


def test_planner_focus_filters_ready_beads_into_deck(tmp_path: Path) -> None:
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
    beads = [
        ReadyBead(
            bead_id="bd-scope",
            title="Simplify plume nav simulation agent loop",
            labels=("scope:plume-simplify",),
            description="Reduce branching and duplicate state transitions in plume_nav_sim.",
        ),
        ReadyBead(
            bead_id="bd-other",
            title="Fix docs typos in unrelated package",
            labels=("docs",),
            description="Unrelated cleanup.",
        ),
    ]

    result = plan_deck_items(
        repo_policy=policy,
        overlay_path=overlay_path,
        ready_beads=beads,
        known_bead_ids={"bd-scope", "bd-other"},
        focus="plume nav sim simplification",
    )

    assert [item.bead_id for item in result.deck_items] == ["bd-scope"]
    assert len(result.skipped_beads) == 1
    assert result.skipped_beads[0].bead_id == "bd-other"
    assert "Excluded by focus filter" in result.skipped_beads[0].next_action
