from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import (
    ReadyBead,
    ValidationResult,
    build_run_deck,
    plan_deck_items,
    write_run_deck,
)
from codex_orchestrator.planning_pass import ensure_repo_run_deck
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


def test_planning_pass_reuses_existing_deck(tmp_path: Path, monkeypatch) -> None:
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
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    baseline = ValidationResult(
        command="pytest -q",
        exit_code=0,
        started_at=now,
        finished_at=now,
    )
    deck = build_run_deck(
        run_id="run-123",
        repo_policy=policy,
        planning=planning,
        baseline_results_by_command={"pytest -q": baseline},
        now=now,
    )

    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    deck_path = write_run_deck(paths, deck=deck)

    import codex_orchestrator.planning_pass as planning_pass

    def _fail(*_args, **_kwargs):
        raise AssertionError("Planning pass recomputed scope unexpectedly.")

    monkeypatch.setattr(planning_pass, "bd_init", _fail)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _fail)
    monkeypatch.setattr(planning_pass, "bd_ready", _fail)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _fail)

    result = ensure_repo_run_deck(
        paths=paths,
        run_id="run-123",
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=False,
    )

    assert result.reused_existing_deck is True
    assert result.deck_path == deck_path
    assert result.deck.run_id == "run-123"
    assert result.planning is None
