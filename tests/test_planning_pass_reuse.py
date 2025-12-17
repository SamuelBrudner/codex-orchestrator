from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import (
    ReadyBead,
    ValidationResult,
    build_run_deck,
    plan_deck_items,
    write_run_deck,
)
from codex_orchestrator.planning_pass import PlanningPassError, ensure_repo_run_deck
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
    monkeypatch.setattr(planning_pass, "build_planning_audit", _fail)
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

    audit_json_path = paths.repo_planning_audit_json_path("run-123", "test_repo")
    audit_md_path = paths.repo_planning_audit_md_path("run-123", "test_repo")
    assert audit_json_path.exists() is False
    assert audit_md_path.exists() is False


def test_planning_pass_writes_planning_audit_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "models.py").write_text(
        "from dataclasses import dataclass\n\n@dataclass\nclass Thing:\n    x: int\n",
        encoding="utf-8",
    )

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
    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "run-456"
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    import codex_orchestrator.planning_pass as planning_pass

    def _bd_init(*, repo_root: Path) -> None:
        return None

    def _bd_list_ids(*, repo_root: Path) -> set[str]:
        return {"bd-1"}

    def _bd_ready(*, repo_root: Path) -> list[ReadyBead]:
        return [ReadyBead(bead_id="bd-1", title="My bead")]

    def _run_validations(
        commands,
        *,
        cwd: Path,
        timeout_seconds: float = 900.0,
        output_limit_chars: int = 20_000,
    ) -> dict[str, ValidationResult]:
        return {
            cmd: ValidationResult(command=cmd, exit_code=0, started_at=now, finished_at=now)
            for cmd in commands
        }

    monkeypatch.setattr(planning_pass, "bd_init", _bd_init)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _bd_list_ids)
    monkeypatch.setattr(planning_pass, "bd_ready", _bd_ready)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)

    result = ensure_repo_run_deck(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=False,
        now=now,
    )

    assert result.reused_existing_deck is False
    assert result.deck_path.exists()
    assert result.planning is not None

    audit_json_path = paths.repo_planning_audit_json_path(run_id, "test_repo")
    audit_md_path = paths.repo_planning_audit_md_path(run_id, "test_repo")
    assert audit_json_path.exists()
    assert audit_md_path.exists()

    audit = json.loads(audit_json_path.read_text(encoding="utf-8"))
    assert audit["run_id"] == run_id
    assert audit["repo_id"] == "test_repo"

    md = audit_md_path.read_text(encoding="utf-8")
    assert "# Planning Audit (test_repo)" in md


def test_planning_pass_fails_without_writing_deck_when_audit_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "run-789"
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    import codex_orchestrator.planning_pass as planning_pass

    def _bd_init(*, repo_root: Path) -> None:
        return None

    def _bd_list_ids(*, repo_root: Path) -> set[str]:
        return {"bd-1"}

    def _bd_ready(*, repo_root: Path) -> list[ReadyBead]:
        return [ReadyBead(bead_id="bd-1", title="My bead")]

    def _run_validations(
        commands,
        *,
        cwd: Path,
        timeout_seconds: float = 900.0,
        output_limit_chars: int = 20_000,
    ) -> dict[str, ValidationResult]:
        return {
            cmd: ValidationResult(command=cmd, exit_code=0, started_at=now, finished_at=now)
            for cmd in commands
        }

    def _build_audit_fail(*, run_id: str, repo_policy: RepoPolicy) -> dict[str, object]:
        raise RuntimeError("boom")

    monkeypatch.setattr(planning_pass, "bd_init", _bd_init)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _bd_list_ids)
    monkeypatch.setattr(planning_pass, "bd_ready", _bd_ready)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)
    monkeypatch.setattr(planning_pass, "build_planning_audit", _build_audit_fail)

    with pytest.raises(PlanningPassError):
        ensure_repo_run_deck(
            paths=paths,
            run_id=run_id,
            repo_policy=policy,
            overlay_path=overlay_path,
            replan=False,
            now=now,
        )

    assert paths.find_existing_run_deck_path(run_id, "test_repo") is None
    assert paths.repo_planning_audit_json_path(run_id, "test_repo").exists() is False
    assert paths.repo_planning_audit_md_path(run_id, "test_repo").exists() is False
