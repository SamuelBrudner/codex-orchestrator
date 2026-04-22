from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from codex_orchestrator.beads_subprocess import BdIssue
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


@pytest.fixture(autouse=True)
def _stub_bootstrap_repo_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import codex_orchestrator.planning_pass as planning_pass
    from codex_orchestrator.env_bootstrap import BootstrapResult

    def _bootstrap_repo_env(*, env_name: str, repo_root: Path, allow_env_creation: bool) -> BootstrapResult:
        return BootstrapResult(
            env_name=env_name,
            env_existed=True,
            env_created=False,
            repo_installed=True,
            install_attempted=True,
            install_succeeded=True,
            error=None,
        )

    monkeypatch.setattr(planning_pass, "bootstrap_repo_env", _bootstrap_repo_env)


@pytest.fixture(autouse=True)
def _stub_commit_message_guidance(monkeypatch: pytest.MonkeyPatch) -> None:
    import codex_orchestrator.planning_pass as planning_pass
    from codex_orchestrator.agent_guidance import CommitGuidanceResult

    def _ensure_commit_message_guidance_issue(*, repo_root: Path) -> CommitGuidanceResult:
        return CommitGuidanceResult(
            agents_path=repo_root / "AGENTS.md",
            guidance_present=True,
            note=None,
            next_action=None,
        )

    monkeypatch.setattr(
        planning_pass,
        "ensure_commit_message_guidance_issue",
        _ensure_commit_message_guidance_issue,
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
        env: str | None = None,
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


def test_planning_pass_applies_focus_filter_to_run_deck(
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
    run_id = "run-focus-1"
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    import codex_orchestrator.planning_pass as planning_pass

    def _bd_init(*, repo_root: Path) -> None:
        return None

    def _bd_list_ids(*, repo_root: Path) -> set[str]:
        return {"bd-scope", "bd-other"}

    def _bd_ready(*, repo_root: Path) -> list[ReadyBead]:
        return [
            ReadyBead(
                bead_id="bd-scope",
                title="Simplify plume nav simulation logic",
                labels=("scope:plume-simplify",),
                description="Focus target.",
            ),
            ReadyBead(
                bead_id="bd-other",
                title="Update unrelated docs",
                labels=("docs",),
                description="Out of scope.",
            ),
        ]

    def _run_validations(
        commands,
        *,
        cwd: Path,
        env: str | None = None,
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
        focus="plume nav sim simplification",
        now=now,
    )

    assert [item.bead_id for item in result.deck.items] == ["bd-scope"]
    assert result.planning is not None
    assert len(result.planning.skipped_beads) == 1
    assert result.planning.skipped_beads[0].bead_id == "bd-other"
    assert "Excluded by focus filter" in result.planning.skipped_beads[0].next_action


def test_planning_pass_filters_ready_beads_with_non_open_live_status(
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
    run_id = "run-status-filter-1"
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    import codex_orchestrator.planning_pass as planning_pass

    def _bd_init(*, repo_root: Path) -> None:
        return None

    def _bd_list_ids(*, repo_root: Path) -> set[str]:
        return {"bd-open", "bd-in-progress", "bd-closed", "bd-blocked", "bd-canceled"}

    def _bd_ready(*, repo_root: Path) -> list[ReadyBead]:
        return [
            ReadyBead(bead_id="bd-open", title="Open bead"),
            ReadyBead(bead_id="bd-in-progress", title="In progress bead"),
            ReadyBead(bead_id="bd-closed", title="Closed bead"),
            ReadyBead(bead_id="bd-blocked", title="Blocked bead"),
            ReadyBead(bead_id="bd-canceled", title="Canceled bead"),
        ]

    statuses = {
        "bd-open": "open",
        "bd-in-progress": "in_progress",
        "bd-closed": "closed",
        "bd-blocked": "blocked",
        "bd-canceled": "canceled",
    }

    def _bd_show(*, repo_root: Path, issue_id: str) -> BdIssue:
        return BdIssue(
            issue_id=issue_id,
            title=issue_id,
            status=statuses[issue_id],
            notes="",
            dependencies=(),
            dependents=(),
        )

    def _run_validations(
        commands,
        *,
        cwd: Path,
        env: str | None = None,
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
    monkeypatch.setattr(planning_pass, "bd_show", _bd_show)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)

    result = ensure_repo_run_deck(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=False,
        now=now,
    )

    assert [item.bead_id for item in result.deck.items] == ["bd-open", "bd-in-progress"]


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
        env: str | None = None,
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


def test_planning_pass_keeps_created_issues_empty_when_issue_creation_is_enabled(
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
                "enable_planning_audit_issue_creation = true",
                "planning_audit_issue_limit = 2",
                "",
            ]
        ),
    )

    policy = _policy(tmp_path=tmp_path)
    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "run-issue-1"
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
        env: str | None = None,
        timeout_seconds: float = 900.0,
        output_limit_chars: int = 20_000,
    ) -> dict[str, ValidationResult]:
        return {
            cmd: ValidationResult(command=cmd, exit_code=0, started_at=now, finished_at=now)
            for cmd in commands
        }

    def _build_audit(*, run_id: str, repo_policy: RepoPolicy) -> dict[str, object]:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "repo_id": repo_policy.repo_id,
            "findings": [
                {
                    "category": "semantic_registry",
                    "title": "Already exists",
                    "severity": "high",
                    "confidence": "high",
                    "recommendation": "Do the thing",
                },
                {
                    "category": "semantic_registry",
                    "title": "Create me next",
                    "severity": "medium",
                    "confidence": "high",
                    "recommendation": "Do the next thing",
                },
                {
                    "category": "semantic_modeling_consistency",
                    "title": "Create me too",
                    "severity": "low",
                    "confidence": "medium",
                    "recommendation": "Do the other thing",
                },
            ],
            "summary": {"overall_severity": "high", "findings_count": 3},
        }

    monkeypatch.setattr(planning_pass, "bd_init", _bd_init)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _bd_list_ids)
    monkeypatch.setattr(planning_pass, "bd_ready", _bd_ready)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)
    monkeypatch.setattr(planning_pass, "build_planning_audit", _build_audit)

    ensure_repo_run_deck(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=False,
        now=now,
    )

    audit_json_path = paths.repo_planning_audit_json_path(run_id, "test_repo")
    audit = json.loads(audit_json_path.read_text(encoding="utf-8"))
    assert audit["created_issues"] == []


def test_planning_pass_records_notebook_changes_without_creating_issues(
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
                "enable_notebook_refactor_issue_creation = true",
                "notebook_refactor_issue_limit = 2",
                "",
            ]
        ),
    )

    policy = _policy(tmp_path=tmp_path)
    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "run-nb-1"
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    import codex_orchestrator.planning_pass as planning_pass

    def _bd_init(*, repo_root: Path) -> None:
        return None

    def _bd_list_ids(*, repo_root: Path) -> set[str]:
        return {"bd-1"}

    def _bd_ready(*, repo_root: Path) -> list[ReadyBead]:
        return [ReadyBead(bead_id="bd-1", title="Downstream bead")]

    def _run_validations(
        commands,
        *,
        cwd: Path,
        env: str | None = None,
        timeout_seconds: float = 900.0,
        output_limit_chars: int = 20_000,
    ) -> dict[str, ValidationResult]:
        return {
            cmd: ValidationResult(command=cmd, exit_code=0, started_at=now, finished_at=now)
            for cmd in commands
        }

    def _build_audit(*, run_id: str, repo_policy: RepoPolicy) -> dict[str, object]:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "repo_id": repo_policy.repo_id,
            "findings": [],
            "summary": {"overall_severity": "none", "findings_count": 0},
        }

    def _detect_changed_notebooks(
        *, repo_root: Path, notebook_roots: tuple[Path, ...]
    ) -> tuple[str, ...]:
        return ("notebooks/a.ipynb",)

    monkeypatch.setattr(planning_pass, "bd_init", _bd_init)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _bd_list_ids)
    monkeypatch.setattr(planning_pass, "bd_ready", _bd_ready)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)
    monkeypatch.setattr(planning_pass, "build_planning_audit", _build_audit)
    monkeypatch.setattr(planning_pass, "detect_changed_notebooks", _detect_changed_notebooks)

    ensure_repo_run_deck(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=False,
        now=now,
    )

    audit_json_path = paths.repo_planning_audit_json_path(run_id, "test_repo")
    audit = json.loads(audit_json_path.read_text(encoding="utf-8"))
    assert audit["notebook_refactor"] == {
        "changed_notebooks": ["notebooks/a.ipynb"],
        "created_issues": [],
        "enabled": True,
        "limit": 2,
    }


def test_planning_pass_records_commit_guidance_gap_in_audit(
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
    run_id = "run-guidance-1"
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    import codex_orchestrator.planning_pass as planning_pass
    from codex_orchestrator.agent_guidance import CommitGuidanceResult

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
        env: str | None = None,
        timeout_seconds: float = 900.0,
        output_limit_chars: int = 20_000,
    ) -> dict[str, ValidationResult]:
        return {
            cmd: ValidationResult(command=cmd, exit_code=0, started_at=now, finished_at=now)
            for cmd in commands
        }

    def _guidance(*, repo_root: Path) -> CommitGuidanceResult:
        return CommitGuidanceResult(
            agents_path=repo_root / "AGENTS.md",
            guidance_present=False,
            note="Commit message guidance missing from AGENTS.md.",
            next_action="Add a Commit Messages section to AGENTS.md.",
        )

    monkeypatch.setattr(planning_pass, "bd_init", _bd_init)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _bd_list_ids)
    monkeypatch.setattr(planning_pass, "bd_ready", _bd_ready)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)
    monkeypatch.setattr(planning_pass, "ensure_commit_message_guidance_issue", _guidance)

    ensure_repo_run_deck(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=False,
        now=now,
    )

    audit_json_path = paths.repo_planning_audit_json_path(run_id, "test_repo")
    audit = json.loads(audit_json_path.read_text(encoding="utf-8"))
    assert audit["commit_guidance"] == {
        "agents_path": "AGENTS.md",
        "guidance_present": False,
        "note": "Commit message guidance missing from AGENTS.md.",
        "next_action": "Add a Commit Messages section to AGENTS.md.",
    }
    assert "Commit message guidance missing from AGENTS.md." in audit["audit_notes"]
    assert "Add a Commit Messages section to AGENTS.md." in audit["next_actions"]
