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

    def _ensure_commit_message_guidance_issue(*, repo_root: Path) -> None:
        return None

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


def test_planning_pass_creates_planning_audit_issues_once_when_enabled(
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

    import codex_orchestrator.planning_audit_issues as audit_issues
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

    created: list[tuple[str, str]] = []
    create_counter = {"n": 0}

    def _bd_list_open_titles(*, repo_root: Path) -> set[str]:
        return {"planning-audit(semantic_registry): Already exists"}

    def _bd_create(
        *, repo_root: Path, title: str, issue_type: str = "task", priority: int = 2
    ) -> BdIssue:
        create_counter["n"] += 1
        issue_id = f"bd-created-{create_counter['n']}"
        created.append((issue_id, title))
        return BdIssue(
            issue_id=issue_id,
            title=title,
            status="open",
            notes="",
            dependencies=(),
            dependents=(),
        )

    def _bd_update(*, repo_root: Path, issue_id: str, status=None, notes=None) -> BdIssue:
        matching = next((t for i, t in created if i == issue_id), "<unknown>")
        return BdIssue(
            issue_id=issue_id,
            title=matching,
            status="open",
            notes=str(notes or ""),
            dependencies=(),
            dependents=(),
        )

    monkeypatch.setattr(planning_pass, "bd_init", _bd_init)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _bd_list_ids)
    monkeypatch.setattr(planning_pass, "bd_ready", _bd_ready)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)
    monkeypatch.setattr(planning_pass, "build_planning_audit", _build_audit)

    monkeypatch.setattr(audit_issues, "bd_list_open_titles", _bd_list_open_titles)
    monkeypatch.setattr(audit_issues, "bd_create", _bd_create)
    monkeypatch.setattr(audit_issues, "bd_update", _bd_update)

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
    assert audit["created_issues"] == [
        {"id": "bd-created-1", "title": "planning-audit(semantic_registry): Create me next"},
        {
            "id": "bd-created-2",
            "title": "planning-audit(semantic_modeling_consistency): Create me too",
        },
    ]
    assert create_counter["n"] == 2

    ensure_repo_run_deck(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=True,
        now=now,
    )
    assert create_counter["n"] == 2


def test_planning_pass_creates_notebook_refactor_issues_when_enabled(
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

    import codex_orchestrator.notebook_refactor_issues as nb
    import codex_orchestrator.planning_pass as planning_pass

    def _bd_init(*, repo_root: Path) -> None:
        return None

    list_counter = {"n": 0}

    def _bd_list_ids(*, repo_root: Path) -> set[str]:
        list_counter["n"] += 1
        if list_counter["n"] >= 2:
            return {"bd-1", "bd-nb1"}
        return {"bd-1"}

    ready_counter = {"n": 0}

    def _bd_ready(*, repo_root: Path) -> list[ReadyBead]:
        ready_counter["n"] += 1
        if ready_counter["n"] >= 2:
            return [ReadyBead(bead_id="bd-nb1", title="notebook-refactor: notebooks/a.ipynb")]
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

    ensured: dict[str, object] = {}

    def _ensure_notebook_refactor_issues(
        *,
        repo_root: Path,
        notebook_paths,
        limit: int,
        time_budget_minutes,
        validation_commands,
        notebook_output_policy,
        block_bead_ids,
        label: str = "notebook-refactor",
    ) -> nb.NotebookRefactorResult:
        ensured["block_bead_ids"] = tuple(block_bead_ids)
        return nb.NotebookRefactorResult(
            notebook_paths=("notebooks/a.ipynb",),
            issue_ids=("bd-nb1",),
            created_issues=(
                nb.NotebookRefactorIssue(
                    issue_id="bd-nb1",
                    title="notebook-refactor: notebooks/a.ipynb",
                ),
            ),
        )

    monkeypatch.setattr(planning_pass, "bd_init", _bd_init)
    monkeypatch.setattr(planning_pass, "bd_list_ids", _bd_list_ids)
    monkeypatch.setattr(planning_pass, "bd_ready", _bd_ready)
    monkeypatch.setattr(planning_pass, "run_validation_commands", _run_validations)
    monkeypatch.setattr(planning_pass, "build_planning_audit", _build_audit)
    monkeypatch.setattr(planning_pass, "detect_changed_notebooks", _detect_changed_notebooks)
    monkeypatch.setattr(
        planning_pass, "ensure_notebook_refactor_issues", _ensure_notebook_refactor_issues
    )

    ensure_repo_run_deck(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        replan=False,
        now=now,
    )

    assert ensured["block_bead_ids"] == ("bd-1",)

    audit_json_path = paths.repo_planning_audit_json_path(run_id, "test_repo")
    audit = json.loads(audit_json_path.read_text(encoding="utf-8"))
    assert audit["notebook_refactor"] == {
        "changed_notebooks": ["notebooks/a.ipynb"],
        "created_issues": [
            {"id": "bd-nb1", "title": "notebook-refactor: notebooks/a.ipynb"},
        ],
        "enabled": True,
        "limit": 2,
    }
