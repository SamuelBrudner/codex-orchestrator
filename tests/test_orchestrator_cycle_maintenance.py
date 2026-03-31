from __future__ import annotations

from pathlib import Path

import codex_orchestrator.orchestrator_cycle as orchestrator_cycle
from codex_orchestrator.beads_subprocess import (
    BdCapabilities,
    BdDoctorResult,
    BdSyncResult,
)
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.repo_inventory import RepoPolicy


def _policy(tmp_path: Path) -> RepoPolicy:
    return RepoPolicy(
        repo_id="test_repo",
        path=tmp_path / "repo",
        base_branch="main",
        env=None,
        notebook_roots=(Path("notebooks"),),
        allowed_roots=(Path("."),),
        deny_roots=(),
        validation_commands=("pytest -q",),
        notebook_output_policy="strip",
    )


def _read_log(paths: OrchestratorPaths, run_id: str) -> str:
    return paths.run_log_path(run_id).read_text(encoding="utf-8")


def test_attempt_beads_maintenance_logs_capabilities_and_nonfatal_statuses(
    tmp_path: Path, monkeypatch
) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "run-maint-1"
    policy = _policy(tmp_path)
    policy.path.mkdir(parents=True)

    capabilities = BdCapabilities(
        version="bd version 0.63.3",
        workspace_layout="jsonl_only",
        supports_bootstrap=True,
        supports_init_from_jsonl=True,
        supports_sync=False,
        supports_structured_doctor_output=None,
    )
    monkeypatch.setattr(orchestrator_cycle, "bd_detect_capabilities", lambda *, repo_root: capabilities)
    monkeypatch.setattr(
        orchestrator_cycle,
        "bd_doctor",
        lambda *, repo_root: BdDoctorResult(
            status="skipped",
            capabilities=capabilities,
            message="tracked_jsonl_without_active_database",
        ),
    )
    monkeypatch.setattr(
        orchestrator_cycle,
        "bd_sync",
        lambda *, repo_root: BdSyncResult(
            status="unsupported",
            capabilities=capabilities,
            message="sync_command_unavailable",
        ),
    )

    orchestrator_cycle._attempt_beads_maintenance(paths=paths, run_id=run_id, repos=(policy,))

    log = _read_log(paths, run_id)
    assert "beads_capabilities repo_id=test_repo" in log
    assert "workspace_layout=jsonl_only" in log
    assert "beads_doctor repo_id=test_repo status=skipped" in log
    assert "beads_sync repo_id=test_repo status=unsupported" in log


def test_attempt_beads_maintenance_logs_warn_and_ok(tmp_path: Path, monkeypatch) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "run-maint-2"
    policy = _policy(tmp_path)
    policy.path.mkdir(parents=True)

    capabilities = BdCapabilities(
        version="bd version 0.63.3",
        workspace_layout="modern_embeddeddolt",
        supports_bootstrap=True,
        supports_init_from_jsonl=True,
        supports_sync=False,
        supports_structured_doctor_output=True,
    )
    monkeypatch.setattr(orchestrator_cycle, "bd_detect_capabilities", lambda *, repo_root: capabilities)
    monkeypatch.setattr(
        orchestrator_cycle,
        "bd_doctor",
        lambda *, repo_root: BdDoctorResult(
            status="warn",
            capabilities=capabilities,
            overall_ok=False,
            failed_checks=2,
        ),
    )
    monkeypatch.setattr(
        orchestrator_cycle,
        "bd_sync",
        lambda *, repo_root: BdSyncResult(status="ok", capabilities=capabilities),
    )

    orchestrator_cycle._attempt_beads_maintenance(paths=paths, run_id=run_id, repos=(policy,))

    log = _read_log(paths, run_id)
    assert "beads_doctor repo_id=test_repo status=warn overall_ok=false failed_checks=2" in log
    assert "beads_sync repo_id=test_repo status=ok" in log
