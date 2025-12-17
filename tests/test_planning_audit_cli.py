from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _run_cli(*, cwd: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-m", "codex_orchestrator", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_planning_audit_cli_reports_paths_and_existence(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    run_id = "run-123"
    repo_id = "test_repo"

    run_dir = cache_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    md_path = run_dir / f"{repo_id}.planning_audit.md"
    json_path = run_dir / f"{repo_id}.planning_audit.json"
    md_path.write_text("# Planning Audit (test_repo)\n", encoding="utf-8")
    json_path.write_text('{"schema_version": 1}\n', encoding="utf-8")

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "planning-audit",
            "--repo-id",
            repo_id,
            "--run-id",
            run_id,
            "--cache-dir",
            cache_dir.as_posix(),
        ],
    )
    assert result.returncode == 0, result.stderr
    assert f"RUN_ID={run_id}" in result.stdout
    assert f"repo_id={repo_id}" in result.stdout
    assert "status=ok" in result.stdout
    assert f"json_path={json_path.as_posix()}" in result.stdout
    assert "json_exists=true" in result.stdout
    assert f"md_path={md_path.as_posix()}" in result.stdout
    assert "md_exists=true" in result.stdout


def test_planning_audit_cli_can_dump_markdown(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    run_id = "run-123"
    repo_id = "test_repo"

    run_dir = cache_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    md_path = run_dir / f"{repo_id}.planning_audit.md"
    md_path.write_text("# Planning Audit (test_repo)\n\n## Summary\n- Overall severity: `low`\n", encoding="utf-8")

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "planning-audit",
            "--repo-id",
            repo_id,
            "--run-id",
            run_id,
            "--cache-dir",
            cache_dir.as_posix(),
            "--dump",
            "md",
        ],
    )
    assert result.returncode == 0, result.stderr
    assert "# Planning Audit (test_repo)" in result.stdout


def test_planning_audit_cli_missing_artifacts_is_actionable(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    run_id = "run-123"
    repo_id = "test_repo"

    run_dir = cache_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "planning-audit",
            "--repo-id",
            repo_id,
            "--run-id",
            run_id,
            "--cache-dir",
            cache_dir.as_posix(),
        ],
    )
    assert result.returncode != 0
    assert "status=missing" in result.stdout
    assert "error=planning_audit_missing" in result.stdout
    assert "next_action=" in result.stdout

