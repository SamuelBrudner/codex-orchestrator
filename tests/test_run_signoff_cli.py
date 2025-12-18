from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_signoff import RunSignoffError, validate_run_signoff, write_run_signoff


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


def _write_run_end(*, paths: OrchestratorPaths, run_id: str, ended_at: datetime) -> None:
    payload = {"run_id": run_id, "ended_at": ended_at.isoformat(), "reason": "test_end"}
    path = paths.run_end_path(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_signoff_cli_writes_artifacts(tmp_path: Path) -> None:
    run_id = "20250101-000000-deadbeef"
    paths = OrchestratorPaths(cache_dir=tmp_path)
    _write_run_end(paths=paths, run_id=run_id, ended_at=datetime(2025, 1, 1, tzinfo=timezone.utc))

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "signoff",
            "--cache-dir",
            tmp_path.as_posix(),
            "--run-id",
            run_id,
            "--reviewer",
            "Test Reviewer",
            "--notes",
            "LGTM",
        ],
    )
    assert result.returncode == 0, result.stderr
    assert f"RUN_ID={run_id} status=ok" in result.stdout
    assert f"json_path={paths.run_signoff_json_path(run_id).as_posix()}" in result.stdout
    assert f"md_path={paths.run_signoff_md_path(run_id).as_posix()}" in result.stdout

    assert paths.run_signoff_json_path(run_id).exists()
    assert paths.run_signoff_md_path(run_id).exists()
    assert paths.final_review_json_path(run_id).exists()
    assert paths.final_review_md_path(run_id).exists()

    signoff = json.loads(paths.run_signoff_json_path(run_id).read_text(encoding="utf-8"))
    assert signoff["run_id"] == run_id
    assert signoff["reviewer"] == "Test Reviewer"
    assert signoff["notes"] == "LGTM"

    expected_sha = hashlib.sha256(paths.final_review_json_path(run_id).read_bytes()).hexdigest()
    assert signoff["final_review"]["sha256"] == expected_sha


def test_validate_run_signoff_fails_when_final_review_sha_mismatches(tmp_path: Path) -> None:
    run_id = "20250101-000000-deadbeef"
    paths = OrchestratorPaths(cache_dir=tmp_path)
    _write_run_end(paths=paths, run_id=run_id, ended_at=datetime(2025, 1, 1, tzinfo=timezone.utc))

    signoff = write_run_signoff(
        paths,
        run_id=run_id,
        reviewer="Test Reviewer",
        reviewed_at=datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc),
        notes=None,
    )

    final_review = json.loads(paths.final_review_json_path(run_id).read_text(encoding="utf-8"))
    final_review["tampered"] = True
    paths.final_review_json_path(run_id).write_text(
        json.dumps(final_review, sort_keys=True) + "\n", encoding="utf-8"
    )
    tampered_sha = hashlib.sha256(paths.final_review_json_path(run_id).read_bytes()).hexdigest()
    assert tampered_sha != signoff.final_review_sha256

    with pytest.raises(RunSignoffError) as excinfo:
        validate_run_signoff(paths, run_id=run_id)
    msg = str(excinfo.value)
    assert "Run signoff no longer matches current final review content" in msg
    assert f"expected sha256={signoff.final_review_sha256}" in msg
    assert f"got sha256={tampered_sha}" in msg

