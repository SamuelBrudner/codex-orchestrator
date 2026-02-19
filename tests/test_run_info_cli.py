from __future__ import annotations

import json
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


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_run_info_lists_recent_runs_with_status_and_counts(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    run_a = "20250102-010101-aaaa1111"
    run_b = "20250101-010101-bbbb2222"

    _write_json(
        cache_dir / "current_run.json",
        {
            "schema_version": 3,
            "run_id": run_a,
            "mode": "manual",
            "created_at": "2025-01-02T01:01:01+00:00",
            "last_tick_at": "2025-01-02T01:05:01+00:00",
            "expires_at": "2099-01-02T01:05:01+00:00",
            "tick_count": 1,
            "consecutive_idle_ticks": 0,
            "beads_attempted_total": 0,
            "beads_attempted_since_review": 0,
        },
    )

    _write_json(
        cache_dir / "runs" / run_a / "run.json",
        {
            "schema_version": 3,
            "run_id": run_a,
            "mode": "manual",
            "created_at": "2025-01-02T01:01:01+00:00",
        },
    )
    _write_json(
        cache_dir / "runs" / run_a / "run_summary.json",
        {
            "schema_version": 1,
            "run_id": run_a,
            "repos": [
                {
                    "repo_id": "repo_a",
                    "skipped": False,
                    "stop_reason": "tick_time_remaining",
                    "beads_attempted": 2,
                    "beads_closed": 1,
                }
            ],
        },
    )

    _write_json(
        cache_dir / "runs" / run_b / "run.json",
        {
            "schema_version": 3,
            "run_id": run_b,
            "mode": "manual",
            "created_at": "2025-01-01T01:01:01+00:00",
        },
    )
    _write_json(
        cache_dir / "runs" / run_b / "run_end.json",
        {
            "run_id": run_b,
            "ended_at": "2025-01-01T02:00:00+00:00",
            "reason": "idle_ticks",
        },
    )
    _write_json(
        cache_dir / "runs" / run_b / "run_summary.json",
        {
            "schema_version": 1,
            "run_id": run_b,
            "repos": [
                {
                    "repo_id": "repo_b",
                    "skipped": True,
                    "skip_reason": "git_dirty",
                    "beads_attempted": 0,
                    "beads_closed": 0,
                }
            ],
        },
    )

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "run-info",
            "--cache-dir",
            cache_dir.as_posix(),
            "--limit",
            "2",
        ],
    )
    assert result.returncode == 0, result.stderr
    assert f"RUN_ID={run_a} status=active" in result.stdout
    assert f"RUN_ID={run_b} status=ended" in result.stdout
    assert "reason=idle_ticks" in result.stdout
    assert "failed=1" in result.stdout
    assert result.stdout.find(run_a) < result.stdout.find(run_b)


def test_run_info_can_inspect_single_run_with_debug_tails(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    run_id = "20250103-120000-cccc3333"
    run_dir = cache_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        run_dir / "run.json",
        {
            "schema_version": 3,
            "run_id": run_id,
            "mode": "manual",
            "created_at": "2025-01-03T12:00:00+00:00",
            "last_tick_at": "2025-01-03T12:10:00+00:00",
        },
    )
    _write_json(
        run_dir / "run_end.json",
        {
            "run_id": run_id,
            "ended_at": "2025-01-03T12:20:00+00:00",
            "reason": "idle_ticks",
        },
    )
    _write_json(
        run_dir / "run_summary.json",
        {
            "schema_version": 1,
            "run_id": run_id,
            "repos": [
                {
                    "repo_id": "alpha",
                    "skipped": False,
                    "stop_reason": "error",
                    "beads_attempted": 1,
                    "beads_closed": 0,
                    "next_action": "Inspect exec log for error and re-run.",
                    "failures": ["git push failed"],
                    "beads": [
                        {
                            "bead_id": "bd-1",
                            "outcome": "failed",
                            "detail": "Validation failed: pytest -q",
                        }
                    ],
                }
            ],
        },
    )

    (run_dir / "orchestrator.log").write_text(
        "\n".join(
            [
                "line-1",
                "line-2",
                "line-3",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "alpha.exec.log").write_text("exec-1\nexec-2\nexec-3\n", encoding="utf-8")
    (run_dir / "alpha.stderr.log").write_text("err-1\nerr-2\nerr-3\n", encoding="utf-8")
    (run_dir / "alpha.events.jsonl").write_text('{"type":"x"}\n{"type":"y"}\n{"type":"z"}\n', encoding="utf-8")

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "run-info",
            "--run-id",
            run_id,
            "--cache-dir",
            cache_dir.as_posix(),
            "--tail-lines",
            "2",
        ],
    )
    assert result.returncode == 0, result.stderr
    assert f"RUN_ID={run_id} status=ended" in result.stdout
    assert "artifact=run_log exists=true" in result.stdout
    assert "repo_id=alpha status=stop:error" in result.stdout
    assert "failure=git push failed" in result.stdout
    assert "failure=bd-1: Validation failed: pytest -q" in result.stdout
    assert "repo_log_tail=alpha:exec_log" in result.stdout
    assert "exec-2" in result.stdout
    assert "exec-3" in result.stdout
    assert "run_log_tail=orchestrator.log" in result.stdout
    assert "line-2" in result.stdout
    assert "line-3" in result.stdout


def test_run_info_json_filter_by_repo_id(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    run_id = "20250104-120000-dddd4444"
    run_dir = cache_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        run_dir / "run_summary.json",
        {
            "schema_version": 1,
            "run_id": run_id,
            "repos": [
                {
                    "repo_id": "alpha",
                    "skipped": False,
                    "stop_reason": "completed",
                    "beads_attempted": 1,
                    "beads_closed": 1,
                },
                {
                    "repo_id": "beta",
                    "skipped": False,
                    "stop_reason": "completed",
                    "beads_attempted": 1,
                    "beads_closed": 1,
                },
            ],
        },
    )

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "run-info",
            "--run-id",
            run_id,
            "--repo-id",
            "beta",
            "--cache-dir",
            cache_dir.as_posix(),
            "--json",
        ],
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["run_id"] == run_id
    assert len(payload["repos"]) == 1
    assert payload["repos"][0]["repo_id"] == "beta"


def test_run_info_missing_run_dir_is_actionable(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "run-info",
            "--run-id",
            "does-not-exist",
            "--cache-dir",
            cache_dir.as_posix(),
        ],
    )
    assert result.returncode != 0
    assert "run dir not found" in result.stderr
