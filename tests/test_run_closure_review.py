from __future__ import annotations

import json
import os
import stat
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from codex_orchestrator.ai_policy import AiSettings
from codex_orchestrator.contracts import ResolvedExecutionContract
from codex_orchestrator.orchestrator_cycle import run_orchestrator_cycle
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import RunDeck, RunDeckItem, ValidationResult, write_run_deck
from codex_orchestrator.run_closure_review import (
    RunClosureReviewError,
    build_final_review,
    run_review_only_codex_pass,
    write_final_review,
)
from codex_orchestrator.run_lifecycle import tick_run


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _make_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR)


def _git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return (completed.stdout or "").strip()


def test_write_final_review_creates_deterministic_artifacts(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250101-000000-deadbeef"

    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    _write_json(
        paths.run_metadata_path(run_id),
        {
            "schema_version": 1,
            "run_id": run_id,
            "mode": "manual",
            "created_at": now.isoformat(),
            "last_tick_at": now.isoformat(),
            "expires_at": now.isoformat(),
            "tick_count": 1,
            "consecutive_idle_ticks": 1,
        },
    )
    _write_json(
        paths.run_end_path(run_id),
        {"run_id": run_id, "ended_at": "2025-01-01T00:10:00+00:00", "reason": "idle_ticks"},
    )

    contract = ResolvedExecutionContract(
        time_budget_minutes=10,
        validation_commands=("pytest -q",),
        env="test",
        allow_env_creation=False,
        requires_notebook_execution=False,
        allowed_roots=(Path("."),),
        deny_roots=(),
        notebook_roots=(Path("."),),
        notebook_output_policy="strip",
    )
    baseline = ValidationResult(
        command="pytest -q",
        exit_code=0,
        started_at=now,
        finished_at=now,
        stdout="",
        stderr="",
    )

    deck_a = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="repo_a",
        created_at=now,
        items=(
            RunDeckItem(bead_id="bd-1", title="One", contract=contract, baseline_validation=(baseline,)),
            RunDeckItem(bead_id="bd-2", title="Two", contract=contract, baseline_validation=(baseline,)),
        ),
    )
    deck_b = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="repo_b",
        created_at=now,
        items=(RunDeckItem(bead_id="bd-9", title="Nine", contract=contract, baseline_validation=(baseline,)),),
    )
    deck_a_path = write_run_deck(paths, deck=deck_a)
    deck_b_path = write_run_deck(paths, deck=deck_b)

    _write_json(
        paths.repo_summary_path(run_id, "repo_b"),
        {
            "schema_version": 1,
            "run_id": run_id,
            "repo_id": "repo_b",
            "repo_path": "/tmp/repo_b",
            "branch": f"run/{run_id}",
            "skipped": True,
            "skip_reason": "missing_tools",
            "stop_reason": None,
            "beads_attempted": 0,
            "beads_closed": 0,
            "deck_path": deck_b_path.as_posix(),
            "reused_existing_deck": True,
            "beads": [],
            "next_action": "Install required tools and re-run.",
        },
    )
    # Intentionally reverse bead audit ordering vs deck order to verify stable deck-ordered output.
    _write_json(
        paths.repo_summary_path(run_id, "repo_a"),
        {
            "schema_version": 1,
            "run_id": run_id,
            "repo_id": "repo_a",
            "repo_path": "/tmp/repo_a",
            "branch": f"run/{run_id}",
            "skipped": False,
            "skip_reason": None,
            "stop_reason": "completed",
            "beads_attempted": 2,
            "beads_closed": 1,
            "deck_path": deck_a_path.as_posix(),
            "reused_existing_deck": False,
            "beads": [
                {
                    "bead_id": "bd-2",
                    "title": "Two",
                    "outcome": "failed",
                    "detail": "Validation failed.",
                    "validation": {"pytest -q": "exit=1"},
                    "changed_paths": ["b.py"],
                },
                {
                    "bead_id": "bd-1",
                    "title": "One",
                    "outcome": "closed",
                    "detail": "Closed successfully.",
                    "validation": {"pytest -q": "ok"},
                    "changed_paths": ["a.py"],
                    "commit_hash": "0123456789abcdef",
                },
            ],
            "next_action": "Inspect failing bead details in logs; fix and re-run.",
        },
    )

    ai_settings = AiSettings(model="gpt-5.2", reasoning_effort="xhigh")
    review_1 = build_final_review(paths, run_id=run_id, ai_settings=ai_settings)
    review_2 = build_final_review(paths, run_id=run_id, ai_settings=ai_settings)
    assert review_1 == review_2

    artifacts = write_final_review(paths, run_id=run_id, ai_settings=ai_settings)
    assert artifacts.json_path.exists()
    assert artifacts.md_path.exists()
    assert paths.run_summary_path(run_id).exists()

    loaded = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert loaded["run_id"] == run_id
    assert [r["repo_id"] for r in loaded["repos"]] == ["repo_a", "repo_b"]
    assert loaded["repos"][0]["deck"]["planned_bead_ids"] == ["bd-1", "bd-2"]
    assert [b["bead_id"] for b in loaded["repos"][0]["beads"]] == ["bd-1", "bd-2"]


def test_orchestrator_cycle_writes_final_review_on_end(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    started = tick_run(paths=paths, mode="manual", now=now, actionable_work_found=False, idle_ticks_to_end=10)
    assert started.ended is False
    assert started.run_id is not None

    # End the run at ensure_active_run by lowering the idle threshold.
    result = run_orchestrator_cycle(
        cache_dir=cache_dir,
        mode="manual",
        ai_settings=AiSettings(model="gpt-5.2", reasoning_effort="xhigh"),
        repo_config_path=tmp_path / "unused_repos.toml",
        overlays_dir=tmp_path / "unused_overlays",
        idle_ticks_to_end=1,
        now=now,
    )
    assert result.ensure_result.ended is True
    run_id = result.ensure_result.run_id
    assert run_id is not None
    assert paths.final_review_json_path(run_id).exists()
    assert paths.final_review_md_path(run_id).exists()


def _write_fake_codex(bin_dir: Path, *, writes_diff: bool) -> None:
    script = bin_dir / "codex"
    lines = [
        "#!/usr/bin/env python3",
        "from __future__ import annotations",
        "import sys",
        "from pathlib import Path",
        "",
        "def main(argv):",
        "    _ = sys.stdin.read()",
    ]
    if writes_diff:
        lines.append("    Path('oops.txt').write_text('oops\\n', encoding='utf-8')")
    lines.extend(
        [
            "    print('review')",
            "    return 0",
            "",
            "if __name__ == '__main__':",
            "    raise SystemExit(main(sys.argv))",
        ]
    )
    script.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _make_executable(script)


def test_review_only_codex_pass_enforces_zero_diffs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / "README.md").write_text("hi\n", encoding="utf-8")
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250101-000000-deadbeef"
    _write_json(paths.run_end_path(run_id), {"run_id": run_id, "ended_at": "x", "reason": "manual"})
    _write_json(
        paths.repo_summary_path(run_id, "repo_x"),
        {
            "schema_version": 1,
            "run_id": run_id,
            "repo_id": "repo_x",
            "repo_path": repo_root.as_posix(),
            "skipped": False,
        },
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_codex(bin_dir, writes_diff=True)
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))

    with pytest.raises(RunClosureReviewError):
        run_review_only_codex_pass(
            paths,
            run_id=run_id,
            ai_settings=AiSettings(model="gpt-5.2", reasoning_effort="xhigh"),
            timeout_seconds=5.0,
        )
    assert (paths.run_dir(run_id) / "final_codex_review.repo_x.json").exists()


def test_review_only_codex_pass_allows_clean_stdout_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / "README.md").write_text("hi\n", encoding="utf-8")
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250101-000000-deadbeef"
    _write_json(paths.run_end_path(run_id), {"run_id": run_id, "ended_at": "x", "reason": "manual"})
    _write_json(
        paths.repo_summary_path(run_id, "repo_x"),
        {
            "schema_version": 1,
            "run_id": run_id,
            "repo_id": "repo_x",
            "repo_path": repo_root.as_posix(),
            "skipped": False,
        },
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_codex(bin_dir, writes_diff=False)
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))

    logs = run_review_only_codex_pass(
        paths,
        run_id=run_id,
        ai_settings=AiSettings(model="gpt-5.2", reasoning_effort="xhigh"),
        timeout_seconds=5.0,
    )
    assert len(logs) == 1
    assert logs[0].repo_id == "repo_x"
    assert logs[0].path.exists()
    assert _git(repo_root, "status", "--porcelain") == ""

