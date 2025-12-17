from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_closure_review import write_final_review
from codex_orchestrator.run_lifecycle import (
    RunLifecycleError,
    end_current_run,
    ensure_active_run,
    tick_run,
)
from codex_orchestrator.run_signoff import write_run_signoff


def _end_run(paths: OrchestratorPaths, *, run_id: str, now: datetime) -> None:
    ended = end_current_run(paths=paths, reason="test_end", now=now)
    assert ended == run_id
    write_final_review(paths, run_id=run_id, ai_settings=None)


def test_new_run_blocked_when_latest_ended_run_not_signed_off_tick(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=1)
    t2 = t1 + timedelta(minutes=1)

    started = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    assert started.run_id is not None
    _end_run(paths, run_id=started.run_id, now=t1)

    with pytest.raises(RunLifecycleError) as excinfo:
        tick_run(paths=paths, mode="manual", now=t2, idle_ticks_to_end=10)
    msg = str(excinfo.value)
    assert "Refusing to start a new run" in msg
    assert str(paths.final_review_json_path(started.run_id)) in msg
    assert "codex-orchestrator signoff" in msg


def test_new_run_blocked_when_latest_ended_run_not_signed_off_ensure(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=1)
    t2 = t1 + timedelta(minutes=1)

    started = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    assert started.run_id is not None
    _end_run(paths, run_id=started.run_id, now=t1)

    with pytest.raises(RunLifecycleError) as excinfo:
        ensure_active_run(paths=paths, mode="manual", now=t2, idle_ticks_to_end=10)
    msg = str(excinfo.value)
    assert "Refusing to start a new run" in msg
    assert str(paths.final_review_json_path(started.run_id)) in msg
    assert "codex-orchestrator signoff" in msg


def test_new_run_allowed_after_latest_ended_run_signed_off(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=1)
    t2 = t1 + timedelta(minutes=1)

    started = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    assert started.run_id is not None
    _end_run(paths, run_id=started.run_id, now=t1)

    write_run_signoff(
        paths,
        run_id=started.run_id,
        reviewer="Test Reviewer",
        reviewed_at=t2,
        notes="LGTM",
    )

    result = tick_run(paths=paths, mode="manual", now=t2, idle_ticks_to_end=10)
    assert result.run_id is not None
    assert result.started_new is True
