from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_lifecycle import tick_run


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_tick_creates_new_run_and_persists_state(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    now = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    result = tick_run(paths=paths, mode="manual", now=now, idle_ticks_to_end=10)
    assert result.run_id is not None
    assert result.started_new is True
    assert result.ended is False

    state = _read_json(paths.current_run_path)
    assert state["run_id"] == result.run_id
    assert (paths.runs_dir / result.run_id).exists()
    assert paths.run_metadata_path(result.run_id).exists()


def test_tick_reuses_run_id_within_expiry(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=1)

    r0 = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    r1 = tick_run(paths=paths, mode="manual", now=t1, idle_ticks_to_end=10)
    assert r0.run_id == r1.run_id


def test_tick_ends_run_after_idle_threshold(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=1)

    r0 = tick_run(paths=paths, mode="manual", now=t0, actionable_work_found=False, idle_ticks_to_end=2)
    assert r0.ended is False
    assert paths.current_run_path.exists()

    r1 = tick_run(paths=paths, mode="manual", now=t1, actionable_work_found=False, idle_ticks_to_end=2)
    assert r1.ended is True
    assert not paths.current_run_path.exists()


def test_tick_creates_new_run_after_expiry(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    ttl = timedelta(seconds=30)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=31)

    r0 = tick_run(paths=paths, mode="manual", now=t0, manual_ttl=ttl, idle_ticks_to_end=10)
    r1 = tick_run(paths=paths, mode="manual", now=t1, manual_ttl=ttl, idle_ticks_to_end=10)
    assert r0.run_id != r1.run_id


def test_automated_run_ends_at_window_end(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 6, 50, 0, tzinfo=timezone.utc)
    t1 = datetime(2025, 1, 1, 7, 1, 0, tzinfo=timezone.utc)

    r0 = tick_run(paths=paths, mode="automated", now=t0, idle_ticks_to_end=10)
    assert r0.ended is False
    assert paths.current_run_path.exists()

    r1 = tick_run(paths=paths, mode="automated", now=t1, idle_ticks_to_end=10)
    assert r1.ended is True
    assert not paths.current_run_path.exists()

