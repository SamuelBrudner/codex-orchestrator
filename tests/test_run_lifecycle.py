from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_lifecycle import (
    ensure_active_run,
    record_review,
    recover_orphaned_current_run,
    tick_run,
)
from codex_orchestrator.run_state import CurrentRunState


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_dead_pid(start: int = 99999) -> int:
    pid = start
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return pid
        except PermissionError:
            pid += 1
            continue


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


def test_automated_tick_outside_window_does_not_start_run(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    result = tick_run(paths=paths, mode="automated", now=now, idle_ticks_to_end=10)
    assert result.ended is True
    assert result.run_id is None
    assert result.end_reason == "outside_window"
    assert not paths.current_run_path.exists()
    assert not paths.runs_dir.exists()


def test_tick_tracks_review_cadence_without_ending_run(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=1)

    r0 = tick_run(
        paths=paths,
        mode="manual",
        now=t0,
        idle_ticks_to_end=10,
        beads_attempted_delta=1,
    )
    assert r0.ended is False
    assert paths.current_run_path.exists()
    state0 = CurrentRunState.from_json_dict(_read_json(paths.current_run_path))
    assert state0.beads_attempted_since_review == 1
    assert state0.review_due(review_every_beads=2) is False

    r1 = tick_run(
        paths=paths,
        mode="manual",
        now=t1,
        idle_ticks_to_end=10,
        beads_attempted_delta=1,
    )
    assert r1.ended is False
    assert paths.current_run_path.exists()
    state1 = CurrentRunState.from_json_dict(_read_json(paths.current_run_path))
    assert state1.beads_attempted_since_review == 2
    assert state1.review_due(review_every_beads=2) is True

    record_review(paths=paths, run_id=state1.run_id, now=t1)
    state2 = CurrentRunState.from_json_dict(_read_json(paths.current_run_path))
    assert state2.beads_attempted_since_review == 0


def test_ensure_active_run_recovers_orphaned_current_run(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=5)

    initial = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    assert initial.run_id is not None
    dead_pid = _find_dead_pid()
    paths.cycle_in_progress_path.write_text(
        json.dumps({"pid": dead_pid, "run_id": initial.run_id, "started_at": t0.isoformat()})
        + "\n",
        encoding="utf-8",
    )

    recovered = ensure_active_run(paths=paths, mode="manual", now=t1, idle_ticks_to_end=10)
    assert recovered.started_new is True
    assert recovered.run_id is not None
    assert recovered.run_id != initial.run_id

    run_end = _read_json(paths.run_end_path(initial.run_id))
    assert run_end["reason"] == "orphaned_owner_dead"
    assert not paths.cycle_in_progress_path.exists()


def test_tick_run_recovers_orphaned_current_run(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=5)

    initial = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    assert initial.run_id is not None
    dead_pid = _find_dead_pid()
    paths.cycle_in_progress_path.write_text(
        json.dumps({"pid": dead_pid, "run_id": initial.run_id, "started_at": t0.isoformat()})
        + "\n",
        encoding="utf-8",
    )

    recovered = tick_run(paths=paths, mode="manual", now=t1, idle_ticks_to_end=10)
    assert recovered.started_new is True
    assert recovered.run_id is not None
    assert recovered.run_id != initial.run_id

    run_end = _read_json(paths.run_end_path(initial.run_id))
    assert run_end["reason"] == "orphaned_owner_dead"
    assert not paths.cycle_in_progress_path.exists()


def test_recover_orphaned_current_run_marks_end_and_clears_current(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=5)

    initial = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    assert initial.run_id is not None
    dead_pid = _find_dead_pid()
    paths.cycle_in_progress_path.write_text(
        json.dumps({"pid": dead_pid, "run_id": initial.run_id, "started_at": t0.isoformat()})
        + "\n",
        encoding="utf-8",
    )

    recovered_run_id = recover_orphaned_current_run(paths=paths, now=t1)
    assert recovered_run_id == initial.run_id
    assert not paths.current_run_path.exists()

    run_end = _read_json(paths.run_end_path(initial.run_id))
    assert run_end["reason"] == "orphaned_owner_dead"
    assert not paths.cycle_in_progress_path.exists()


def test_orphan_recovery_does_not_end_run_without_cycle_marker(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=5)

    initial = tick_run(paths=paths, mode="manual", now=t0, idle_ticks_to_end=10)
    assert initial.run_id is not None
    dead_pid = _find_dead_pid()
    paths.run_lock_path.write_text(
        json.dumps({"pid": dead_pid, "locked_at": t0.isoformat()}) + "\n",
        encoding="utf-8",
    )

    recovered = ensure_active_run(paths=paths, mode="manual", now=t1, idle_ticks_to_end=10)
    assert recovered.run_id == initial.run_id
    assert recovered.started_new is False
