from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

import codex_orchestrator.cli as orchestrator_cli
import codex_orchestrator.status_cli as status_cli
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_lifecycle import tick_run


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


def _seed_orphaned_current_run(paths: OrchestratorPaths) -> str:
    now = datetime.now(timezone.utc)
    initial = tick_run(paths=paths, mode="manual", now=now, idle_ticks_to_end=10)
    assert initial.run_id is not None
    dead_pid = _find_dead_pid()
    paths.run_lock_path.write_text(
        json.dumps({"pid": dead_pid, "locked_at": now.isoformat()}) + "\n",
        encoding="utf-8",
    )
    return initial.run_id


def test_status_cli_recovers_orphaned_current_run_before_lookup(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    orphaned_run_id = _seed_orphaned_current_run(paths)

    with pytest.raises(SystemExit) as excinfo:
        status_cli.main(["--cache-dir", tmp_path.as_posix()])

    assert "no active run found" in str(excinfo.value)
    assert not paths.current_run_path.exists()
    run_end = _read_json(paths.run_end_path(orphaned_run_id))
    assert run_end["reason"] == "orphaned_owner_dead"


def test_cli_lookup_recovers_orphaned_current_run_before_lookup(tmp_path: Path) -> None:
    paths = OrchestratorPaths(cache_dir=tmp_path)
    orphaned_run_id = _seed_orphaned_current_run(paths)

    with pytest.raises(SystemExit) as excinfo:
        orchestrator_cli._load_current_run_id(paths)

    assert "no active run found" in str(excinfo.value)
    assert not paths.current_run_path.exists()
    run_end = _read_json(paths.run_end_path(orphaned_run_id))
    assert run_end["reason"] == "orphaned_owner_dead"
