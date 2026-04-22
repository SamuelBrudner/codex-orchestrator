from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import codex_orchestrator.repo_execution as repo_execution


def test_codex_start_callback_binds_attempt_context_for_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_path = tmp_path / "exec.log"
    runtime = repo_execution._CodexAttemptRuntime()
    events: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(repo_execution, "_CODEX_HEARTBEAT_INTERVAL_SECONDS", 0.01)

    bead_id = "bd-1"
    attempt = 2
    callback = repo_execution._make_codex_on_start_callback(
        runtime=runtime,
        repo_id="demo",
        bead_id=bead_id,
        attempt=attempt,
        timeout_seconds=120.0,
        log_path=log_path,
        emit=lambda event_type, **fields: events.append((event_type, fields)),
    )

    bead_id = "bd-changed"
    attempt = 99
    started_at = datetime.now().astimezone() - timedelta(seconds=1)
    callback(4321, ("codex", "exec"), started_at)

    assert runtime.pid == 4321
    assert runtime.heartbeat_stop is not None
    assert runtime.heartbeat_thread is not None

    time.sleep(0.03)
    runtime.heartbeat_stop.set()
    runtime.heartbeat_thread.join(timeout=1.0)

    assert events[0][0] == "codex_spawn"
    assert events[0][1]["bead_id"] == "bd-1"
    assert events[0][1]["attempt"] == 2
    assert any(
        event_type == "codex_heartbeat"
        and fields["bead_id"] == "bd-1"
        and fields["attempt"] == 2
        for event_type, fields in events
    )

    log_text = log_path.read_text(encoding="utf-8")
    assert "bead_id=bd-1" in log_text
    assert "attempt=2" in log_text
