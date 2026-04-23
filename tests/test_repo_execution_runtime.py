from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import codex_orchestrator.repo_execution as repo_execution
from codex_orchestrator.contracts import ResolvedExecutionContract


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


def test_capture_runtime_baseline_recomputes_context_for_new_execution_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    item = repo_execution.RunDeckItem(
        bead_id="bd-1",
        title="Bead bd-1",
        contract=ResolvedExecutionContract(
            time_budget_minutes=10,
            validation_commands=("pytest -q",),
            env="test",
            allow_env_creation=False,
            requires_notebook_execution=False,
            allowed_roots=(Path("."),),
            deny_roots=(),
            notebook_roots=(Path("."),),
            notebook_output_policy="strip",
        ),
        baseline_validation=(),
    )
    now = datetime.now().astimezone()
    tick = repo_execution.TickBudget(started_at=now, ends_at=now + timedelta(minutes=10))
    bead_deadline = now + timedelta(minutes=10)
    calls = {"n": 0}

    def _fake_validation(
        commands: tuple[str, ...] | list[str],
        *,
        cwd: Path,
        env: str | None = None,
        timeout_seconds: float = 900.0,
        output_limit_chars: int = 20_000,
    ) -> dict[str, repo_execution.ValidationResult]:
        del cwd, env, timeout_seconds, output_limit_chars
        calls["n"] += 1
        started_at = datetime.now().astimezone()
        exit_code = calls["n"]
        return {
            command: repo_execution.ValidationResult(
                command=command,
                exit_code=exit_code,
                started_at=started_at,
                finished_at=started_at,
                stdout="",
                stderr=f"baseline {exit_code}",
            )
            for command in commands
        }

    monkeypatch.setattr(repo_execution, "run_validation_commands", _fake_validation)

    first = repo_execution._capture_runtime_baseline(
        item=item,
        repo_root=tmp_path,
        tick=tick,
        bead_deadline=bead_deadline,
        configured_timeout_seconds=900.0,
    )
    second = repo_execution._capture_runtime_baseline(
        item=item,
        repo_root=tmp_path,
        tick=tick,
        bead_deadline=bead_deadline,
        configured_timeout_seconds=900.0,
    )

    assert first.failing_commands == ("pytest -q",)
    assert second.failing_commands == ("pytest -q",)
    assert first.context is not None and "- pytest -q: exit=1" in first.context
    assert second.context is not None and "- pytest -q: exit=2" in second.context
    assert calls["n"] == 2
