from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from codex_orchestrator import codex_subprocess


def test_codex_exec_full_auto_calls_on_start_and_returns_pid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeProc:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.pid = 12345
            self.returncode = 0

        def communicate(self, *, input: str | None = None, timeout: float | None = None) -> tuple[str, str]:
            assert input == "PROMPT"
            assert timeout == 5.0
            return ("STDOUT", "STDERR")

    monkeypatch.setattr(codex_subprocess.subprocess, "Popen", FakeProc)

    captured: dict[str, object] = {}

    def _on_start(pid: int, argv: tuple[str, ...], started_at: datetime) -> None:
        captured["pid"] = pid
        captured["argv"] = argv
        captured["started_at"] = started_at

    invocation = codex_subprocess.codex_exec_full_auto(
        prompt="PROMPT",
        cwd=tmp_path,
        timeout_seconds=5.0,
        extra_args=("--model", "gpt-5"),
        on_start=_on_start,
    )

    assert invocation.pid == 12345
    assert captured["pid"] == 12345
    assert captured["argv"] == ("codex", "exec", "--full-auto", "--model", "gpt-5")
    started_at = captured["started_at"]
    assert isinstance(started_at, datetime)
    assert started_at.tzinfo is not None


def test_codex_exec_full_auto_timeout_kills_and_returns_124(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeProc:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.pid = 4242
            self.returncode = None
            self._calls = 0
            self.killed = False

        def communicate(
            self, *, input: str | None = None, timeout: float | None = None
        ) -> tuple[str, str]:
            self._calls += 1
            if self._calls == 1:
                raise subprocess.TimeoutExpired(
                    cmd=["codex", "exec"],
                    timeout=timeout or 0.0,
                    output="partial-out",
                    stderr="partial-err",
                )
            assert input is None
            assert timeout is None
            self.returncode = -9 if self.killed else 0
            return ("rest-out", "rest-err")

        def kill(self) -> None:
            self.killed = True

    monkeypatch.setattr(codex_subprocess.subprocess, "Popen", FakeProc)

    seen: dict[str, object] = {}

    def _on_start(pid: int, argv: tuple[str, ...], started_at: datetime) -> None:
        seen["pid"] = pid
        seen["argv"] = argv
        seen["started_at"] = started_at

    invocation = codex_subprocess.codex_exec_full_auto(
        prompt="PROMPT",
        cwd=tmp_path,
        timeout_seconds=1.0,
        on_start=_on_start,
    )

    assert invocation.pid == 4242
    assert seen["pid"] == 4242
    assert invocation.exit_code == 124
    assert "partial-out" in invocation.stdout
    assert "rest-out" in invocation.stdout
    assert "partial-err" in invocation.stderr
    assert "rest-err" in invocation.stderr

