from __future__ import annotations

import shlex
import subprocess
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from codex_orchestrator.planner import ValidationResult


def _dedupe_preserve_order(items: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        out.append(item)
        seen.add(item)
    return out


def _truncate(text: str, *, limit: int) -> str:
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return text[:limit] + f"\n...<truncated {omitted} chars>"


def _parse_command_argv(command: str) -> list[str] | None:
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    return argv or None


def run_validation_commands(
    commands: Sequence[str],
    *,
    cwd: Path,
    env: str | None = None,
    timeout_seconds: float = 900.0,
    output_limit_chars: int = 20_000,
) -> dict[str, ValidationResult]:
    results: dict[str, ValidationResult] = {}
    for command in _dedupe_preserve_order([c for c in commands if c.strip()]):
        started_at = datetime.now().astimezone()
        argv = _parse_command_argv(command)
        if argv is None:
            finished_at = datetime.now().astimezone()
            results[command] = ValidationResult(
                command=command,
                exit_code=2,
                started_at=started_at,
                finished_at=finished_at,
                stdout="",
                stderr=_truncate(
                    "Failed to parse validation command into argv; "
                    "avoid shell operators and ensure quotes are balanced.",
                    limit=output_limit_chars,
                ),
            )
            continue

        run_argv = argv
        if env is not None and env.strip():
            run_argv = ["conda", "run", "-n", env.strip(), *argv]
        try:
            completed = subprocess.run(
                run_argv,
                cwd=cwd,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds,
            )
            finished_at = datetime.now().astimezone()
            exit_code = int(completed.returncode)
            stdout = _truncate(completed.stdout or "", limit=output_limit_chars)
            stderr = _truncate(completed.stderr or "", limit=output_limit_chars)
        except subprocess.TimeoutExpired as e:
            finished_at = datetime.now().astimezone()
            exit_code = 124
            stdout = _truncate((e.stdout or "") if isinstance(e.stdout, str) else "", limit=output_limit_chars)
            stderr = _truncate((e.stderr or "") if isinstance(e.stderr, str) else "", limit=output_limit_chars)
        except FileNotFoundError as e:
            finished_at = datetime.now().astimezone()
            exit_code = 127
            stdout = ""
            stderr = _truncate(str(e), limit=output_limit_chars)

        results[command] = ValidationResult(
            command=command,
            exit_code=exit_code,
            started_at=started_at,
            finished_at=finished_at,
            stdout=stdout,
            stderr=stderr,
        )
    return results
