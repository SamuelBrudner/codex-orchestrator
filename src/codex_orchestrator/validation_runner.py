from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Sequence

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


def run_validation_commands(
    commands: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float = 900.0,
    output_limit_chars: int = 20_000,
) -> dict[str, ValidationResult]:
    results: dict[str, ValidationResult] = {}
    for command in _dedupe_preserve_order([c for c in commands if c.strip()]):
        started_at = datetime.now().astimezone()
        try:
            completed = subprocess.run(
                command,
                cwd=cwd,
                shell=True,
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

        results[command] = ValidationResult(
            command=command,
            exit_code=exit_code,
            started_at=started_at,
            finished_at=finished_at,
            stdout=stdout,
            stderr=stderr,
        )
    return results

