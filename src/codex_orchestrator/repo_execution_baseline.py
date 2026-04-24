from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from codex_orchestrator.planner import RunDeckItem, ValidationResult

MIN_VALIDATION_RETRY_SECONDS = 60.0


def _parse_command_argv(command: str) -> list[str] | None:
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    return argv or None


def _validation_command_allowed(command: str) -> bool:
    argv = _parse_command_argv(command)
    if argv is None:
        return False
    first = argv[0]
    return first in {"pytest", "python", "python3", "ruff", "make", "nox", "pre-commit"}


def _is_behavioral_test_command(command: str) -> bool:
    argv = _parse_command_argv(command)
    if argv is None:
        return False
    if argv[0] == "pytest":
        return True
    return (
        argv[0] in {"python", "python3"}
        and len(argv) >= 3
        and argv[1] == "-m"
        and argv[2] == "pytest"
    )


def _require_validation_allowlist(
    commands: tuple[str, ...] | list[str],
    *,
    error_type: type[Exception] = RuntimeError,
) -> None:
    forbidden = [command for command in commands if command.strip() and not _validation_command_allowed(command)]
    if forbidden:
        raise error_type(
            "Validation commands must be allowlisted; forbidden:\n- " + "\n- ".join(forbidden)
        )


def _summarize_preflight_output(result: ValidationResult, *, limit: int = 1200) -> str:
    text = (result.stderr or "").strip() or (result.stdout or "").strip()
    if not text:
        return f"exit={result.exit_code}"
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return text[:limit] + f"\n...<truncated {omitted} chars>"


@dataclass(frozen=True, slots=True)
class _RuntimeBaselineSnapshot:
    results: tuple[ValidationResult, ...]
    failing_commands: tuple[str, ...]
    context: str | None


def _format_runtime_baseline_context(results: tuple[ValidationResult, ...] | list[ValidationResult]) -> str | None:
    failures = [result for result in results if result.exit_code != 0]
    if not failures:
        return None
    lines = ["Baseline validation failures (before this bead):"]
    for result in failures:
        lines.append(f"- {result.command}: exit={result.exit_code}")
    lines.append("Resolve these if possible (install missing tools or fix tests).")
    return "\n".join(lines)


def _capture_runtime_baseline(
    *,
    item: RunDeckItem,
    repo_root: Any,
    tick: Any,
    bead_deadline: datetime,
    configured_timeout_seconds: float,
    run_validation_commands_fn: Any,
    remaining_bead_time_fn: Any,
    validation_timeout_seconds_fn: Any,
    error_type: type[Exception] = RuntimeError,
    now_fn: Any,
) -> _RuntimeBaselineSnapshot:
    _require_validation_allowlist(item.contract.validation_commands, error_type=error_type)
    timeout_seconds = validation_timeout_seconds_fn(
        commands=item.contract.validation_commands,
        remaining=remaining_bead_time_fn(
            tick=tick,
            now=now_fn(),
            bead_deadline=bead_deadline,
        ),
        configured_timeout_seconds=configured_timeout_seconds,
    )
    results_by_command = run_validation_commands_fn(
        item.contract.validation_commands,
        cwd=repo_root,
        env=item.contract.env,
        timeout_seconds=timeout_seconds,
    )
    results = tuple(results_by_command.values())
    failing_commands = tuple(result.command for result in results if result.exit_code != 0)
    return _RuntimeBaselineSnapshot(
        results=results,
        failing_commands=failing_commands,
        context=_format_runtime_baseline_context(results),
    )


def _format_validation_retry_context(
    *,
    attempt: int,
    validation_results: dict[str, ValidationResult],
    baseline_failures: tuple[str, ...] | list[str],
) -> str:
    failed = {command: result for command, result in validation_results.items() if result.exit_code != 0}
    lines = [f"Validation failures after attempt {attempt}:"]
    for command in sorted(failed):
        lines.append(f"- {command}: {_validation_status(failed[command].exit_code)}")
    if baseline_failures:
        still = sorted(command for command in baseline_failures if command in failed)
        if still:
            lines.append("Baseline failures still present:")
            lines.extend(f"- {command}" for command in still)
    lines.append("Fix the failures above and re-run validations.")
    return "\n".join(lines)


def _remaining_bead_time(
    *,
    tick: Any,
    now: datetime,
    bead_deadline: datetime,
) -> timedelta:
    remaining_tick = tick.remaining(now=now)
    remaining_bead = max(bead_deadline - now, timedelta(0))
    return remaining_tick if remaining_tick <= remaining_bead else remaining_bead


def _can_retry_validation(*, tick: Any, now: datetime, bead_deadline: datetime) -> bool:
    return _remaining_bead_time(
        tick=tick,
        now=now,
        bead_deadline=bead_deadline,
    ) >= timedelta(seconds=MIN_VALIDATION_RETRY_SECONDS)


def _validation_timeout_seconds(
    *,
    commands: tuple[str, ...] | list[str],
    remaining: timedelta,
    configured_timeout_seconds: float,
) -> float:
    command_count = max(1, len({command.strip() for command in commands if command.strip()}))
    remaining_seconds = max(0.0, remaining.total_seconds())
    if remaining_seconds <= 0:
        return 1.0
    per_command_budget = remaining_seconds / float(command_count)
    return max(1.0, min(float(configured_timeout_seconds), per_command_budget))


def _validation_status(exit_code: int) -> str:
    return "ok" if exit_code == 0 else f"exit={exit_code}"


def _format_validation_summary(results: dict[str, ValidationResult]) -> str:
    lines: list[str] = []
    for command in sorted(results):
        result = results[command]
        status = "ok" if result.exit_code == 0 else f"exit={result.exit_code}"
        lines.append(
            f"- {command}: {status} ({(result.finished_at - result.started_at).total_seconds():.1f}s)"
        )
    return "\n".join(lines)
