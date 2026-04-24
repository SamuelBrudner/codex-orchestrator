from __future__ import annotations

import fnmatch
import json
import os
import re
import shlex
import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import codex_orchestrator.repo_execution_baseline as _baseline_impl
import codex_orchestrator.repo_execution_report as _report_impl
from codex_orchestrator import audit_trail as _audit_trail
from codex_orchestrator import codex_subprocess as _codex_subprocess
from codex_orchestrator.ai_policy import (
    REQUIRED_CODEX_MODEL,
    REQUIRED_REASONING_EFFORT,
    AiSettings,
)
from codex_orchestrator.audit_trail import (
    append_jsonl,
    collect_tool_versions,
    write_json_atomic,
)
from codex_orchestrator.git_subprocess import (
    GitError,
    git_branch_exists,
    git_checkout,
    git_checkout_new_branch,
    git_commit,
    git_current_branch,
    git_diff_numstat,
    git_fetch,
    git_head_is_detached,
    git_is_dirty,
    git_remote_branch_exists,
    git_remotes,
    git_stage_all,
    git_status_filtered,
)
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import RunDeckItem, ValidationResult
from codex_orchestrator.planning_pass import ensure_repo_run_deck
from codex_orchestrator.repo_execution_bead import execute_planned_beads
from codex_orchestrator.repo_execution_prepare import prepare_repo_workspace
from codex_orchestrator.repo_execution_report import RepoExecutionState, RepoRunArtifacts, RepoRunReporter
from codex_orchestrator.repo_inventory import RepoPolicy
from codex_orchestrator.run_lock import RunLock, RunLockError
from codex_orchestrator.validation_runner import run_validation_commands


class RepoExecutionError(RuntimeError):
    pass


RepoSkipReason = Literal[
    "missing_tools",
    "git_dirty",
    "git_detached",
    "git_fetch_failed",
    "git_branch_failed",
    "planning_failed",
    "lock_busy",
]

RepoStopReason = Literal[
    "bead_cap",
    "tick_time_remaining",
    "blocked",
    "error",
    "completed",
]

BeadOutcome = Literal[
    "skipped_closed",
    "skipped_blocked",
    "skipped_not_open",
    "closed",
    "failed",
]

_TIMEOUT_SUMMARY_MARKER = "[orchestrator] Timeout summary"
_DIFFCAP_SUMMARY_MARKER = "[orchestrator] Diff cap summary"
_DECOMPOSE_MARKER = "[orchestrator] Decomposed into follow-up beads"
_FOLLOWUP_MAX_CHANGED_PATHS = 8
_CODEX_HEARTBEAT_INTERVAL_SECONDS = 30.0
_ENV_PREFLIGHT_TORCH_NUMPY_SCRIPT = "\n".join(
    [
        "import importlib.util as _u",
        "import sys as _s",
        "_ts = _u.find_spec('torch')",
        "_ns = _u.find_spec('numpy')",
        "if _ts is None or _ns is None:",
        "    raise SystemExit(0)",
        "import numpy as _np",
        "import torch as _torch",
        "_torch.from_numpy(_np.zeros((1,), dtype=_np.float32))",
    ]
)
_ENV_PREFLIGHT_TORCH_NUMPY_COMMAND = (
    "python -c " + shlex.quote(_ENV_PREFLIGHT_TORCH_NUMPY_SCRIPT)
)
_ENV_PREFLIGHT_NAME = "torch_numpy_bridge"


@dataclass(frozen=True, slots=True)
class DiffCaps:
    max_files_changed: int = 25
    max_lines_added: int = 1_500


@dataclass(frozen=True, slots=True)
class RepoExecutionConfig:
    tick_budget: timedelta = timedelta(minutes=45)
    min_minutes_to_start_new_bead: int = 15
    max_beads_per_tick: int = 3
    diff_caps: DiffCaps = DiffCaps()
    codex_output_limit_chars: int = 200_000
    validation_timeout_seconds: float = 900.0
    codex_timeout_padding: timedelta = timedelta(minutes=3)
    replan: bool = False
    ai_settings: AiSettings = AiSettings(
        model=REQUIRED_CODEX_MODEL,
        reasoning_effort=REQUIRED_REASONING_EFFORT,
    )
    focus: str | None = None


DEFAULT_REPO_EXECUTION_CONFIG = RepoExecutionConfig()


@dataclass(frozen=True, slots=True)
class TickBudget:
    started_at: datetime
    ends_at: datetime

    def remaining(self, *, now: datetime) -> timedelta:
        if now.tzinfo is None:
            raise RepoExecutionError("TickBudget.remaining requires timezone-aware now.")
        return max(self.ends_at - now, timedelta(0))


@dataclass(frozen=True, slots=True)
class BeadResult:
    bead_id: str
    title: str
    outcome: BeadOutcome
    detail: str
    commit_hash: str | None = None


@dataclass(frozen=True, slots=True)
class RepoTickResult:
    repo_id: str
    run_id: str
    branch: str | None
    skipped: bool
    skip_reason: RepoSkipReason | None
    stop_reason: RepoStopReason | None
    beads_attempted: int
    beads_closed: int
    bead_results: tuple[BeadResult, ...]


@dataclass(slots=True)
class _CodexAttemptRuntime:
    pid: int | None = None
    heartbeat_stop: threading.Event | None = None
    heartbeat_thread: threading.Thread | None = None


def _start_codex_heartbeat(
    *,
    repo_id: str,
    bead_id: str,
    attempt: int,
    pid: int,
    started_at: datetime,
    timeout_seconds: float,
    log_path: Path,
    emit: Callable[..., None],
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _heartbeat() -> None:
        while not stop_event.wait(_CODEX_HEARTBEAT_INTERVAL_SECONDS):
            try:
                hb_now = _now()
                elapsed = (hb_now - started_at).total_seconds()
                remaining_seconds = max(0.0, timeout_seconds - elapsed)
                _append_log(
                    log_path,
                    f"{hb_now.isoformat()} codex_heartbeat bead_id={bead_id} "
                    f"attempt={attempt} pid={pid} elapsed={elapsed:.0f}s "
                    f"remaining={remaining_seconds:.0f}s",
                )
                emit(
                    "codex_heartbeat",
                    bead_id=bead_id,
                    attempt=attempt,
                    pid=pid,
                    elapsed_seconds=elapsed,
                    remaining_seconds=remaining_seconds,
                    timeout_seconds=timeout_seconds,
                )
            except Exception as hb_err:
                _append_log(
                    log_path,
                    f"{_now().isoformat()} codex_heartbeat_error bead_id={bead_id} "
                    f"attempt={attempt} pid={pid} error={type(hb_err).__name__}: {hb_err}",
                )
                break

    heartbeat_thread = threading.Thread(
        target=_heartbeat,
        name=f"codex-heartbeat-{repo_id}-{bead_id}-{attempt}",
        daemon=True,
    )
    heartbeat_thread.start()
    return stop_event, heartbeat_thread


def _make_codex_on_start_callback(
    *,
    runtime: _CodexAttemptRuntime,
    repo_id: str,
    bead_id: str,
    attempt: int,
    timeout_seconds: float,
    log_path: Path,
    emit: Callable[..., None],
) -> Callable[[int, tuple[str, ...], datetime], None]:
    def _on_codex_start(pid: int, argv: tuple[str, ...], started_at: datetime) -> None:
        runtime.pid = pid
        _append_log(
            log_path,
            f"{_now().isoformat()} codex_spawn bead_id={bead_id} "
            f"attempt={attempt} pid={pid}",
        )
        emit(
            "codex_spawn",
            bead_id=bead_id,
            attempt=attempt,
            pid=pid,
            started_at=started_at.isoformat(),
            argv=list(argv),
        )
        runtime.heartbeat_stop, runtime.heartbeat_thread = _start_codex_heartbeat(
            repo_id=repo_id,
            bead_id=bead_id,
            attempt=attempt,
            pid=pid,
            started_at=started_at,
            timeout_seconds=timeout_seconds,
            log_path=log_path,
            emit=emit,
        )

    return _on_codex_start


def _which(tool: str) -> str | None:
    # Avoid importing shutil in hot paths; lightweight PATH scan.
    for p in os.environ.get("PATH", "").split(os.pathsep):
        if not p:
            continue
        candidate = Path(p) / tool
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _require_tools(tools: Sequence[str]) -> None:
    missing = [t for t in tools if _which(t) is None]
    if missing:
        raise RepoExecutionError(
            "Missing required tools on PATH: " + ", ".join(sorted(set(missing)))
        )


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
    if first in {"pytest", "python", "python3", "ruff", "make", "nox", "pre-commit"}:
        return True
    return False


def _is_behavioral_test_command(command: str) -> bool:
    argv = _parse_command_argv(command)
    if argv is None:
        return False
    if argv[0] == "pytest":
        return True
    if (
        argv[0] in {"python", "python3"}
        and len(argv) >= 3
        and argv[1] == "-m"
        and argv[2] == "pytest"
    ):
        return True
    return False


def _require_validation_allowlist(commands: Sequence[str]) -> None:
    forbidden = [c for c in commands if c.strip() and not _validation_command_allowed(c)]
    if forbidden:
        raise RepoExecutionError(
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


def _format_codex_prompt(
    *,
    run_id: str,
    repo_policy: RepoPolicy,
    item: RunDeckItem,
    focus: str | None = None,
    validation_context: str | None = None,
) -> str:
    contract = item.contract
    allowed_roots = ", ".join(p.as_posix() for p in contract.allowed_roots)
    deny_roots = ", ".join(p.as_posix() for p in contract.deny_roots) or "<none>"
    validation = "\n".join(f"- {c}" for c in contract.validation_commands) or "<none>"
    env_label = contract.env.strip() if contract.env else "<none>"

    lines = [
        "You are working in a local git repository under an orchestrated run.",
        "",
        f"RUN_ID: {run_id}",
        f"REPO_ID: {repo_policy.repo_id}",
        f"BRANCH: run/{run_id}",
        "",
        f"BEAD: {item.bead_id} — {item.title}",
    ]

    if focus:
        lines.extend([
            "",
            "Focus area for this run:",
            f"{focus}",
            "",
            "Prioritize work that aligns with this focus. Interpret it semantically —",
            "the focus describes a domain or goal, not exact keywords.",
        ])

    constraint_lines = [
        "",
        "Constraints:",
        f"- Time budget: {contract.time_budget_minutes} minutes",
        f"- Conda env: {env_label}",
    ]
    if contract.env:
        constraint_lines.append(
            f"- Run diagnostics in this env (e.g., `conda run -n {env_label} pytest -q`)."
        )
    constraint_lines.extend(
        [
            f"- Allowed roots: {allowed_roots}",
            f"- Deny roots: {deny_roots}",
            "- Do not edit files outside allowed roots or under deny roots.",
            "- Do not create git commits; the orchestrator will commit.",
            "",
            "Validation commands (must pass to close):",
            validation,
        ]
    )
    lines.extend(constraint_lines)
    if validation_context:
        lines.extend([
            "",
            "Validation context:",
            *validation_context.rstrip().splitlines(),
        ])
    lines.extend([
        "",
        "Task:",
        f"- Complete bead {item.bead_id} ({item.title}) conservatively.",
        "- Make the minimal safe changes needed.",
        "- Ensure validation commands pass.",
        "- If validation fails due to missing modules, add deps in pyproject.toml",
        "  and/or environment*.yml; the orchestrator will refresh the env on changes.",
        "- If a validation tool is missing, install it in the configured conda env.",
        "",
        "Style:",
        "- Prefer idiomatic, readable code; avoid deep nesting.",
        "- In pandas: prefer method chaining and `DataFrame.query(...)`",
        "  over temporary boolean masks, and avoid intermediate filtered DataFrames.",
        "- For seaborn/matplotlib: prefer passing filtered data inline",
        "  (e.g. `sns.someplot(data=df.query(\"...\"), ...)`).",
    ])

    return "\n".join(lines)


MIN_VALIDATION_RETRY_SECONDS = 60.0


_MISSING_MODULE_PATTERNS = (
    re.compile(r"ModuleNotFoundError: No module named ['\"]([^'\"]+)['\"]"),
    re.compile(r"ImportError: No module named ['\"]([^'\"]+)['\"]"),
    re.compile(r"ImportError: cannot import name ['\"][^'\"]+['\"] from ['\"]([^'\"]+)['\"]"),
    re.compile(r"No module named ['\"]?([A-Za-z0-9_\\.]+)['\"]?"),
)


def _extract_missing_modules(text: str) -> list[str]:
    missing: set[str] = set()
    for line in text.splitlines():
        for pattern in _MISSING_MODULE_PATTERNS:
            match = pattern.search(line)
            if match:
                missing.add(match.group(1))
    return sorted(missing)


def _collect_missing_modules(validation_results: Mapping[str, ValidationResult]) -> list[str]:
    missing: set[str] = set()
    for result in validation_results.values():
        if result.exit_code == 0:
            continue
        combined = "\n".join([result.stdout, result.stderr]).strip()
        if not combined:
            continue
        missing.update(_extract_missing_modules(combined))
    return sorted(missing)


@dataclass(frozen=True, slots=True)
class _RuntimeBaselineSnapshot:
    results: tuple[ValidationResult, ...]
    failing_commands: tuple[str, ...]
    context: str | None


def _format_runtime_baseline_context(results: Sequence[ValidationResult]) -> str | None:
    failures = [r for r in results if r.exit_code != 0]
    if not failures:
        return None
    lines = ["Baseline validation failures (before this bead):"]
    for r in failures:
        lines.append(f"- {r.command}: exit={r.exit_code}")
    lines.append("Resolve these if possible (install missing tools or fix tests).")
    return "\n".join(lines)


def _capture_runtime_baseline(
    *,
    item: RunDeckItem,
    repo_root: Path,
    tick: TickBudget,
    bead_deadline: datetime,
    configured_timeout_seconds: float,
) -> _RuntimeBaselineSnapshot:
    _require_validation_allowlist(item.contract.validation_commands)
    timeout_seconds = _validation_timeout_seconds(
        commands=item.contract.validation_commands,
        remaining=_remaining_bead_time(
            tick=tick,
            now=_now(),
            bead_deadline=bead_deadline,
        ),
        configured_timeout_seconds=configured_timeout_seconds,
    )
    results_by_command = run_validation_commands(
        item.contract.validation_commands,
        cwd=repo_root,
        env=item.contract.env,
        timeout_seconds=timeout_seconds,
    )
    results = tuple(results_by_command.values())
    failing_commands = tuple(r.command for r in results if r.exit_code != 0)
    return _RuntimeBaselineSnapshot(
        results=results,
        failing_commands=failing_commands,
        context=_format_runtime_baseline_context(results),
    )


def _format_validation_retry_context(
    *,
    attempt: int,
    validation_results: Mapping[str, ValidationResult],
    baseline_failures: Sequence[str],
) -> str:
    failed = {cmd: r for cmd, r in validation_results.items() if r.exit_code != 0}
    lines = [f"Validation failures after attempt {attempt}:"]
    for cmd in sorted(failed):
        lines.append(f"- {cmd}: {_validation_status(failed[cmd].exit_code)}")
    missing = _collect_missing_modules(validation_results)
    if missing:
        lines.append("Missing modules detected:")
        lines.append("- " + ", ".join(missing))
    if baseline_failures:
        still = sorted(cmd for cmd in baseline_failures if cmd in failed)
        if still:
            lines.append("Baseline failures still present:")
            lines.extend(f"- {cmd}" for cmd in still)
    lines.append("Fix the failures above and re-run validations.")
    return "\n".join(lines)


def _truncate_note(text: str, *, limit: int = 1800) -> str:
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return text[:limit] + f"\n...<truncated {omitted} chars>"


def _format_validation_status_line(
    validation_results: Mapping[str, ValidationResult],
) -> str:
    if not validation_results:
        return "none"
    parts = [
        f"{cmd}={_validation_status(r.exit_code)}"
        for cmd, r in sorted(validation_results.items())
    ]
    return ", ".join(parts)


def _format_timeout_summary(
    *,
    run_id: str,
    bead_id: str,
    title: str,
    attempt: int,
    failed_commands: Sequence[str],
    baseline_failures: Sequence[str],
    validation_results: Mapping[str, ValidationResult],
    changed_paths: Sequence[str],
) -> str:
    tried = [
        f"codex_attempts={attempt}",
        f"validations={_format_validation_status_line(validation_results)}",
    ]
    problems: list[str] = ["time budget exhausted"]
    if failed_commands:
        problems.append(f"failed={', '.join(sorted(failed_commands))}")
    if baseline_failures:
        problems.append(f"baseline_failures={', '.join(sorted(baseline_failures))}")

    learned: list[str] = []
    missing_modules = _collect_missing_modules(validation_results)
    if missing_modules:
        learned.append(f"missing_modules={', '.join(missing_modules)}")
    if changed_paths:
        clipped = list(changed_paths[:_FOLLOWUP_MAX_CHANGED_PATHS])
        suffix = " …" if len(changed_paths) > _FOLLOWUP_MAX_CHANGED_PATHS else ""
        learned.append(f"changed_paths={', '.join(clipped)}{suffix}")
    if not learned:
        learned.append("no additional signals")

    lines = [
        f"{_TIMEOUT_SUMMARY_MARKER} RUN_ID={run_id} bead_id={bead_id} title={title!r}",
        f"Tried: {', '.join(tried)}",
        f"Problems: {', '.join(problems)}",
        f"Learned: {', '.join(learned)}",
    ]
    return _truncate_note("\n".join(lines))


def _format_followup_description(
    *,
    run_id: str,
    parent_bead_id: str,
    parent_title: str,
    summary: str,
    focus: str,
    trigger: str = "timeout",
) -> str:
    lines = [
        f"Derived from {parent_bead_id} ({parent_title}) after {trigger} in RUN_ID={run_id}.",
        f"Scope: {focus}",
        "",
        "Context:",
        summary,
    ]
    return _truncate_note("\n".join(lines))


def _format_diff_cap_summary(
    *,
    run_id: str,
    bead_id: str,
    title: str,
    attempt: int,
    cap_kind: str,
    files_changed: int,
    lines_added: int,
    tick_files_changed: int,
    tick_lines_added: int,
    max_files_changed: int,
    max_lines_added: int,
    changed_paths: Sequence[str],
) -> str:
    tried = [
        f"codex_attempts={attempt}",
        f"files_changed={files_changed}",
        f"lines_added={lines_added}",
    ]
    problems = [
        f"diff cap exceeded ({cap_kind})",
        f"tick_files_changed={tick_files_changed} max_files={max_files_changed}",
        f"tick_lines_added={tick_lines_added} max_lines={max_lines_added}",
    ]
    learned: list[str] = []
    if changed_paths:
        clipped = list(changed_paths[:_FOLLOWUP_MAX_CHANGED_PATHS])
        suffix = " …" if len(changed_paths) > _FOLLOWUP_MAX_CHANGED_PATHS else ""
        learned.append(f"changed_paths={', '.join(clipped)}{suffix}")
    else:
        learned.append("no changed_paths captured")

    lines = [
        f"{_DIFFCAP_SUMMARY_MARKER} RUN_ID={run_id} bead_id={bead_id} title={title!r}",
        f"Tried: {', '.join(tried)}",
        f"Problems: {', '.join(problems)}",
        f"Learned: {', '.join(learned)}",
    ]
    return _truncate_note("\n".join(lines))


def _maybe_decompose_timeout_bead(
    *,
    repo_root: Path,
    issue: Any,
    item: RunDeckItem,
    run_id: str,
    attempt: int,
    failed_commands: Sequence[str],
    baseline_failures: Sequence[str],
    validation_results: Mapping[str, ValidationResult],
    changed_paths: Sequence[str],
) -> tuple[str | None, tuple[str, ...]]:
    if issue.status in {"closed", "blocked"}:
        return None, ()
    if _DECOMPOSE_MARKER in (issue.notes or ""):
        return None, ()

    summary = _format_timeout_summary(
        run_id=run_id,
        bead_id=item.bead_id,
        title=item.title,
        attempt=attempt,
        failed_commands=failed_commands,
        baseline_failures=baseline_failures,
        validation_results=validation_results,
        changed_paths=changed_paths,
    )

    try:
        from codex_orchestrator.beads_subprocess import bd_create, bd_list_open_titles
    except Exception:
        return summary, ()

    try:
        open_titles = bd_list_open_titles(repo_root=repo_root)
    except Exception as e:
        return _truncate_note(summary + f"\n[orchestrator] Follow-up creation failed: {e}"), ()
    created_ids: list[str] = []

    priority = issue.priority if getattr(issue, "priority", None) is not None else 2
    issue_type = issue.issue_type if getattr(issue, "issue_type", None) in {
        "bug",
        "feature",
        "task",
        "epic",
        "chore",
    } else "task"
    deps = (f"discovered-from:{item.bead_id}",)

    def _create_followup(*, title: str, focus: str) -> None:
        if title in open_titles:
            return
        try:
            created = bd_create(
                repo_root=repo_root,
                title=title,
                issue_type=issue_type,
                priority=int(priority),
                description=_format_followup_description(
                    run_id=run_id,
                    parent_bead_id=item.bead_id,
                    parent_title=item.title,
                    summary=summary,
                    focus=focus,
                ),
                deps=deps,
            )
        except Exception:
            return
        created_ids.append(created.issue_id)

    if failed_commands:
        for cmd in sorted(set(failed_commands)):
            focus = f"Make `{cmd}` pass with minimal changes."
            title = f"Fix validation failure ({cmd}) for {item.title}"
            _create_followup(title=title, focus=focus)
    else:
        title = f"Break down next step for {item.title}"
        focus = "Identify the smallest concrete next change and capture a short plan."
        _create_followup(title=title, focus=focus)

    return summary, tuple(created_ids)


def _maybe_decompose_diff_cap_bead(
    *,
    repo_root: Path,
    issue: Any,
    item: RunDeckItem,
    run_id: str,
    attempt: int,
    cap_kind: str,
    files_changed: int,
    lines_added: int,
    tick_files_changed: int,
    tick_lines_added: int,
    max_files_changed: int,
    max_lines_added: int,
    changed_paths: Sequence[str],
) -> tuple[str | None, tuple[str, ...]]:
    if issue.status in {"closed", "blocked"}:
        return None, ()
    if _DECOMPOSE_MARKER in (issue.notes or ""):
        return None, ()

    summary = _format_diff_cap_summary(
        run_id=run_id,
        bead_id=item.bead_id,
        title=item.title,
        attempt=attempt,
        cap_kind=cap_kind,
        files_changed=files_changed,
        lines_added=lines_added,
        tick_files_changed=tick_files_changed,
        tick_lines_added=tick_lines_added,
        max_files_changed=max_files_changed,
        max_lines_added=max_lines_added,
        changed_paths=changed_paths,
    )

    try:
        from codex_orchestrator.beads_subprocess import bd_create, bd_list_open_titles
    except Exception:
        return summary, ()

    try:
        open_titles = bd_list_open_titles(repo_root=repo_root)
    except Exception as e:
        return _truncate_note(summary + f"\n[orchestrator] Follow-up creation failed: {e}"), ()
    created_ids: list[str] = []

    priority = issue.priority if getattr(issue, "priority", None) is not None else 2
    issue_type = issue.issue_type if getattr(issue, "issue_type", None) in {
        "bug",
        "feature",
        "task",
        "epic",
        "chore",
    } else "task"
    deps = (f"discovered-from:{item.bead_id}",)

    def _create_followup(*, title: str, focus: str) -> None:
        if title in open_titles:
            return
        try:
            created = bd_create(
                repo_root=repo_root,
                title=title,
                issue_type=issue_type,
                priority=int(priority),
                description=_format_followup_description(
                    run_id=run_id,
                    parent_bead_id=item.bead_id,
                    parent_title=item.title,
                    summary=summary,
                    focus=focus,
                    trigger="diff cap exceeded",
                ),
                deps=deps,
            )
        except Exception:
            return
        created_ids.append(created.issue_id)

    if changed_paths:
        groups: dict[str, list[str]] = {}
        for path in changed_paths:
            top = path.split("/", 1)[0]
            groups.setdefault(top, []).append(path)
        for top in sorted(groups)[:3]:
            focus = (
                "Reduce scope to stay within diff caps; focus changes under "
                f"`{top}` and re-run validations."
            )
            title = f"Reduce scope for {item.title}: {top}"
            _create_followup(title=title, focus=focus)
    else:
        title = f"Split {item.title} to fit diff caps"
        focus = "Break the work into smaller, cap-friendly steps with minimal changes per bead."
        _create_followup(title=title, focus=focus)

    return summary, tuple(created_ids)


def _append_issue_failure_note(
    *,
    repo_root: Path,
    issue_id: str,
    note: str,
    status: str | None = None,
    reopen_closed: bool = False,
) -> Any:
    from codex_orchestrator.beads_subprocess import bd_show, bd_update

    current_issue = bd_show(repo_root=repo_root, issue_id=issue_id)
    next_status = status
    prefix = ""
    if reopen_closed and current_issue.status == "closed":
        next_status = status if status is not None else "open"
        prefix = (
            "[orchestrator] Reopened because post-run checks failed after the bead was closed.\n"
        )
    merged_note = (current_issue.notes + "\n" if current_issue.notes else "") + prefix + note
    return bd_update(
        repo_root=repo_root,
        issue_id=issue_id,
        status=next_status,
        notes=merged_note,
    )


def _remaining_bead_time(
    *,
    tick: TickBudget,
    now: datetime,
    bead_deadline: datetime,
) -> timedelta:
    remaining_tick = tick.remaining(now=now)
    remaining_bead = max(bead_deadline - now, timedelta(0))
    return remaining_tick if remaining_tick <= remaining_bead else remaining_bead


def _can_retry_validation(*, tick: TickBudget, now: datetime, bead_deadline: datetime) -> bool:
    return _remaining_bead_time(
        tick=tick,
        now=now,
        bead_deadline=bead_deadline,
    ) >= timedelta(seconds=MIN_VALIDATION_RETRY_SECONDS)


def _validation_timeout_seconds(
    *,
    commands: Sequence[str],
    remaining: timedelta,
    configured_timeout_seconds: float,
) -> float:
    command_count = max(1, len({c.strip() for c in commands if c.strip()}))
    remaining_seconds = max(0.0, remaining.total_seconds())
    if remaining_seconds <= 0:
        return 1.0
    per_command_budget = remaining_seconds / float(command_count)
    return max(1.0, min(float(configured_timeout_seconds), per_command_budget))


def _append_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(message.rstrip("\n") + "\n")


def _write_text_if_missing(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")


def _validation_status(exit_code: int) -> str:
    return "ok" if exit_code == 0 else f"exit={exit_code}"


def _bead_skip_for_issue_status(status: str) -> tuple[BeadOutcome, str] | None:
    if status == "closed":
        return ("skipped_closed", "Issue already closed; skipping per conservative policy.")
    if status == "blocked":
        return ("skipped_blocked", "Issue is blocked; skipping per conservative policy.")
    if status not in {"open", "in_progress"}:
        return ("skipped_not_open", f"Unsupported status={status!r}; skipping.")
    return None


def _maybe_close_parent_epic(
    *,
    repo_root: Path,
    closed_issue: Any,
    run_id: str,
    run_branch: str,
    bd_show: Any,
    bd_update: Any,
    bd_close: Any,
) -> str | None:
    parent_id = getattr(closed_issue, "parent_id", None)
    if not isinstance(parent_id, str) or not parent_id.strip():
        return None
    parent_id = parent_id.strip()

    parent_issue = bd_show(repo_root=repo_root, issue_id=parent_id)
    if (getattr(parent_issue, "issue_type", None) or "").strip().lower() != "epic":
        return None
    if parent_issue.status.strip().lower() == "closed":
        return None

    child_ids: list[str] = []
    for link in getattr(parent_issue, "dependent_links", ()):
        dep_type = getattr(link, "dependency_type", None)
        if (dep_type or "").strip().lower() != "parent-child":
            continue
        child_id = getattr(link, "issue_id", None)
        if not isinstance(child_id, str) or not child_id.strip():
            continue
        child_ids.append(child_id.strip())

    if not child_ids:
        return None

    seen: set[str] = set()
    ordered_child_ids: list[str] = []
    for child_id in child_ids:
        if child_id in seen:
            continue
        seen.add(child_id)
        ordered_child_ids.append(child_id)

    for child_id in ordered_child_ids:
        child_issue = bd_show(repo_root=repo_root, issue_id=child_id)
        if child_issue.status.strip().lower() != "closed":
            return None

    parent_note = parent_issue.notes + ("\n" if parent_issue.notes else "")
    parent_note += (
        "[orchestrator] Auto-closed epic after all parent-child beads closed "
        f"in RUN_ID={run_id} on {run_branch}."
    )
    bd_update(repo_root=repo_root, issue_id=parent_id, notes=parent_note)
    bd_close(
        repo_root=repo_root,
        issue_id=parent_id,
        reason=f"All parent-child beads closed in RUN_ID={run_id} on {run_branch}",
    )
    return parent_id


def _infer_next_action(
    *,
    skipped: bool,
    skip_reason: RepoSkipReason | None,
    stop_reason: RepoStopReason | None,
    bead_audits: Sequence[Mapping[str, Any]],
) -> str:
    if skipped:
        return {
            "missing_tools": "Install required tools (git, bd, codex) and re-run.",
            "git_dirty": "Clean/stash the repo working tree (or set dirty_ignore_globs) and re-run.",
            "git_detached": "Checkout a branch (not detached HEAD) and re-run.",
            "git_fetch_failed": "Resolve git fetch failure (remotes/network) and re-run.",
            "git_branch_failed": "Resolve git branch setup failure and re-run.",
            "planning_failed": "Inspect planning error in exec log; fix and re-run.",
            "lock_busy": "Another repo tick is running; wait and retry.",
        }.get(skip_reason or "planning_failed", "Inspect logs and re-run.")

    last_failed = next((b for b in reversed(bead_audits) if b.get("outcome") == "failed"), None)
    if last_failed is not None:
        detail = str(last_failed.get("detail") or "").strip()
        if "Baseline failing validations" in detail:
            return "Fix baseline validation failures and re-run."
        if "Validation failed" in detail:
            return "Fix failing validation(s) and re-run."
        if "No behavioral test" in detail:
            return "Add/enable a behavioral test command in validation_commands and re-run."
        if "Given/When/Then" in detail:
            return "Add Given/When/Then markers to modified tests and re-run."
        if "Diff cap exceeded" in detail:
            return "Reduce scope or raise diff caps and re-run."
        if "Safety boundary violation" in detail:
            return "Adjust allowed_roots/deny_roots or reduce scope and re-run."
        return "Inspect failing bead details in logs; fix and re-run."

    if stop_reason == "tick_time_remaining":
        return "Increase tick budget or lower min_minutes_to_start_new_bead and re-run."
    if stop_reason == "bead_cap":
        return "Re-run to continue remaining beads (or raise max_beads_per_tick)."
    if stop_reason == "completed":
        return "Review changes and open PR(s)."
    if stop_reason == "error":
        return "Inspect exec log for error and re-run."
    if stop_reason == "blocked":
        return "Resolve blocker and re-run."
    return "Inspect logs."


def _write_run_summary(paths: OrchestratorPaths, *, run_id: str) -> None:
    run_dir = paths.run_dir(run_id)
    summaries: list[dict[str, Any]] = []
    for summary_path in sorted(run_dir.glob("*.summary.json")):
        try:
            data = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, dict):
            summaries.append(data)
    write_json_atomic(
        paths.run_summary_path(run_id),
        {"schema_version": 1, "run_id": run_id, "repos": summaries},
    )


def _load_json_object(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _summary_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _summary_list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _summary_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if text:
            out.append(text)
    return out


def _merge_unique_strings(*lists: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for values in lists:
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            merged.append(value)
    return merged


def _merge_records_by_keys(
    existing: list[dict[str, Any]],
    current: list[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index_by_key: dict[tuple[str, str], int] = {}

    def _record_key(item: Mapping[str, Any]) -> tuple[str, str] | None:
        for field in key_fields:
            value = item.get(field)
            if isinstance(value, str) and value.strip():
                return (field, value.strip())
        return None

    for source in (existing, current):
        for item in source:
            record = dict(item)
            record_key = _record_key(record)
            if record_key is None:
                merged.append(record)
                continue
            idx = index_by_key.get(record_key)
            if idx is None:
                index_by_key[record_key] = len(merged)
                merged.append(record)
                continue
            merged[idx].update(record)
    return merged


def _is_skipped_bead_outcome(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("skipped")


def _merge_bead_audits(
    existing: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index_by_bead_id: dict[str, int] = {}

    for item in existing:
        record = dict(item)
        bead_id = str(record.get("bead_id") or "").strip()
        if not bead_id:
            merged.append(record)
            continue
        index_by_bead_id[bead_id] = len(merged)
        merged.append(record)

    for item in current:
        record = dict(item)
        bead_id = str(record.get("bead_id") or "").strip()
        if not bead_id:
            merged.append(record)
            continue

        idx = index_by_bead_id.get(bead_id)
        if idx is None:
            index_by_bead_id[bead_id] = len(merged)
            merged.append(record)
            continue

        prior = dict(merged[idx])
        merged_record = dict(prior)
        merged_record.update(record)

        prior_outcome = prior.get("outcome")
        current_outcome = record.get("outcome")
        if _is_skipped_bead_outcome(current_outcome) and isinstance(prior_outcome, str):
            if prior_outcome and not _is_skipped_bead_outcome(prior_outcome):
                merged_record["outcome"] = prior_outcome
                if "detail" in prior:
                    merged_record["detail"] = prior.get("detail")
                else:
                    merged_record.pop("detail", None)

        merged[idx] = merged_record

    return merged


def _merge_notebook_refactors(existing: Any, current: Any) -> dict[str, list[str]]:
    existing_map = existing if isinstance(existing, dict) else {}
    current_map = current if isinstance(current, dict) else {}
    return {
        "notebooks": _merge_unique_strings(
            _summary_string_list(existing_map.get("notebooks")),
            _summary_string_list(current_map.get("notebooks")),
        ),
        "extracted_code": _merge_unique_strings(
            _summary_string_list(existing_map.get("extracted_code")),
            _summary_string_list(current_map.get("extracted_code")),
        ),
    }


def _merge_high_level_context(
    existing: Any,
    current: Any,
    *,
    planning_skipped: list[dict[str, Any]],
) -> dict[str, Any] | None:
    existing_map = existing if isinstance(existing, dict) else {}
    current_map = current if isinstance(current, dict) else {}

    focus = current_map.get("focus")
    if not isinstance(focus, str) or not focus.strip():
        focus = existing_map.get("focus")
    if not isinstance(focus, str) or not focus.strip():
        focus = None

    safety = current_map.get("safety")
    if not isinstance(safety, dict):
        safety = existing_map.get("safety") if isinstance(existing_map.get("safety"), dict) else None

    replan_requested = current_map.get("replan_requested")
    if not isinstance(replan_requested, bool):
        replan_requested = (
            existing_map.get("replan_requested")
            if isinstance(existing_map.get("replan_requested"), bool)
            else None
        )

    reused_existing_deck = current_map.get("reused_existing_deck")
    if not isinstance(reused_existing_deck, bool):
        reused_existing_deck = (
            existing_map.get("reused_existing_deck")
            if isinstance(existing_map.get("reused_existing_deck"), bool)
            else None
        )

    planned_beads = _merge_records_by_keys(
        _summary_list_of_dicts(existing_map.get("planned_beads")),
        _summary_list_of_dicts(current_map.get("planned_beads")),
        key_fields=("bead_id",),
    )

    if (
        focus is None
        and not planned_beads
        and safety is None
        and replan_requested is None
        and reused_existing_deck is None
        and not planning_skipped
    ):
        return None

    return {
        "focus": focus,
        "planned_beads": planned_beads,
        "replan_requested": replan_requested,
        "reused_existing_deck": reused_existing_deck,
        "planning_skipped_count": len(planning_skipped),
        "safety": safety,
    }


def _merge_repo_summary(
    existing: dict[str, Any] | None,
    current: dict[str, Any],
) -> dict[str, Any]:
    if existing is None:
        return current

    merged_beads = _merge_bead_audits(
        _summary_list_of_dicts(existing.get("beads")),
        _summary_list_of_dicts(current.get("beads")),
    )
    merged_planning_skipped = _merge_records_by_keys(
        _summary_list_of_dicts(existing.get("planning_skipped_beads")),
        _summary_list_of_dicts(current.get("planning_skipped_beads")),
        key_fields=("bead_id",),
    )
    merged_prompts = _merge_records_by_keys(
        _summary_list_of_dicts(existing.get("prompts")),
        _summary_list_of_dicts(current.get("prompts")),
        key_fields=("path", "bead_id"),
    )
    merged_validations = _merge_records_by_keys(
        _summary_list_of_dicts(existing.get("validations")),
        _summary_list_of_dicts(current.get("validations")),
        key_fields=("command",),
    )

    merged = dict(existing)
    merged.update(current)
    merged["beads_attempted"] = _summary_int(existing.get("beads_attempted")) + _summary_int(
        current.get("beads_attempted")
    )
    merged["beads_closed"] = _summary_int(existing.get("beads_closed")) + _summary_int(
        current.get("beads_closed")
    )
    merged["beads"] = merged_beads
    merged["planning_skipped_beads"] = merged_planning_skipped
    merged["failures"] = _merge_unique_strings(
        _summary_string_list(existing.get("failures")),
        _summary_string_list(current.get("failures")),
    )
    merged["follow_ups"] = _merge_unique_strings(
        _summary_string_list(existing.get("follow_ups")),
        _summary_string_list(current.get("follow_ups")),
    )
    merged["prompts"] = merged_prompts
    merged["validations"] = merged_validations
    merged["notebook_refactors"] = _merge_notebook_refactors(
        existing.get("notebook_refactors"),
        current.get("notebook_refactors"),
    )
    merged["high_level_context"] = _merge_high_level_context(
        existing.get("high_level_context"),
        current.get("high_level_context"),
        planning_skipped=merged_planning_skipped,
    )
    if current.get("run_report_path") is None and isinstance(existing.get("run_report_path"), str):
        merged["run_report_path"] = existing.get("run_report_path")
    return merged


def _count_lines_limited(path: Path, *, byte_limit: int = 2_000_000) -> int:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return 0
    except OSError:
        return 0
    if len(data) > byte_limit:
        data = data[:byte_limit]
    try:
        text = data.decode("utf-8", errors="ignore")
    except Exception:
        return 0
    return text.count("\n") + (1 if text and not text.endswith("\n") else 0)


_GWT_GIVEN_RE = re.compile(
    r"^[ \t]*(?:#|//|--|;|\*+|/\*+|<!--)?[ \t]*given\b",
    flags=re.IGNORECASE | re.MULTILINE,
)
_GWT_WHEN_RE = re.compile(
    r"^[ \t]*(?:#|//|--|;|\*+|/\*+|<!--)?[ \t]*when\b",
    flags=re.IGNORECASE | re.MULTILINE,
)
_GWT_THEN_RE = re.compile(
    r"^[ \t]*(?:#|//|--|;|\*+|/\*+|<!--)?[ \t]*then\b",
    flags=re.IGNORECASE | re.MULTILINE,
)


def _read_text_limited(path: Path, *, byte_limit: int = 2_000_000) -> str:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ""
    except OSError:
        return ""
    if len(data) > byte_limit:
        data = data[:byte_limit]
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _is_probable_test_path(path: str) -> bool:
    p = Path(path)
    name = p.name.lower()
    if not name or name in {"__init__.py", "conftest.py"}:
        return False

    if name.startswith("test_"):
        return True
    if name.endswith("_test.py") or name.endswith("_test.go") or name.endswith("_spec.rb"):
        return True
    if ".test." in name or ".spec." in name:
        return True

    parts_lower = {part.lower() for part in p.parts}
    if "__tests__" in parts_lower:
        return name.endswith((".js", ".jsx", ".ts", ".tsx"))

    return False


def _tests_missing_given_when_then(*, repo_root: Path, changed_paths: Sequence[str]) -> list[str]:
    missing: list[str] = []
    for raw in changed_paths:
        if not _is_probable_test_path(raw):
            continue
        path = repo_root / raw
        if not path.exists() or not path.is_file():
            continue
        text = _read_text_limited(path)
        if not text:
            missing.append(raw)
            continue
        if not (
            _GWT_GIVEN_RE.search(text)
            and _GWT_WHEN_RE.search(text)
            and _GWT_THEN_RE.search(text)
        ):
            missing.append(raw)
    return missing


def _diff_stats(
    *,
    repo_root: Path,
    dirty_ignore_globs: Sequence[str],
) -> tuple[int, int, tuple[str, ...]]:
    status = git_status_filtered(repo_root=repo_root, ignore_globs=dirty_ignore_globs)
    changed_paths = tuple(sorted({e.path for e in status if e.path}))

    tracked_numstat = git_diff_numstat(
        repo_root=repo_root,
        staged=False,
        ignore_globs=dirty_ignore_globs,
    )
    added_by_path: dict[str, int] = {p: added for p, added, _ in tracked_numstat}
    lines_added = sum(added_by_path.values())

    untracked = [e.path for e in status if e.xy == "??"]
    for raw in untracked:
        lines_added += _count_lines_limited(repo_root / raw)

    return (len(changed_paths), lines_added, changed_paths)


_PIP_EDITABLE_NAMES = {
    "pyproject.toml",
    "setup.cfg",
    "setup.py",
    "Pipfile",
    "Pipfile.lock",
}
_ENV_FILE_GLOBS = ("environment*.yml", "environment*.yaml")
_REQUIREMENTS_GLOBS = (
    "requirements*.txt",
    "requirements*.in",
    "constraints*.txt",
    "constraints*.in",
)


@dataclass(frozen=True, slots=True)
class DependencyChange:
    env_files: tuple[str, ...]
    requirements_files: tuple[str, ...]
    pip_editable: bool
    paths: tuple[str, ...]


def _normalize_path(raw: str) -> str:
    return raw.replace("\\", "/")


def _matches_any_glob(path: str, patterns: Sequence[str]) -> bool:
    name = Path(path).name
    for pattern in patterns:
        if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(name, pattern):
            return True
    return False


def _classify_dependency_changes(changed_paths: Sequence[str]) -> DependencyChange:
    env_files: list[str] = []
    requirements_files: list[str] = []
    paths: list[str] = []
    pip_editable = False
    for raw in changed_paths:
        path = _normalize_path(raw)
        name = Path(path).name
        if name in _PIP_EDITABLE_NAMES:
            pip_editable = True
            paths.append(path)
            continue
        if _matches_any_glob(path, _ENV_FILE_GLOBS):
            env_files.append(path)
            paths.append(path)
            continue
        if _matches_any_glob(path, _REQUIREMENTS_GLOBS):
            requirements_files.append(path)
            paths.append(path)
            continue
    return DependencyChange(
        env_files=tuple(sorted(set(env_files))),
        requirements_files=tuple(sorted(set(requirements_files))),
        pip_editable=pip_editable,
        paths=tuple(sorted(set(paths))),
    )


def _dependency_signature(repo_root: Path, paths: Sequence[str]) -> tuple[tuple[str, int, int], ...]:
    signature: list[tuple[str, int, int]] = []
    for raw in sorted(set(paths)):
        path = _normalize_path(raw)
        target = repo_root / path
        if not target.exists():
            signature.append((path, -1, -1))
            continue
        stat = target.stat()
        signature.append((path, int(stat.st_mtime_ns), int(stat.st_size)))
    return tuple(signature)


def _last_failed_bead(bead_audits: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for audit in reversed(bead_audits):
        if audit.get("outcome") == "failed":
            return audit
    return None


def _commit_failure_snapshot(
    *,
    repo_root: Path,
    run_id: str,
    bead_audits: Sequence[Mapping[str, Any]],
) -> str | None:
    failed = _last_failed_bead(bead_audits)
    if failed is None:
        return None
    bead_id = failed.get("bead_id", "<unknown>")
    title = failed.get("title", "Failed bead")
    subject = f"beads({bead_id}): {title} (failed)"
    body = f"RUN_ID: {run_id}\n\nFailure snapshot; bead did not close."
    git_stage_all(repo_root=repo_root)
    return git_commit(repo_root=repo_root, subject=subject, body=body)


def _ordered_remotes(remotes: Sequence[str]) -> list[str]:
    preferred = ("origin", "upstream")
    ordered: list[str] = []
    seen: set[str] = set()
    for name in preferred:
        if name in remotes and name not in seen:
            ordered.append(name)
            seen.add(name)
    for name in remotes:
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _resolve_base_ref(*, repo_root: Path, base_branch: str) -> str:
    if git_branch_exists(repo_root=repo_root, branch=base_branch):
        return base_branch

    remotes = _ordered_remotes(git_remotes(repo_root=repo_root))
    for remote in remotes:
        if git_remote_branch_exists(repo_root=repo_root, remote=remote, branch=base_branch):
            return f"refs/remotes/{remote}/{base_branch}"

    return base_branch


def _ensure_run_branch(
    *,
    repo_root: Path,
    run_id: str,
    base_branch: str,
    dirty_ignore_globs: Sequence[str],
) -> tuple[str, str | None]:
    try:
        if git_is_dirty(repo_root=repo_root, ignore_globs=dirty_ignore_globs):
            raise RepoExecutionError("Repo is dirty; refusing to run unattended work.")
        if git_head_is_detached(repo_root=repo_root):
            raise RepoExecutionError(
                "Repo is in detached HEAD state; refusing to run unattended work."
            )
    except GitError as e:
        raise RepoExecutionError(str(e)) from e

    fetch_error: str | None = None
    try:
        git_fetch(repo_root=repo_root)
    except GitError as e:
        fetch_error = str(e)

    run_branch = f"run/{run_id}"
    try:
        if git_branch_exists(repo_root=repo_root, branch=run_branch):
            git_checkout(repo_root=repo_root, ref=run_branch)
            return run_branch, fetch_error

        base_ref = _resolve_base_ref(repo_root=repo_root, base_branch=base_branch)
        git_checkout_new_branch(repo_root=repo_root, branch=run_branch, base_ref=base_ref)
    except GitError as e:
        raise RepoExecutionError(f"git branch setup failed: {e}") from e
    return run_branch, fetch_error


def _format_validation_summary(results: dict[str, ValidationResult]) -> str:
    lines: list[str] = []
    for cmd in sorted(results):
        r = results[cmd]
        status = "ok" if r.exit_code == 0 else f"exit={r.exit_code}"
        lines.append(f"- {cmd}: {status} ({(r.finished_at - r.started_at).total_seconds():.1f}s)")
    return "\n".join(lines)


def _commit_body(*, run_id: str, item: RunDeckItem, validation: dict[str, ValidationResult]) -> str:
    return "\n".join(
        [
            f"RUN_ID: {run_id}",
            f"BEAD_ID: {item.bead_id}",
            "",
            "Validation:",
            _format_validation_summary(validation) or "<none>",
        ]
    )


def _should_start_new_bead(*, tick: TickBudget, now: datetime, min_minutes: int) -> bool:
    remaining = tick.remaining(now=now)
    return remaining >= timedelta(minutes=min_minutes)


def _now() -> datetime:
    now = datetime.now().astimezone()
    if now.tzinfo is None:
        raise RepoExecutionError("Expected timezone-aware now datetime.")
    return now


def execute_repo_tick(
    *,
    paths: OrchestratorPaths,
    run_id: str,
    repo_policy: RepoPolicy,
    overlay_path: Path,
    tick: TickBudget | None = None,
    config: RepoExecutionConfig = DEFAULT_REPO_EXECUTION_CONFIG,
) -> RepoTickResult:
    if tick is None:
        started_at = _now()
        tick = TickBudget(started_at=started_at, ends_at=started_at + config.tick_budget)

    run_dir = paths.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    artifacts = RepoRunArtifacts(
        run_log_path=paths.run_log_path(run_id),
        exec_log_path=paths.repo_exec_log_path(run_id, repo_policy.repo_id),
        stdout_log_path=paths.repo_stdout_log_path(run_id, repo_policy.repo_id),
        stderr_log_path=paths.repo_stderr_log_path(run_id, repo_policy.repo_id),
        events_path=paths.repo_events_path(run_id, repo_policy.repo_id),
        summary_path=paths.repo_summary_path(run_id, repo_policy.repo_id),
        planning_audit_json_path=paths.repo_planning_audit_json_path(run_id, repo_policy.repo_id),
        planning_audit_md_path=paths.repo_planning_audit_md_path(run_id, repo_policy.repo_id),
    )
    for path in (
        artifacts.run_log_path,
        artifacts.exec_log_path,
        artifacts.stdout_log_path,
        artifacts.stderr_log_path,
        artifacts.events_path,
    ):
        _write_text_if_missing(path)

    tool_versions = collect_tool_versions(safe_cwd=paths.cache_dir)
    state = RepoExecutionState()
    events_lock = threading.Lock()

    def emit(event_type: str, **fields: Any) -> None:
        ts = _now().isoformat()
        payload = {
            "ts": ts,
            "type": event_type,
            "run_id": run_id,
            "repo_id": repo_policy.repo_id,
            **fields,
        }
        with events_lock:
            append_jsonl(artifacts.events_path, payload)

    reporter = RepoRunReporter(
        paths=paths,
        run_id=run_id,
        repo_policy=repo_policy,
        config=config,
        tool_versions=tool_versions,
        artifacts=artifacts,
        state=state,
        emit=emit,
        now_fn=_now,
        append_log_fn=_append_log,
    )

    def finalize(result: RepoTickResult) -> RepoTickResult:
        return reporter.finalize(result)

    lock_path = paths.repo_lock_path(repo_policy.repo_id)
    try:
        with RunLock(lock_path):
            try:
                prepared = prepare_repo_workspace(
                    repo_policy=repo_policy,
                    run_id=run_id,
                    emit=emit,
                    exec_log_path=artifacts.exec_log_path,
                    now_fn=_now,
                    append_log_fn=_append_log,
                    error_type=RepoExecutionError,
                )
            except RepoExecutionError as e:
                msg = str(e)
                if "dirty" in msg:
                    emit("repo_skipped", reason="git_dirty", error=msg)
                    return finalize(
                        RepoTickResult(
                            repo_id=repo_policy.repo_id,
                            run_id=run_id,
                            branch=None,
                            skipped=True,
                            skip_reason="git_dirty",
                            stop_reason=None,
                            beads_attempted=0,
                            beads_closed=0,
                            bead_results=(),
                        )
                    )
                elif "detached" in msg:
                    emit("repo_skipped", reason="git_detached", error=msg)
                    return finalize(
                        RepoTickResult(
                            repo_id=repo_policy.repo_id,
                            run_id=run_id,
                            branch=None,
                            skipped=True,
                            skip_reason="git_detached",
                            stop_reason=None,
                            beads_attempted=0,
                            beads_closed=0,
                            bead_results=(),
                        )
                    )
                elif "fetch" in msg:
                    emit("repo_skipped", reason="git_fetch_failed", error=msg)
                    return finalize(
                        RepoTickResult(
                            repo_id=repo_policy.repo_id,
                            run_id=run_id,
                            branch=None,
                            skipped=True,
                            skip_reason="git_fetch_failed",
                            stop_reason=None,
                            beads_attempted=0,
                            beads_closed=0,
                            bead_results=(),
                        )
                    )
                emit("repo_skipped", reason="git_branch_failed", error=msg)
                return finalize(
                    RepoTickResult(
                        repo_id=repo_policy.repo_id,
                        run_id=run_id,
                        branch=None,
                        skipped=True,
                        skip_reason="git_branch_failed",
                        stop_reason=None,
                        beads_attempted=0,
                        beads_closed=0,
                        bead_results=(),
                    )
                )

            run_branch = prepared.run_branch
            dirty_ignore_globs = prepared.dirty_ignore_globs
            if prepared.fetch_error:
                warning = f"git fetch failed; proceeding with local refs only: {prepared.fetch_error}"
                state.repo_failures.append(warning)
                emit("git_fetch_warning", error=prepared.fetch_error)
                _append_log(
                    artifacts.exec_log_path,
                    f"{_now().isoformat()} git_fetch_warning error={prepared.fetch_error}",
                )

            emit("repo_start", branch=run_branch, base_branch=repo_policy.base_branch)
            _append_log(
                artifacts.run_log_path,
                f"{_now().isoformat()} repo_start repo_id={repo_policy.repo_id} "
                f"branch={run_branch}",
            )
            log_path = artifacts.exec_log_path
            _append_log(
                log_path,
                f"{_now().isoformat()} repo_start repo_id={repo_policy.repo_id} "
                f"branch={run_branch}",
            )

            try:
                emit("planning_start", overlay_path=str(overlay_path), replan=config.replan)
                deck_plan = ensure_repo_run_deck(
                    paths=paths,
                    run_id=run_id,
                    repo_policy=repo_policy,
                    overlay_path=overlay_path,
                    replan=config.replan,
                    focus=config.focus,
                    now=_now(),
                )
                state.deck_path = deck_plan.deck_path
                state.reused_existing_deck = deck_plan.reused_existing_deck
                state.planned_scope = [
                    {"bead_id": item.bead_id, "title": item.title} for item in deck_plan.deck.items
                ]
                if deck_plan.planning is not None:
                    state.planning_skipped = [
                        {
                            "bead_id": skipped.bead_id,
                            "title": skipped.title,
                            "next_action": skipped.next_action,
                        }
                        for skipped in deck_plan.planning.skipped_beads
                    ]
                emit(
                    "planning_end",
                    deck_path=str(deck_plan.deck_path),
                    reused_existing_deck=deck_plan.reused_existing_deck,
                    planned=len(deck_plan.deck.items),
                    skipped=len(state.planning_skipped),
                )
            except Exception as e:
                _append_log(log_path, f"{_now().isoformat()} planning_failed error={e}")
                state.repo_failures.append(f"Planning failed: {e}")
                emit("planning_failed", error=str(e))
                was_clean_before_report = not git_is_dirty(
                    repo_root=repo_policy.path,
                    ignore_globs=dirty_ignore_globs,
                )
                reporter.maybe_write_repo_report(branch=run_branch)
                if was_clean_before_report and state.run_report_path is not None:
                    try:
                        git_stage_all(repo_root=repo_policy.path)
                        git_commit(
                            repo_root=repo_policy.path,
                            subject=f"run_report({run_id}): {repo_policy.repo_id}",
                            body=f"RUN_ID: {run_id}\n\nPlanning failed; see docs/runs/{run_id}.md",
                        )
                        state.run_report_committed = True
                    except GitError as commit_err:
                        state.repo_failures.append(f"Failed to commit run report: {commit_err}")
                return finalize(
                    RepoTickResult(
                        repo_id=repo_policy.repo_id,
                        run_id=run_id,
                        branch=run_branch,
                        skipped=True,
                        skip_reason="planning_failed",
                        stop_reason=None,
                        beads_attempted=0,
                        beads_closed=0,
                        bead_results=(),
                    )
                )

            bead_results = execute_planned_beads(
                deck_items=tuple(deck_plan.deck.items),
                repo_policy=repo_policy,
                run_id=run_id,
                run_branch=run_branch,
                config=config,
                tick=tick,
                paths=paths,
                dirty_ignore_globs=dirty_ignore_globs,
                reporter=reporter,
                state=state,
            )

            if state.stop_reason in {"blocked", "error"} and git_is_dirty(
                repo_root=repo_policy.path,
                ignore_globs=(),
            ):
                try:
                    snapshot_hash = _commit_failure_snapshot(
                        repo_root=repo_policy.path,
                        run_id=run_id,
                        bead_audits=state.bead_audits,
                    )
                except GitError as e:
                    state.repo_failures.append(f"Failure snapshot commit failed: {e}")
                else:
                    if snapshot_hash:
                        state.failure_snapshot_committed = True
                        failed = _last_failed_bead(state.bead_audits)
                        if failed is not None:
                            failed["commit_hash"] = snapshot_hash
                        emit(
                            "bead_failure_snapshot",
                            bead_id=(failed or {}).get("bead_id"),
                            commit_hash=snapshot_hash,
                        )

            _append_log(
                log_path,
                f"{_now().isoformat()} repo_end repo_id={repo_policy.repo_id} "
                f"attempted={state.beads_attempted} closed={state.beads_closed} "
                f"stop_reason={state.stop_reason}",
            )

            if state.beads_closed == 0 and not state.failure_snapshot_committed and not git_is_dirty(
                repo_root=repo_policy.path,
                ignore_globs=dirty_ignore_globs,
            ):
                if not state.run_report_committed:
                    reporter.maybe_write_repo_report(branch=run_branch)
                    if state.run_report_path is not None:
                        try:
                            git_stage_all(repo_root=repo_policy.path)
                            git_commit(
                                repo_root=repo_policy.path,
                                subject=f"run_report({run_id}): {repo_policy.repo_id}",
                                body=f"RUN_ID: {run_id}\n\nRun report: docs/runs/{run_id}.md",
                            )
                            state.run_report_committed = True
                        except GitError as commit_err:
                            state.repo_failures.append(f"Failed to commit run report: {commit_err}")

            return finalize(
                RepoTickResult(
                    repo_id=repo_policy.repo_id,
                    run_id=run_id,
                    branch=git_current_branch(repo_root=repo_policy.path),
                    skipped=False,
                    skip_reason=None,
                    stop_reason=state.stop_reason,
                    beads_attempted=state.beads_attempted,
                    beads_closed=state.beads_closed,
                    bead_results=tuple(bead_results),
                )
            )
    except RunLockError:
        emit("repo_skipped", reason="lock_busy")
        return finalize(
            RepoTickResult(
                repo_id=repo_policy.repo_id,
                run_id=run_id,
                branch=None,
                skipped=True,
                skip_reason="lock_busy",
                stop_reason=None,
                beads_attempted=0,
                beads_closed=0,
                bead_results=(),
            )
        )


def execute_repos_tick(
    *,
    paths: OrchestratorPaths,
    run_id: str,
    repos: Sequence[RepoPolicy],
    overlays_dir: Path,
    max_parallel: int,
    tick: TickBudget | None = None,
    config: RepoExecutionConfig = DEFAULT_REPO_EXECUTION_CONFIG,
) -> tuple[RepoTickResult, ...]:
    if max_parallel < 1:
        raise RepoExecutionError(f"max_parallel must be >= 1, got {max_parallel}")
    tick_budget = config.tick_budget
    if tick is not None:
        tick_budget = tick.ends_at - tick.started_at

    repo_list = list(repos)
    results: list[RepoTickResult] = []

    def _run_repo(policy: RepoPolicy) -> RepoTickResult:
        overlay_path = overlays_dir / f"{policy.repo_id}.toml"
        started_at = _now()
        repo_tick = TickBudget(started_at=started_at, ends_at=started_at + tick_budget)
        return execute_repo_tick(
            paths=paths,
            run_id=run_id,
            repo_policy=policy,
            overlay_path=overlay_path,
            tick=repo_tick,
            config=config,
        )

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = []
        for policy in repo_list:
            futures.append(pool.submit(_run_repo, policy))
        for fut in futures:
            results.append(fut.result())

    results.sort(key=lambda r: r.repo_id)
    return tuple(results)


def _require_validation_allowlist(commands: Sequence[str]) -> None:
    _baseline_impl._require_validation_allowlist(
        list(commands),
        error_type=RepoExecutionError,
    )


def _capture_runtime_baseline(
    *,
    item: RunDeckItem,
    repo_root: Path,
    tick: TickBudget,
    bead_deadline: datetime,
    configured_timeout_seconds: float,
) -> Any:
    return _baseline_impl._capture_runtime_baseline(
        item=item,
        repo_root=repo_root,
        tick=tick,
        bead_deadline=bead_deadline,
        configured_timeout_seconds=configured_timeout_seconds,
        run_validation_commands_fn=run_validation_commands,
        remaining_bead_time_fn=_remaining_bead_time,
        validation_timeout_seconds_fn=_validation_timeout_seconds,
        error_type=RepoExecutionError,
        now_fn=_now,
    )


_validation_command_allowed = _baseline_impl._validation_command_allowed
_is_behavioral_test_command = _baseline_impl._is_behavioral_test_command
_summarize_preflight_output = _baseline_impl._summarize_preflight_output
_format_runtime_baseline_context = _baseline_impl._format_runtime_baseline_context
_format_validation_retry_context = _baseline_impl._format_validation_retry_context
_remaining_bead_time = _baseline_impl._remaining_bead_time
_can_retry_validation = _baseline_impl._can_retry_validation
_validation_timeout_seconds = _baseline_impl._validation_timeout_seconds
_validation_status = _baseline_impl._validation_status
_format_validation_summary = _baseline_impl._format_validation_summary

_write_run_summary = _report_impl._write_run_summary
_load_json_object = _report_impl._load_json_object
_summary_int = _report_impl._summary_int
_summary_list_of_dicts = _report_impl._summary_list_of_dicts
_summary_string_list = _report_impl._summary_string_list
_merge_unique_strings = _report_impl._merge_unique_strings
_merge_records_by_keys = _report_impl._merge_records_by_keys
_is_skipped_bead_outcome = _report_impl._is_skipped_bead_outcome
_merge_bead_audits = _report_impl._merge_bead_audits
_merge_notebook_refactors = _report_impl._merge_notebook_refactors
_merge_high_level_context = _report_impl._merge_high_level_context
_merge_repo_summary = _report_impl._merge_repo_summary

write_text_atomic = _audit_trail.write_text_atomic
codex_exec_full_auto = _codex_subprocess.codex_exec_full_auto
CodexCliError = _codex_subprocess.CodexCliError
