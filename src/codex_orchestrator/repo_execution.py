from __future__ import annotations

import fnmatch
import json
import os
import re
import shlex
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from codex_orchestrator.ai_policy import (
    REQUIRED_CODEX_MODEL,
    REQUIRED_REASONING_EFFORT,
    AiSettings,
    codex_cli_args_for_settings,
)
from codex_orchestrator.audit_trail import (
    append_jsonl,
    collect_tool_versions,
    format_repo_run_report_md,
    write_json_atomic,
    write_repo_run_report,
    write_text_atomic,
)
from codex_orchestrator.codex_subprocess import CodexCliError, codex_exec_full_auto
from codex_orchestrator.env_bootstrap import refresh_repo_env
from codex_orchestrator.git_subprocess import (
    GitError,
    git_branch_exists,
    git_checkout,
    git_checkout_new_branch,
    git_commit,
    git_commit_amend_no_edit,
    git_current_branch,
    git_diff_numstat,
    git_fetch,
    git_head_is_detached,
    git_is_dirty,
    git_remove_ignored_untracked,
    git_rev_parse,
    resolve_dirty_ignore_globs,
    git_stage_all,
    git_status_filtered,
    validate_paths_within_policy,
)
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import RunDeckItem, ValidationResult
from codex_orchestrator.planning_pass import ensure_repo_run_deck
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
_DECOMPOSE_MARKER = "[orchestrator] Decomposed into follow-up beads"
_FOLLOWUP_MAX_CHANGED_PATHS = 8


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

    lines.extend([
        "",
        "Constraints:",
        f"- Time budget: {contract.time_budget_minutes} minutes",
        f"- Conda env: {env_label}",
        f"- Allowed roots: {allowed_roots}",
        f"- Deny roots: {deny_roots}",
        "- Do not edit files outside allowed roots or under deny roots.",
        "- Do not create git commits; the orchestrator will commit.",
        "",
        "Validation commands (must pass to close):",
        validation,
    ])
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


def _baseline_validation_context(item: RunDeckItem) -> str | None:
    failures = [r for r in item.baseline_validation if r.exit_code != 0]
    if not failures:
        return None
    lines = ["Baseline validation failures (before this bead):"]
    for r in failures:
        lines.append(f"- {r.command}: exit={r.exit_code}")
    lines.append("Resolve these if possible (install missing tools or fix tests).")
    return "\n".join(lines)


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
) -> str:
    lines = [
        f"Derived from {parent_bead_id} ({parent_title}) after timeout in RUN_ID={run_id}.",
        f"Scope: {focus}",
        "",
        "Context:",
        summary,
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


def _ensure_run_branch(
    *,
    repo_root: Path,
    run_id: str,
    base_branch: str,
    dirty_ignore_globs: Sequence[str],
) -> str:
    try:
        if git_is_dirty(repo_root=repo_root, ignore_globs=dirty_ignore_globs):
            raise RepoExecutionError("Repo is dirty; refusing to run unattended work.")
        if git_head_is_detached(repo_root=repo_root):
            raise RepoExecutionError(
                "Repo is in detached HEAD state; refusing to run unattended work."
            )
    except GitError as e:
        raise RepoExecutionError(str(e)) from e

    try:
        git_fetch(repo_root=repo_root)
    except GitError as e:
        raise RepoExecutionError(f"git fetch failed: {e}") from e

    run_branch = f"run/{run_id}"
    try:
        if git_branch_exists(repo_root=repo_root, branch=run_branch):
            git_checkout(repo_root=repo_root, ref=run_branch)
            return run_branch

        git_checkout(repo_root=repo_root, ref=base_branch)
        git_checkout_new_branch(repo_root=repo_root, branch=run_branch, base_ref=base_branch)
    except GitError as e:
        raise RepoExecutionError(f"git branch setup failed: {e}") from e
    return run_branch


def _baseline_by_command(item: RunDeckItem) -> dict[str, ValidationResult]:
    return {r.command: r for r in item.baseline_validation}


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

    run_log_path = paths.run_log_path(run_id)

    exec_log_path = paths.repo_exec_log_path(run_id, repo_policy.repo_id)
    stdout_log_path = paths.repo_stdout_log_path(run_id, repo_policy.repo_id)
    stderr_log_path = paths.repo_stderr_log_path(run_id, repo_policy.repo_id)
    events_path = paths.repo_events_path(run_id, repo_policy.repo_id)
    summary_path = paths.repo_summary_path(run_id, repo_policy.repo_id)
    for p in (run_log_path, exec_log_path, stdout_log_path, stderr_log_path, events_path):
        _write_text_if_missing(p)

    tool_versions = collect_tool_versions(safe_cwd=paths.cache_dir)
    bead_audits: list[dict[str, Any]] = []
    planning_skipped: list[dict[str, str]] = []
    validation_status_by_command: dict[str, str] = {}
    notebooks_touched: set[str] = set()
    extracted_code_touched: set[str] = set()
    repo_failures: list[str] = []
    follow_ups: list[str] = []
    prompt_records: list[dict[str, object]] = []
    run_report_path: Path | None = None
    run_report_committed: bool = False
    deck_path: Path | None = None
    reused_existing_deck: bool | None = None
    last_dependency_signature: tuple[tuple[str, int, int], ...] | None = None
    failure_snapshot_committed = False

    planning_audit_json_path = paths.repo_planning_audit_json_path(run_id, repo_policy.repo_id)
    planning_audit_md_path = paths.repo_planning_audit_md_path(run_id, repo_policy.repo_id)

    def emit(event_type: str, **fields: Any) -> None:
        ts = _now().isoformat()
        payload = {
            "ts": ts,
            "type": event_type,
            "run_id": run_id,
            "repo_id": repo_policy.repo_id,
            **fields,
        }
        append_jsonl(events_path, payload)

    def maybe_write_repo_report(*, branch: str | None) -> Path | None:
        nonlocal run_report_path
        if branch is None:
            return None

        rel_report = f"docs/runs/{run_id}.md"
        try:
            validate_paths_within_policy(
                paths=[rel_report],
                allowed_roots=repo_policy.allowed_roots,
                deny_roots=repo_policy.deny_roots,
            )
        except GitError as e:
            repo_failures.append(f"Run report not written: {e}")
            emit("run_report_skipped", reason=str(e))
            return None

        notebook_refactors = {
            "notebooks": sorted(notebooks_touched),
            "extracted_code": sorted(extracted_code_touched),
        }
        validations = [
            {"command": cmd, "status": status}
            for cmd, status in sorted(validation_status_by_command.items())
        ]
        codex_command = shlex.join(
            (
                "codex",
                "exec",
                "--full-auto",
                *codex_cli_args_for_settings(config.ai_settings),
            )
        )
        planning_audit = {
            "json_path": planning_audit_json_path.relative_to(paths.cache_dir).as_posix(),
            "md_path": planning_audit_md_path.relative_to(paths.cache_dir).as_posix(),
            "json_exists": planning_audit_json_path.exists(),
            "md_exists": planning_audit_md_path.exists(),
        }
        content = format_repo_run_report_md(
            repo_id=repo_policy.repo_id,
            run_id=run_id,
            branch=branch,
            planning_audit=planning_audit,
            ai_settings=config.ai_settings.to_json_dict(),
            codex_command=codex_command,
            prompts=prompt_records,
            beads=bead_audits,
            planning_skipped=planning_skipped,
            notebook_refactors=notebook_refactors,
            validations=validations,
            failures=repo_failures,
            follow_ups=follow_ups,
            tool_versions=tool_versions,
            generated_at=tick.started_at,
        )

        try:
            run_report_path = write_repo_run_report(
                repo_root=repo_policy.path,
                run_id=run_id,
                content=content,
            )
        except OSError as e:
            repo_failures.append(f"Run report write failed: {e}")
            emit("run_report_failed", error=str(e))
            return None
        emit("run_report_written", path=str(run_report_path))
        return run_report_path

    def finalize(result: RepoTickResult) -> RepoTickResult:
        next_action = _infer_next_action(
            skipped=result.skipped,
            skip_reason=result.skip_reason,
            stop_reason=result.stop_reason,
            bead_audits=bead_audits,
        )
        codex_argv = (
            "codex",
            "exec",
            "--full-auto",
            *codex_cli_args_for_settings(config.ai_settings),
        )
        summary = {
            "schema_version": 1,
            "run_id": run_id,
            "repo_id": repo_policy.repo_id,
            "repo_path": repo_policy.path.as_posix(),
            "branch": result.branch,
            "skipped": result.skipped,
            "skip_reason": result.skip_reason,
            "stop_reason": result.stop_reason,
            "beads_attempted": result.beads_attempted,
            "beads_closed": result.beads_closed,
            "deck_path": deck_path.as_posix() if deck_path is not None else None,
            "reused_existing_deck": reused_existing_deck,
            "planning_audit": {
                "json_path": planning_audit_json_path.as_posix(),
                "md_path": planning_audit_md_path.as_posix(),
                "json_exists": planning_audit_json_path.exists(),
                "md_exists": planning_audit_md_path.exists(),
            },
            "run_report_path": run_report_path.as_posix() if run_report_path is not None else None,
            "beads": bead_audits,
            "planning_skipped_beads": planning_skipped,
            "failures": repo_failures,
            "follow_ups": follow_ups,
            "ai_settings": config.ai_settings.to_json_dict(),
            "codex_command": shlex.join(codex_argv),
            "codex_argv": list(codex_argv),
            "tool_versions": tool_versions,
            "next_action": next_action,
        }
        write_json_atomic(summary_path, summary)
        _write_run_summary(paths, run_id=run_id)
        _append_log(
            run_log_path,
            f"{_now().isoformat()} repo_end repo_id={repo_policy.repo_id} "
            f"skipped={result.skipped} skip_reason={result.skip_reason} "
            f"stop_reason={result.stop_reason} attempted={result.beads_attempted} "
            f"closed={result.beads_closed}",
        )
        emit(
            "repo_end",
            skipped=result.skipped,
            skip_reason=result.skip_reason,
            stop_reason=result.stop_reason,
            beads_attempted=result.beads_attempted,
            beads_closed=result.beads_closed,
            next_action=next_action,
        )
        return result

    lock_path = paths.repo_lock_path(repo_policy.repo_id)
    try:
        with RunLock(lock_path):
            _require_tools(["git", "bd", "codex"])

            dirty_resolution = resolve_dirty_ignore_globs(
                repo_root=repo_policy.path,
                configured=repo_policy.dirty_ignore_globs,
            )
            dirty_ignore_globs = dirty_resolution.resolved
            if dirty_ignore_globs:
                emit("repo_dirty_ignore", globs=list(dirty_ignore_globs))
            if dirty_resolution.detected:
                emit("repo_dirty_ignore_detected", globs=list(dirty_resolution.detected))

            if repo_policy.dirty_cleanup and dirty_ignore_globs:
                try:
                    removed_paths = git_remove_ignored_untracked(
                        repo_root=repo_policy.path,
                        ignore_globs=dirty_ignore_globs,
                    )
                except GitError as e:
                    _append_log(
                        exec_log_path,
                        f"{_now().isoformat()} dirty_cleanup_failed error={e}",
                    )
                    emit("repo_dirty_cleanup_failed", error=str(e))
                else:
                    if removed_paths:
                        _append_log(
                            exec_log_path,
                            f"{_now().isoformat()} dirty_cleanup_removed count={len(removed_paths)}",
                        )
                        emit("repo_dirty_cleanup", removed=removed_paths)

            try:
                run_branch = _ensure_run_branch(
                    repo_root=repo_policy.path,
                    run_id=run_id,
                    base_branch=repo_policy.base_branch,
                    dirty_ignore_globs=dirty_ignore_globs,
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
                if "detached" in msg:
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
                if "fetch" in msg:
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

            emit("repo_start", branch=run_branch, base_branch=repo_policy.base_branch)
            _append_log(
                run_log_path,
                f"{_now().isoformat()} repo_start repo_id={repo_policy.repo_id} "
                f"branch={run_branch}",
            )
            log_path = exec_log_path
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
                    now=_now(),
                )
                deck_path = deck_plan.deck_path
                reused_existing_deck = deck_plan.reused_existing_deck
                if deck_plan.planning is not None:
                    planning_skipped = [
                        {
                            "bead_id": s.bead_id,
                            "title": s.title,
                            "next_action": s.next_action,
                        }
                        for s in deck_plan.planning.skipped_beads
                    ]
                emit(
                    "planning_end",
                    deck_path=str(deck_plan.deck_path),
                    reused_existing_deck=deck_plan.reused_existing_deck,
                    planned=len(deck_plan.deck.items),
                    skipped=len(planning_skipped),
                )
            except Exception as e:
                _append_log(log_path, f"{_now().isoformat()} planning_failed error={e}")
                repo_failures.append(f"Planning failed: {e}")
                emit("planning_failed", error=str(e))
                was_clean_before_report = not git_is_dirty(
                    repo_root=repo_policy.path,
                    ignore_globs=dirty_ignore_globs,
                )
                maybe_write_repo_report(branch=run_branch)
                if was_clean_before_report and run_report_path is not None:
                    try:
                        git_stage_all(repo_root=repo_policy.path)
                        git_commit(
                            repo_root=repo_policy.path,
                            subject=f"run_report({run_id}): {repo_policy.repo_id}",
                            body=f"RUN_ID: {run_id}\n\nPlanning failed; see docs/runs/{run_id}.md",
                        )
                        run_report_committed = True
                    except GitError as commit_err:
                        repo_failures.append(f"Failed to commit run report: {commit_err}")
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

            bead_results: list[BeadResult] = []
            beads_attempted = 0
            beads_closed = 0
            tick_files_changed = 0
            tick_lines_added = 0
            stop_reason: RepoStopReason | None = None

            for item in deck_plan.deck.items:
                now = _now()
                if beads_attempted >= config.max_beads_per_tick:
                    stop_reason = "bead_cap"
                    break
                if not _should_start_new_bead(
                    tick=tick, now=now, min_minutes=config.min_minutes_to_start_new_bead
                ):
                    stop_reason = "tick_time_remaining"
                    break

                try:
                    from codex_orchestrator.beads_subprocess import bd_close, bd_show, bd_update
                except Exception as e:  # pragma: no cover
                    raise RepoExecutionError(f"Failed to import bd wrappers: {e}") from e

                issue = bd_show(repo_root=repo_policy.path, issue_id=item.bead_id)
                if issue.status == "closed":
                    bead_audits.append(
                        {
                            "bead_id": item.bead_id,
                            "title": item.title,
                            "outcome": "skipped_closed",
                            "detail": "Issue already closed; skipping per conservative policy.",
                        }
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="skipped_closed",
                            detail="Issue already closed; skipping per conservative policy.",
                        )
                    )
                    continue
                if issue.status == "blocked":
                    bead_audits.append(
                        {
                            "bead_id": item.bead_id,
                            "title": item.title,
                            "outcome": "skipped_blocked",
                            "detail": "Issue is blocked; skipping per conservative policy.",
                        }
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="skipped_blocked",
                            detail="Issue is blocked; skipping per conservative policy.",
                        )
                    )
                    continue
                if issue.status not in {"open", "in_progress"}:
                    bead_audits.append(
                        {
                            "bead_id": item.bead_id,
                            "title": item.title,
                            "outcome": "skipped_not_open",
                            "detail": f"Unsupported status={issue.status!r}; skipping.",
                        }
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="skipped_not_open",
                            detail=f"Unsupported status={issue.status!r}; skipping.",
                        )
                    )
                    continue

                beads_attempted += 1
                if issue.status == "open":
                    bd_update(
                        repo_root=repo_policy.path,
                        issue_id=item.bead_id,
                        status="in_progress",
                    )

                head_before = git_rev_parse(repo_root=repo_policy.path)
                emit("bead_start", bead_id=item.bead_id, title=item.title)
                baseline_failures = tuple(
                    r.command for r in item.baseline_validation if r.exit_code != 0
                )
                validation_context = _baseline_validation_context(item)
                bead_started_at = _now()
                bead_deadline = bead_started_at + (
                    timedelta(minutes=item.contract.time_budget_minutes)
                    + config.codex_timeout_padding
                )
                attempt = 0

                while True:
                    attempt += 1
                    now = _now()
                    remaining = _remaining_bead_time(
                        tick=tick,
                        now=now,
                        bead_deadline=bead_deadline,
                    )
                    codex_prompt = _format_codex_prompt(
                        run_id=run_id,
                        repo_policy=repo_policy,
                        item=item,
                        focus=config.focus,
                        validation_context=validation_context,
                    )
                    prompt_path = paths.repo_prompt_path(
                        run_id,
                        repo_policy.repo_id,
                        item.bead_id,
                        attempt,
                    )
                    prompt_rel = prompt_path.relative_to(paths.cache_dir).as_posix()
                    write_text_atomic(prompt_path, codex_prompt)
                    prompt_records.append(
                        {
                            "bead_id": item.bead_id,
                            "attempt": attempt,
                            "path": prompt_rel,
                        }
                    )
                    emit(
                        "codex_prompt",
                        bead_id=item.bead_id,
                        attempt=attempt,
                        path=prompt_rel,
                    )
                    timeout_seconds = max(
                        60.0,
                        min(
                            remaining.total_seconds(),
                            (
                                timedelta(minutes=item.contract.time_budget_minutes)
                                + config.codex_timeout_padding
                            ).total_seconds(),
                        ),
                    )
                    _append_log(
                        log_path,
                        f"{_now().isoformat()} codex_start bead_id={item.bead_id} "
                        f"attempt={attempt} timeout={timeout_seconds:.0f}s",
                    )
                    codex_argv = (
                        "codex",
                        "exec",
                        "--full-auto",
                        *codex_cli_args_for_settings(config.ai_settings),
                    )
                    emit(
                        "codex_start",
                        bead_id=item.bead_id,
                        attempt=attempt,
                        timeout_seconds=timeout_seconds,
                        argv=list(codex_argv),
                    )
                    try:
                        codex_invocation = codex_exec_full_auto(
                            prompt=codex_prompt,
                            cwd=repo_policy.path,
                            timeout_seconds=timeout_seconds,
                            extra_args=codex_cli_args_for_settings(config.ai_settings),
                            output_limit_chars=config.codex_output_limit_chars,
                        )
                    except CodexCliError as e:
                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            notes=(issue.notes + "\n" if issue.notes else "")
                            + f"[orchestrator] codex invocation failed: {e}",
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail=f"codex CLI failed: {e}",
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": f"codex CLI failed: {e}",
                            }
                        )
                        repo_failures.append(f"codex failed for {item.bead_id}: {e}")
                        emit(
                            "codex_failed",
                            bead_id=item.bead_id,
                            attempt=attempt,
                            error=str(e),
                            argv=list(codex_argv),
                        )
                        stop_reason = "error"
                        was_clean_before_report = not git_is_dirty(
                            repo_root=repo_policy.path,
                            ignore_globs=dirty_ignore_globs,
                        )
                        maybe_write_repo_report(branch=run_branch)
                        if was_clean_before_report and run_report_path is not None:
                            try:
                                git_stage_all(repo_root=repo_policy.path)
                                git_commit(
                                    repo_root=repo_policy.path,
                                    subject=f"run_report({run_id}): {repo_policy.repo_id}",
                                    body=f"RUN_ID: {run_id}\n\nRun report: docs/runs/{run_id}.md",
                                )
                                run_report_committed = True
                            except GitError as commit_err:
                                repo_failures.append(f"Failed to commit run report: {commit_err}")
                        break

                    _append_log(
                        log_path,
                        f"{_now().isoformat()} codex_end bead_id={item.bead_id} "
                        f"attempt={attempt} exit={codex_invocation.exit_code}",
                    )
                    emit(
                        "codex_end",
                        bead_id=item.bead_id,
                        attempt=attempt,
                        exit_code=codex_invocation.exit_code,
                        started_at=codex_invocation.started_at.isoformat(),
                        finished_at=codex_invocation.finished_at.isoformat(),
                        argv=list(codex_invocation.args),
                    )
                    _append_log(log_path, codex_invocation.stdout)
                    _append_log(
                        stdout_log_path,
                        f"{_now().isoformat()} codex_stdout bead_id={item.bead_id} "
                        f"attempt={attempt} exit={codex_invocation.exit_code}",
                    )
                    _append_log(stdout_log_path, codex_invocation.stdout)
                    if codex_invocation.stderr.strip():
                        _append_log(log_path, "[stderr]")
                        _append_log(log_path, codex_invocation.stderr)
                        _append_log(
                            stderr_log_path,
                            f"{_now().isoformat()} codex_stderr bead_id={item.bead_id} "
                            f"attempt={attempt} exit={codex_invocation.exit_code}",
                        )
                        _append_log(stderr_log_path, codex_invocation.stderr)

                    head_after = git_rev_parse(repo_root=repo_policy.path)
                    if head_after != head_before:
                        raise RepoExecutionError(
                            "Policy violation: codex created commits; orchestrator must own commits."
                        )

                    files_changed, lines_added, changed_paths = _diff_stats(
                        repo_root=repo_policy.path,
                        dirty_ignore_globs=dirty_ignore_globs,
                    )
                    emit(
                        "diff_stats",
                        bead_id=item.bead_id,
                        attempt=attempt,
                        files_changed=files_changed,
                        lines_added=lines_added,
                        changed_paths=list(changed_paths),
                    )
                    if files_changed == 0:
                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            notes=(
                                (issue.notes + "\n" if issue.notes else "")
                                + "[orchestrator] No git changes detected after codex; "
                                "cannot commit/close."
                            ),
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail="No changes detected.",
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": "No changes detected.",
                                "changed_paths": list(changed_paths),
                            }
                        )
                        repo_failures.append(f"{item.bead_id}: no changes detected after codex.")
                        stop_reason = "blocked"
                        was_clean_before_report = not git_is_dirty(
                            repo_root=repo_policy.path,
                            ignore_globs=dirty_ignore_globs,
                        )
                        maybe_write_repo_report(branch=run_branch)
                        if was_clean_before_report and run_report_path is not None:
                            try:
                                git_stage_all(repo_root=repo_policy.path)
                                git_commit(
                                    repo_root=repo_policy.path,
                                    subject=f"run_report({run_id}): {repo_policy.repo_id}",
                                    body=f"RUN_ID: {run_id}\n\nRun report: docs/runs/{run_id}.md",
                                )
                                run_report_committed = True
                            except GitError as commit_err:
                                repo_failures.append(f"Failed to commit run report: {commit_err}")
                        break

                    try:
                        validate_paths_within_policy(
                            paths=changed_paths,
                            allowed_roots=item.contract.allowed_roots,
                            deny_roots=item.contract.deny_roots,
                        )
                    except GitError as e:
                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            notes=(issue.notes + "\n" if issue.notes else "") + f"[orchestrator] {e}",
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail=str(e),
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": str(e),
                                "changed_paths": list(changed_paths),
                            }
                        )
                        repo_failures.append(f"{item.bead_id}: {e}")
                        stop_reason = "blocked"
                        maybe_write_repo_report(branch=run_branch)
                        break

                    if tick_files_changed + files_changed > config.diff_caps.max_files_changed:
                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            notes=(issue.notes + "\n" if issue.notes else "")
                            + "[orchestrator] Diff cap exceeded: "
                            + f"tick_files_changed={tick_files_changed + files_changed} "
                            + f"max={config.diff_caps.max_files_changed}",
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail="Diff cap exceeded (files changed).",
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": "Diff cap exceeded (files changed).",
                                "changed_paths": list(changed_paths),
                            }
                        )
                        repo_failures.append(f"{item.bead_id}: diff cap exceeded (files changed).")
                        stop_reason = "blocked"
                        maybe_write_repo_report(branch=run_branch)
                        break

                    if tick_lines_added + lines_added > config.diff_caps.max_lines_added:
                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            notes=(issue.notes + "\n" if issue.notes else "")
                            + "[orchestrator] Diff cap exceeded: "
                            + f"tick_lines_added={tick_lines_added + lines_added} "
                            + f"max={config.diff_caps.max_lines_added}",
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail="Diff cap exceeded (lines added).",
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": "Diff cap exceeded (lines added).",
                                "changed_paths": list(changed_paths),
                            }
                        )
                        repo_failures.append(f"{item.bead_id}: diff cap exceeded (lines added).")
                        stop_reason = "blocked"
                        maybe_write_repo_report(branch=run_branch)
                        break

                    dependency_change = _classify_dependency_changes(changed_paths)
                    if dependency_change.paths and item.contract.env:
                        dep_signature = _dependency_signature(
                            repo_root=repo_policy.path,
                            paths=dependency_change.paths,
                        )
                        if dep_signature != last_dependency_signature:
                            emit(
                                "env_refresh_start",
                                env=item.contract.env,
                                env_files=list(dependency_change.env_files),
                                requirements_files=list(dependency_change.requirements_files),
                                pip_editable=dependency_change.pip_editable,
                            )
                            refresh_result = refresh_repo_env(
                                env_name=item.contract.env,
                                repo_root=repo_policy.path,
                                allow_env_creation=item.contract.allow_env_creation,
                                env_files=[repo_policy.path / p for p in dependency_change.env_files],
                                requirements_files=[
                                    repo_policy.path / p for p in dependency_change.requirements_files
                                ],
                                pip_editable=dependency_change.pip_editable,
                            )
                            emit(
                                "env_refresh_end",
                                env=item.contract.env,
                                env_files=list(dependency_change.env_files),
                                requirements_files=list(dependency_change.requirements_files),
                                pip_editable=dependency_change.pip_editable,
                                conda_update_attempted=refresh_result.conda_update_attempted,
                                conda_update_succeeded=refresh_result.conda_update_succeeded,
                                pip_install_attempted=refresh_result.pip_install_attempted,
                                pip_install_succeeded=refresh_result.pip_install_succeeded,
                                error=refresh_result.error,
                            )
                            if refresh_result.error:
                                bd_update(
                                    repo_root=repo_policy.path,
                                    issue_id=item.bead_id,
                                    notes=(issue.notes + "\n" if issue.notes else "")
                                    + f"[orchestrator] Env refresh failed: {refresh_result.error}",
                                )
                                bead_results.append(
                                    BeadResult(
                                        bead_id=item.bead_id,
                                        title=item.title,
                                        outcome="failed",
                                        detail=f"Env refresh failed: {refresh_result.error}",
                                    )
                                )
                                bead_audits.append(
                                    {
                                        "bead_id": item.bead_id,
                                        "title": item.title,
                                        "outcome": "failed",
                                        "detail": f"Env refresh failed: {refresh_result.error}",
                                        "changed_paths": list(changed_paths),
                                    }
                                )
                                repo_failures.append(
                                    f"{item.bead_id}: env refresh failed: {refresh_result.error}"
                                )
                                stop_reason = "blocked"
                                maybe_write_repo_report(branch=run_branch)
                                break
                            last_dependency_signature = dep_signature

                    try:
                        _require_validation_allowlist(item.contract.validation_commands)
                    except RepoExecutionError as e:
                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            notes=(issue.notes + "\n" if issue.notes else "")
                            + f"[orchestrator] {e}",
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail=str(e),
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": str(e),
                                "changed_paths": list(changed_paths),
                            }
                        )
                        repo_failures.append(f"{item.bead_id}: {e}")
                        stop_reason = "blocked"
                        maybe_write_repo_report(branch=run_branch)
                        break

                    validation_results = run_validation_commands(
                        item.contract.validation_commands,
                        cwd=repo_policy.path,
                        env=item.contract.env,
                        timeout_seconds=config.validation_timeout_seconds,
                    )
                    for cmd, r in validation_results.items():
                        validation_status_by_command[cmd] = _validation_status(r.exit_code)
                        _append_log(
                            stdout_log_path,
                            f"{_now().isoformat()} validation_stdout bead_id={item.bead_id} "
                            f"attempt={attempt} cmd={cmd} exit={r.exit_code}",
                        )
                        if r.stdout.strip():
                            _append_log(stdout_log_path, r.stdout)
                        _append_log(
                            stderr_log_path,
                            f"{_now().isoformat()} validation_stderr bead_id={item.bead_id} "
                            f"attempt={attempt} cmd={cmd} exit={r.exit_code}",
                        )
                        if r.stderr.strip():
                            _append_log(stderr_log_path, r.stderr)
                    emit(
                        "validation_end",
                        bead_id=item.bead_id,
                        attempt=attempt,
                        results={cmd: r.exit_code for cmd, r in validation_results.items()},
                    )
                    still_failing = sorted(
                        cmd
                        for cmd in baseline_failures
                        if validation_results.get(cmd) is not None
                        and validation_results[cmd].exit_code != 0
                    )
                    failed_commands = sorted(
                        cmd for cmd, r in validation_results.items() if r.exit_code != 0
                    )
                    if failed_commands:
                        if _can_retry_validation(
                            tick=tick,
                            now=_now(),
                            bead_deadline=bead_deadline,
                        ):
                            validation_context = _format_validation_retry_context(
                                attempt=attempt,
                                validation_results=validation_results,
                                baseline_failures=baseline_failures,
                            )
                            continue

                        attempt_note = f" after {attempt} attempt(s)" if attempt > 1 else ""
                        if still_failing:
                            detail = "Baseline failing validations still failing (time budget exhausted)."
                            note_prefix = (
                                "[orchestrator] Pre-existing failing validations remain failing; "
                                "time budget exhausted; cannot close.\n"
                            )
                            repo_failures.append(
                                f"{item.bead_id}: baseline failing validations still failing{attempt_note}."
                            )
                        else:
                            detail = "Validation failed (time budget exhausted)."
                            note_prefix = "[orchestrator] Validation failed; time budget exhausted.\n"
                            repo_failures.append(
                                f"{item.bead_id}: validation failed ({', '.join(failed_commands)})"
                                f"{attempt_note}."
                            )

                        timeout_summary, followup_ids = _maybe_decompose_timeout_bead(
                            repo_root=repo_policy.path,
                            issue=issue,
                            item=item,
                            run_id=run_id,
                            attempt=attempt,
                            failed_commands=failed_commands,
                            baseline_failures=baseline_failures,
                            validation_results=validation_results,
                            changed_paths=changed_paths,
                        )
                        extra_notes = ""
                        if timeout_summary:
                            extra_notes += "\n" + timeout_summary
                        if followup_ids:
                            extra_notes += (
                                "\n"
                                + _DECOMPOSE_MARKER
                                + ": "
                                + ", ".join(sorted(followup_ids))
                            )
                            follow_ups.append(
                                f"Timeout decomposition for {item.bead_id}: created "
                                + ", ".join(sorted(followup_ids))
                            )
                            if deck_path is not None and not config.replan:
                                try:
                                    deck_path.unlink(missing_ok=True)
                                    follow_ups.append(
                                        f"Cleared run deck to force replan: {deck_path.as_posix()}"
                                    )
                                except OSError as e:
                                    repo_failures.append(
                                        f"{item.bead_id}: failed to clear run deck {deck_path}: {e}"
                                    )

                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            status="blocked" if followup_ids else None,
                            notes=(issue.notes + "\n" if issue.notes else "")
                            + note_prefix
                            + _format_validation_summary(validation_results)
                            + extra_notes,
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail=detail,
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": detail,
                                "changed_paths": list(changed_paths),
                                "validation": {
                                    cmd: _validation_status(r.exit_code)
                                    for cmd, r in validation_results.items()
                                },
                                "followups": list(followup_ids) if followup_ids else [],
                            }
                        )
                        stop_reason = "blocked"
                        maybe_write_repo_report(branch=run_branch)
                        break

                    tick_files_changed += files_changed
                    tick_lines_added += lines_added
                    break

                if stop_reason is not None:
                    break

                if not any(
                    _is_behavioral_test_command(c)
                    for c in item.contract.validation_commands
                ):
                    bd_update(
                        repo_root=repo_policy.path,
                        issue_id=item.bead_id,
                        notes=(issue.notes + "\n" if issue.notes else "")
                        + "[orchestrator] No behavioral test command executed; cannot close.",
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="failed",
                            detail="No behavioral test executed.",
                        )
                    )
                    bead_audits.append(
                        {
                            "bead_id": item.bead_id,
                            "title": item.title,
                            "outcome": "failed",
                            "detail": "No behavioral test executed.",
                            "changed_paths": list(changed_paths),
                            "validation": {
                                cmd: _validation_status(r.exit_code)
                                for cmd, r in validation_results.items()
                            },
                        }
                    )
                    repo_failures.append(
                        f"{item.bead_id}: no behavioral test executed; cannot close."
                    )
                    stop_reason = "blocked"
                    maybe_write_repo_report(branch=run_branch)
                    break

                if item.contract.enforce_given_when_then:
                    missing_gwt = _tests_missing_given_when_then(
                        repo_root=repo_policy.path,
                        changed_paths=changed_paths,
                    )
                    if missing_gwt:
                        formatted = "\n".join(f"- {p}" for p in missing_gwt)
                        bd_update(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            notes=(issue.notes + "\n" if issue.notes else "")
                            + "[orchestrator] Given/When/Then markers missing in modified tests; "
                            "cannot close.\n"
                            + formatted,
                        )
                        bead_results.append(
                            BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail="Given/When/Then markers missing in modified tests.",
                            )
                        )
                        bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": "Given/When/Then markers missing in modified tests.",
                                "changed_paths": list(changed_paths),
                                "gwt_missing_paths": missing_gwt,
                                "validation": {
                                    cmd: _validation_status(r.exit_code)
                                    for cmd, r in validation_results.items()
                                },
                            }
                        )
                        repo_failures.append(
                            f"{item.bead_id}: Given/When/Then markers missing in modified tests."
                        )
                        stop_reason = "blocked"
                        maybe_write_repo_report(branch=run_branch)
                        break

                for p in changed_paths:
                    if p.endswith(".ipynb"):
                        notebooks_touched.add(p)
                    if p.endswith(".py"):
                        extracted_code_touched.add(p)

                dependents_updated = list(issue.dependents)
                if dependents_updated:
                    follow_ups.append(
                        f"Updated downstream bead notes for `{item.bead_id}`: "
                        + ", ".join(f"`{d}`" for d in dependents_updated)
                    )

                bead_audit: dict[str, Any] = {
                    "bead_id": item.bead_id,
                    "title": item.title,
                    "outcome": "closed",
                    "detail": "Closed successfully.",
                    "changed_paths": list(changed_paths),
                    "validation": {
                        cmd: _validation_status(r.exit_code)
                        for cmd, r in validation_results.items()
                    },
                    "dependents_updated": dependents_updated,
                }
                bead_audits.append(bead_audit)
                maybe_write_repo_report(branch=run_branch)

                subject = f"beads({item.bead_id}): {item.title}"
                try:
                    git_stage_all(repo_root=repo_policy.path)
                    commit_hash = git_commit(
                        repo_root=repo_policy.path,
                        subject=subject,
                        body=_commit_body(run_id=run_id, item=item, validation=validation_results),
                    )
                except GitError as e:
                    bd_update(
                        repo_root=repo_policy.path,
                        issue_id=item.bead_id,
                        notes=(issue.notes + "\n" if issue.notes else "") + f"[orchestrator] {e}",
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="failed",
                            detail=f"git commit failed: {e}",
                        )
                    )
                    bead_audit["outcome"] = "failed"
                    bead_audit["detail"] = f"git commit failed: {e}"
                    repo_failures.append(f"{item.bead_id}: git commit failed: {e}")
                    stop_reason = "blocked"
                    break

                summary_note = issue.notes + ("\n" if issue.notes else "")
                summary_note += (
                    f"[orchestrator] Closed in RUN_ID={run_id} on {run_branch}.\n"
                    + _format_validation_summary(validation_results)
                )
                bd_update(repo_root=repo_policy.path, issue_id=item.bead_id, notes=summary_note)
                close_reason = f"Completed in RUN_ID={run_id} on {run_branch}"
                bd_close(repo_root=repo_policy.path, issue_id=item.bead_id, reason=close_reason)

                for dependent_id in issue.dependents:
                    dep = bd_show(repo_root=repo_policy.path, issue_id=dependent_id)
                    dep_note = dep.notes + ("\n" if dep.notes else "")
                    dep_note += (
                        f"[orchestrator] Upstream {item.bead_id} closed in RUN_ID={run_id} "
                        f"on {run_branch}."
                    )
                    bd_update(repo_root=repo_policy.path, issue_id=dependent_id, notes=dep_note)

                try:
                    git_stage_all(repo_root=repo_policy.path)
                    commit_hash = git_commit_amend_no_edit(repo_root=repo_policy.path)
                except GitError as e:
                    repo_failures.append(f"{item.bead_id}: git commit amend failed: {e}")
                    bead_audit["outcome"] = "failed"
                    bead_audit["detail"] = f"git commit amend failed: {e}"
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="failed",
                            detail=f"git commit amend failed: {e}",
                        )
                    )
                    stop_reason = "error"
                    break

                bead_audit["commit_hash"] = commit_hash
                bead_results.append(
                    BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="closed",
                        detail="Closed successfully.",
                        commit_hash=commit_hash,
                    )
                )
                beads_closed += 1
                emit(
                    "bead_end",
                    bead_id=item.bead_id,
                    outcome="closed",
                    commit_hash=commit_hash,
                    dependents_updated=dependents_updated,
                )

            if stop_reason is None:
                stop_reason = "completed"

            if stop_reason in {"blocked", "error"} and git_is_dirty(
                repo_root=repo_policy.path,
                ignore_globs=(),
            ):
                try:
                    snapshot_hash = _commit_failure_snapshot(
                        repo_root=repo_policy.path,
                        run_id=run_id,
                        bead_audits=bead_audits,
                    )
                except GitError as e:
                    repo_failures.append(f"Failure snapshot commit failed: {e}")
                else:
                    if snapshot_hash:
                        failure_snapshot_committed = True
                        failed = _last_failed_bead(bead_audits)
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
                f"attempted={beads_attempted} closed={beads_closed} stop_reason={stop_reason}",
            )

            if beads_closed == 0 and not failure_snapshot_committed and not git_is_dirty(
                repo_root=repo_policy.path,
                ignore_globs=dirty_ignore_globs,
            ):
                if not run_report_committed:
                    maybe_write_repo_report(branch=run_branch)
                    if run_report_path is not None:
                        try:
                            git_stage_all(repo_root=repo_policy.path)
                            git_commit(
                                repo_root=repo_policy.path,
                                subject=f"run_report({run_id}): {repo_policy.repo_id}",
                                body=f"RUN_ID: {run_id}\n\nRun report: docs/runs/{run_id}.md",
                            )
                            run_report_committed = True
                        except GitError as commit_err:
                            repo_failures.append(f"Failed to commit run report: {commit_err}")

            return finalize(
                RepoTickResult(
                repo_id=repo_policy.repo_id,
                run_id=run_id,
                branch=git_current_branch(repo_root=repo_policy.path),
                skipped=False,
                skip_reason=None,
                stop_reason=stop_reason,
                beads_attempted=beads_attempted,
                beads_closed=beads_closed,
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
