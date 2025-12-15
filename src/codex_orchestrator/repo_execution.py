from __future__ import annotations

import os
import shlex
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Sequence

from codex_orchestrator.codex_subprocess import CodexCliError, codex_exec_full_auto
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
    git_rev_parse,
    git_stage_all,
    git_status_porcelain,
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
    if argv[0] in {"python", "python3"} and len(argv) >= 3 and argv[1] == "-m" and argv[2] == "pytest":
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
) -> str:
    contract = item.contract
    allowed_roots = ", ".join(p.as_posix() for p in contract.allowed_roots)
    deny_roots = ", ".join(p.as_posix() for p in contract.deny_roots) or "<none>"
    validation = "\n".join(f"- {c}" for c in contract.validation_commands) or "<none>"
    return "\n".join(
        [
            "You are working in a local git repository under an orchestrated run.",
            "",
            f"RUN_ID: {run_id}",
            f"REPO_ID: {repo_policy.repo_id}",
            f"BRANCH: run/{run_id}",
            "",
            f"BEAD: {item.bead_id} — {item.title}",
            "",
            "Constraints:",
            f"- Time budget: {contract.time_budget_minutes} minutes",
            f"- Allowed roots: {allowed_roots}",
            f"- Deny roots: {deny_roots}",
            "- Do not edit files outside allowed roots or under deny roots.",
            "- Do not create git commits; the orchestrator will commit.",
            "",
            "Validation commands (must pass to close):",
            validation,
            "",
            "Task:",
            f"- Complete bead {item.bead_id} ({item.title}) conservatively.",
            "- Make the minimal safe changes needed.",
            "- Ensure validation commands pass.",
        ]
    )


def _append_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(message.rstrip("\n") + "\n")


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


def _diff_stats(*, repo_root: Path) -> tuple[int, int, tuple[str, ...]]:
    status = git_status_porcelain(repo_root=repo_root)
    changed_paths = tuple(sorted({e.path for e in status if e.path}))

    tracked_numstat = git_diff_numstat(repo_root=repo_root, staged=False)
    added_by_path: dict[str, int] = {p: added for p, added, _ in tracked_numstat}
    lines_added = sum(added_by_path.values())

    untracked = [e.path for e in status if e.xy == "??"]
    for raw in untracked:
        lines_added += _count_lines_limited(repo_root / raw)

    return (len(changed_paths), lines_added, changed_paths)


def _ensure_run_branch(*, repo_root: Path, run_id: str, base_branch: str) -> str:
    try:
        if git_is_dirty(repo_root=repo_root):
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
    config: RepoExecutionConfig = RepoExecutionConfig(),
) -> RepoTickResult:
    if tick is None:
        started_at = _now()
        tick = TickBudget(started_at=started_at, ends_at=started_at + config.tick_budget)

    lock_path = paths.repo_lock_path(repo_policy.repo_id)
    try:
        with RunLock(lock_path):
            _require_tools(["git", "bd", "codex"])

            try:
                run_branch = _ensure_run_branch(
                    repo_root=repo_policy.path,
                    run_id=run_id,
                    base_branch=repo_policy.base_branch,
                )
            except RepoExecutionError as e:
                msg = str(e)
                if "dirty" in msg:
                    return RepoTickResult(
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
                if "detached" in msg:
                    return RepoTickResult(
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
                if "fetch" in msg:
                    return RepoTickResult(
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
                return RepoTickResult(
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

            log_path = paths.run_dir(run_id) / f"{repo_policy.repo_id}.exec.log"
            _append_log(
                log_path,
                f"{_now().isoformat()} repo_start repo_id={repo_policy.repo_id} branch={run_branch}",
            )

            try:
                deck_plan = ensure_repo_run_deck(
                    paths=paths,
                    run_id=run_id,
                    repo_policy=repo_policy,
                    overlay_path=overlay_path,
                    replan=False,
                    now=_now(),
                )
            except Exception as e:
                _append_log(log_path, f"{_now().isoformat()} planning_failed error={e}")
                return RepoTickResult(
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
                    from codex_orchestrator.beads_subprocess import bd_show, bd_update, bd_close
                except Exception as e:  # pragma: no cover
                    raise RepoExecutionError(f"Failed to import bd wrappers: {e}") from e

                issue = bd_show(repo_root=repo_policy.path, issue_id=item.bead_id)
                if issue.status == "closed":
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
                    bd_update(repo_root=repo_policy.path, issue_id=item.bead_id, status="in_progress")

                head_before = git_rev_parse(repo_root=repo_policy.path)
                codex_prompt = _format_codex_prompt(run_id=run_id, repo_policy=repo_policy, item=item)
                timeout_seconds = max(
                    60.0,
                    min(
                        (tick.ends_at - now).total_seconds(),
                        (timedelta(minutes=item.contract.time_budget_minutes) + config.codex_timeout_padding).total_seconds(),
                    ),
                )
                _append_log(
                    log_path,
                    f"{_now().isoformat()} codex_start bead_id={item.bead_id} timeout={timeout_seconds:.0f}s",
                )
                try:
                    codex_invocation = codex_exec_full_auto(
                        prompt=codex_prompt,
                        cwd=repo_policy.path,
                        timeout_seconds=timeout_seconds,
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
                    stop_reason = "error"
                    break

                _append_log(
                    log_path,
                    f"{_now().isoformat()} codex_end bead_id={item.bead_id} exit={codex_invocation.exit_code}",
                )
                _append_log(log_path, codex_invocation.stdout)
                if codex_invocation.stderr.strip():
                    _append_log(log_path, "[stderr]")
                    _append_log(log_path, codex_invocation.stderr)

                head_after = git_rev_parse(repo_root=repo_policy.path)
                if head_after != head_before:
                    raise RepoExecutionError(
                        "Policy violation: codex created commits; orchestrator must own commits."
                    )

                files_changed, lines_added, changed_paths = _diff_stats(repo_root=repo_policy.path)
                if files_changed == 0:
                    bd_update(
                        repo_root=repo_policy.path,
                        issue_id=item.bead_id,
                        notes=(issue.notes + "\n" if issue.notes else "")
                        + "[orchestrator] No git changes detected after codex; cannot commit/close.",
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="failed",
                            detail="No changes detected.",
                        )
                    )
                    stop_reason = "blocked"
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
                    stop_reason = "blocked"
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
                    stop_reason = "blocked"
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
                    stop_reason = "blocked"
                    break

                tick_files_changed += files_changed
                tick_lines_added += lines_added

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
                    stop_reason = "blocked"
                    break

                validation_results = run_validation_commands(
                    item.contract.validation_commands,
                    cwd=repo_policy.path,
                    timeout_seconds=config.validation_timeout_seconds,
                )
                baseline = _baseline_by_command(item)
                baseline_failures = sorted(
                    cmd for cmd, r in baseline.items() if cmd in validation_results and r.exit_code != 0
                )
                still_failing = sorted(
                    cmd
                    for cmd in baseline_failures
                    if validation_results.get(cmd) is not None and validation_results[cmd].exit_code != 0
                )
                if still_failing:
                    bd_update(
                        repo_root=repo_policy.path,
                        issue_id=item.bead_id,
                        notes=(issue.notes + "\n" if issue.notes else "")
                        + "[orchestrator] Pre-existing failing validations remain failing; cannot close.\n"
                        + _format_validation_summary(validation_results),
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="failed",
                            detail="Baseline failing validations still failing.",
                        )
                    )
                    stop_reason = "blocked"
                    break

                failures = sorted(cmd for cmd, r in validation_results.items() if r.exit_code != 0)
                if failures:
                    bd_update(
                        repo_root=repo_policy.path,
                        issue_id=item.bead_id,
                        notes=(issue.notes + "\n" if issue.notes else "")
                        + "[orchestrator] Validation failed; stopping.\n"
                        + _format_validation_summary(validation_results),
                    )
                    bead_results.append(
                        BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="failed",
                            detail="Validation failed.",
                        )
                    )
                    stop_reason = "blocked"
                    break

                if not any(_is_behavioral_test_command(c) for c in item.contract.validation_commands):
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
                    stop_reason = "blocked"
                    break

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
                    stop_reason = "blocked"
                    break

                summary_note = issue.notes + ("\n" if issue.notes else "")
                summary_note += (
                    f"[orchestrator] Closed in RUN_ID={run_id} on {run_branch} "
                    f"(commit {commit_hash[:12]}).\n"
                    + _format_validation_summary(validation_results)
                )
                bd_update(repo_root=repo_policy.path, issue_id=item.bead_id, notes=summary_note)
                close_reason = f"Completed in RUN_ID={run_id} (commit {commit_hash[:12]})"
                bd_close(repo_root=repo_policy.path, issue_id=item.bead_id, reason=close_reason)

                for dependent_id in issue.dependents:
                    dep = bd_show(repo_root=repo_policy.path, issue_id=dependent_id)
                    dep_note = dep.notes + ("\n" if dep.notes else "")
                    dep_note += (
                        f"[orchestrator] Upstream {item.bead_id} closed in RUN_ID={run_id} "
                        f"on {run_branch} (commit {commit_hash[:12]})."
                    )
                    bd_update(repo_root=repo_policy.path, issue_id=dependent_id, notes=dep_note)

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

            if stop_reason is None:
                stop_reason = "completed"

            _append_log(
                log_path,
                f"{_now().isoformat()} repo_end repo_id={repo_policy.repo_id} "
                f"attempted={beads_attempted} closed={beads_closed} stop_reason={stop_reason}",
            )

            return RepoTickResult(
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
    except RunLockError:
        return RepoTickResult(
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


def execute_repos_tick(
    *,
    paths: OrchestratorPaths,
    run_id: str,
    repos: Sequence[RepoPolicy],
    overlays_dir: Path,
    max_parallel: int,
    tick: TickBudget | None = None,
    config: RepoExecutionConfig = RepoExecutionConfig(),
) -> tuple[RepoTickResult, ...]:
    if max_parallel < 1:
        raise RepoExecutionError(f"max_parallel must be >= 1, got {max_parallel}")
    if tick is None:
        started_at = _now()
        tick = TickBudget(started_at=started_at, ends_at=started_at + config.tick_budget)

    repo_list = list(repos)
    results: list[RepoTickResult] = []
    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = []
        for policy in repo_list:
            overlay_path = overlays_dir / f"{policy.repo_id}.toml"
            futures.append(
                pool.submit(
                    execute_repo_tick,
                    paths=paths,
                    run_id=run_id,
                    repo_policy=policy,
                    overlay_path=overlay_path,
                    tick=tick,
                    config=config,
                )
            )
        for fut in futures:
            results.append(fut.result())

    results.sort(key=lambda r: r.repo_id)
    return tuple(results)
