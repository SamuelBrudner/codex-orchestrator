from __future__ import annotations

from datetime import timedelta
from typing import Any

from codex_orchestrator.ai_policy import codex_cli_args_for_settings
from codex_orchestrator.codex_subprocess import CodexCliError
from codex_orchestrator.env_bootstrap import refresh_repo_env
from codex_orchestrator.git_subprocess import (
    GitError,
    git_commit,
    git_commit_amend_no_edit,
    git_is_dirty,
    git_rev_parse,
    git_stage_all,
    validate_paths_within_policy,
)


def _maybe_commit_run_report_if_clean(
    *,
    reporter: Any,
    state: Any,
    repo_policy: Any,
    run_id: str,
    run_branch: str,
    dirty_ignore_globs: tuple[str, ...] | list[str],
) -> None:
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
                body=f"RUN_ID: {run_id}\n\nRun report: docs/runs/{run_id}.md",
            )
            state.run_report_committed = True
        except GitError as commit_err:
            state.repo_failures.append(f"Failed to commit run report: {commit_err}")


def execute_planned_beads(
    *,
    deck_items: tuple[Any, ...] | list[Any],
    repo_policy: Any,
    run_id: str,
    run_branch: str,
    config: Any,
    tick: Any,
    paths: Any,
    dirty_ignore_globs: tuple[str, ...] | list[str],
    reporter: Any,
    state: Any,
) -> tuple[Any, ...]:
    import codex_orchestrator.repo_execution as repo_execution

    log_path = reporter.artifacts.exec_log_path
    stdout_log_path = reporter.artifacts.stdout_log_path
    stderr_log_path = reporter.artifacts.stderr_log_path

    bead_results: list[Any] = []
    tick_files_changed = 0
    tick_lines_added = 0

    for item in deck_items:
        now = repo_execution._now()
        if state.beads_attempted >= config.max_beads_per_tick:
            state.stop_reason = "bead_cap"
            break
        if not repo_execution._should_start_new_bead(
            tick=tick,
            now=now,
            min_minutes=config.min_minutes_to_start_new_bead,
        ):
            state.stop_reason = "tick_time_remaining"
            break

        try:
            from codex_orchestrator.beads_subprocess import bd_close, bd_show, bd_update
        except Exception as e:  # pragma: no cover
            raise repo_execution.RepoExecutionError(f"Failed to import bd wrappers: {e}") from e

        issue = bd_show(repo_root=repo_policy.path, issue_id=item.bead_id)
        skip_for_status = repo_execution._bead_skip_for_issue_status(issue.status)
        if skip_for_status is not None:
            outcome, detail = skip_for_status
            state.bead_audits.append(
                {
                    "bead_id": item.bead_id,
                    "title": item.title,
                    "outcome": outcome,
                    "detail": detail,
                }
            )
            bead_results.append(
                repo_execution.BeadResult(
                    bead_id=item.bead_id,
                    title=item.title,
                    outcome=outcome,
                    detail=detail,
                )
            )
            continue

        state.beads_attempted += 1
        if issue.status == "open":
            bd_update(repo_root=repo_policy.path, issue_id=item.bead_id, status="in_progress")

        head_before = git_rev_parse(repo_root=repo_policy.path)
        reporter.emit("bead_start", bead_id=item.bead_id, title=item.title)
        bead_started_at = repo_execution._now()
        bead_deadline = bead_started_at + (
            timedelta(minutes=item.contract.time_budget_minutes) + config.codex_timeout_padding
        )
        attempt = 0
        env_preflight_checked = False
        stop_retry_attempts = False
        runtime_baseline: Any = None
        validation_context: str | None = None
        changed_paths: tuple[str, ...] = ()
        validation_results: dict[str, Any] = {}

        while True:
            issue = bd_show(repo_root=repo_policy.path, issue_id=item.bead_id)
            skip_for_status = repo_execution._bead_skip_for_issue_status(issue.status)
            if skip_for_status is not None:
                outcome, detail = skip_for_status
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": outcome,
                        "detail": detail,
                    }
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome=outcome,
                        detail=detail,
                    )
                )
                stop_retry_attempts = True
                break

            if runtime_baseline is None:
                try:
                    runtime_baseline = repo_execution._capture_runtime_baseline(
                        item=item,
                        repo_root=repo_policy.path,
                        tick=tick,
                        bead_deadline=bead_deadline,
                        configured_timeout_seconds=config.validation_timeout_seconds,
                    )
                except repo_execution.RepoExecutionError as e:
                    repo_execution._append_issue_failure_note(
                        repo_root=repo_policy.path,
                        issue_id=item.bead_id,
                        note=f"[orchestrator] {e}",
                        reopen_closed=True,
                    )
                    bead_results.append(
                        repo_execution.BeadResult(
                            bead_id=item.bead_id,
                            title=item.title,
                            outcome="failed",
                            detail=str(e),
                        )
                    )
                    state.bead_audits.append(
                        {
                            "bead_id": item.bead_id,
                            "title": item.title,
                            "outcome": "failed",
                            "detail": str(e),
                        }
                    )
                    state.repo_failures.append(f"{item.bead_id}: {e}")
                    state.stop_reason = "blocked"
                    reporter.maybe_write_repo_report(branch=run_branch)
                    stop_retry_attempts = True
                    break
                validation_context = runtime_baseline.context

            attempt += 1
            now = repo_execution._now()
            remaining = repo_execution._remaining_bead_time(
                tick=tick,
                now=now,
                bead_deadline=bead_deadline,
            )
            codex_prompt = repo_execution._format_codex_prompt(
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
            repo_execution.write_text_atomic(prompt_path, codex_prompt)
            state.prompt_records.append(
                {"bead_id": item.bead_id, "attempt": attempt, "path": prompt_rel}
            )
            reporter.emit(
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
            attempt_runtime = repo_execution._CodexAttemptRuntime()
            repo_execution._append_log(
                log_path,
                f"{repo_execution._now().isoformat()} codex_start bead_id={item.bead_id} "
                f"attempt={attempt} timeout={timeout_seconds:.0f}s",
            )
            codex_argv = (
                "codex",
                "exec",
                "--full-auto",
                *codex_cli_args_for_settings(config.ai_settings),
            )
            reporter.emit(
                "codex_start",
                bead_id=item.bead_id,
                attempt=attempt,
                timeout_seconds=timeout_seconds,
                argv=list(codex_argv),
            )
            try:
                codex_invocation = repo_execution.codex_exec_full_auto(
                    prompt=codex_prompt,
                    cwd=repo_policy.path,
                    timeout_seconds=timeout_seconds,
                    extra_args=codex_cli_args_for_settings(config.ai_settings),
                    output_limit_chars=config.codex_output_limit_chars,
                    on_start=repo_execution._make_codex_on_start_callback(
                        runtime=attempt_runtime,
                        repo_id=repo_policy.repo_id,
                        bead_id=item.bead_id,
                        attempt=attempt,
                        timeout_seconds=timeout_seconds,
                        log_path=log_path,
                        emit=reporter.emit,
                    ),
                )
            except Exception as e:
                if isinstance(e, CodexCliError):
                    failure_detail = f"codex CLI failed: {e}"
                    failure_error = str(e)
                else:
                    failure_detail = f"codex invocation crashed: {type(e).__name__}: {e}"
                    failure_error = f"{type(e).__name__}: {e}"
                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    note=f"[orchestrator] codex invocation failed: {failure_error}",
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail=failure_detail,
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": failure_detail,
                    }
                )
                state.repo_failures.append(f"codex failed for {item.bead_id}: {failure_error}")
                reporter.emit(
                    "codex_failed",
                    bead_id=item.bead_id,
                    attempt=attempt,
                    pid=attempt_runtime.pid,
                    error=failure_error,
                    argv=list(codex_argv),
                )
                state.stop_reason = "error"
                _maybe_commit_run_report_if_clean(
                    reporter=reporter,
                    state=state,
                    repo_policy=repo_policy,
                    run_id=run_id,
                    run_branch=run_branch,
                    dirty_ignore_globs=dirty_ignore_globs,
                )
                break
            finally:
                if attempt_runtime.heartbeat_stop is not None:
                    attempt_runtime.heartbeat_stop.set()
                if attempt_runtime.heartbeat_thread is not None:
                    attempt_runtime.heartbeat_thread.join(timeout=2.0)

            repo_execution._append_log(
                log_path,
                f"{repo_execution._now().isoformat()} codex_end bead_id={item.bead_id} "
                f"attempt={attempt} pid={codex_invocation.pid} exit={codex_invocation.exit_code}",
            )
            reporter.emit(
                "codex_end",
                bead_id=item.bead_id,
                attempt=attempt,
                pid=codex_invocation.pid,
                exit_code=codex_invocation.exit_code,
                started_at=codex_invocation.started_at.isoformat(),
                finished_at=codex_invocation.finished_at.isoformat(),
                argv=list(codex_invocation.args),
            )
            repo_execution._append_log(log_path, codex_invocation.stdout)
            repo_execution._append_log(
                stdout_log_path,
                f"{repo_execution._now().isoformat()} codex_stdout bead_id={item.bead_id} "
                f"attempt={attempt} exit={codex_invocation.exit_code}",
            )
            repo_execution._append_log(stdout_log_path, codex_invocation.stdout)
            if codex_invocation.stderr.strip():
                repo_execution._append_log(log_path, "[stderr]")
                repo_execution._append_log(log_path, codex_invocation.stderr)
                repo_execution._append_log(
                    stderr_log_path,
                    f"{repo_execution._now().isoformat()} codex_stderr bead_id={item.bead_id} "
                    f"attempt={attempt} exit={codex_invocation.exit_code}",
                )
                repo_execution._append_log(stderr_log_path, codex_invocation.stderr)

            head_after = git_rev_parse(repo_root=repo_policy.path)
            if head_after != head_before:
                raise repo_execution.RepoExecutionError(
                    "Policy violation: codex created commits; orchestrator must own commits."
                )

            files_changed, lines_added, changed_paths = repo_execution._diff_stats(
                repo_root=repo_policy.path,
                dirty_ignore_globs=dirty_ignore_globs,
            )
            reporter.emit(
                "diff_stats",
                bead_id=item.bead_id,
                attempt=attempt,
                files_changed=files_changed,
                lines_added=lines_added,
                changed_paths=list(changed_paths),
            )
            if files_changed == 0:
                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    note="[orchestrator] No git changes detected after codex; cannot commit/close.",
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail="No changes detected.",
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": "No changes detected.",
                        "changed_paths": list(changed_paths),
                    }
                )
                state.repo_failures.append(f"{item.bead_id}: no changes detected after codex.")
                state.stop_reason = "blocked"
                _maybe_commit_run_report_if_clean(
                    reporter=reporter,
                    state=state,
                    repo_policy=repo_policy,
                    run_id=run_id,
                    run_branch=run_branch,
                    dirty_ignore_globs=dirty_ignore_globs,
                )
                break

            try:
                validate_paths_within_policy(
                    paths=changed_paths,
                    allowed_roots=item.contract.allowed_roots,
                    deny_roots=item.contract.deny_roots,
                )
            except GitError as e:
                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    note=f"[orchestrator] {e}",
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail=str(e),
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": str(e),
                        "changed_paths": list(changed_paths),
                    }
                )
                state.repo_failures.append(f"{item.bead_id}: {e}")
                state.stop_reason = "blocked"
                reporter.maybe_write_repo_report(branch=run_branch)
                break

            if tick_files_changed + files_changed > config.diff_caps.max_files_changed:
                cap_summary, followup_ids = repo_execution._maybe_decompose_diff_cap_bead(
                    repo_root=repo_policy.path,
                    issue=issue,
                    item=item,
                    run_id=run_id,
                    attempt=attempt,
                    cap_kind="files changed",
                    files_changed=files_changed,
                    lines_added=lines_added,
                    tick_files_changed=tick_files_changed + files_changed,
                    tick_lines_added=tick_lines_added,
                    max_files_changed=config.diff_caps.max_files_changed,
                    max_lines_added=config.diff_caps.max_lines_added,
                    changed_paths=changed_paths,
                )
                extra_notes = ""
                if cap_summary:
                    extra_notes += "\n" + cap_summary
                if followup_ids:
                    extra_notes += "\n" + repo_execution._DECOMPOSE_MARKER + ": " + ", ".join(
                        sorted(followup_ids)
                    )
                    state.follow_ups.append(
                        f"Diff cap decomposition for {item.bead_id}: created "
                        + ", ".join(sorted(followup_ids))
                    )
                    if state.deck_path is not None and not config.replan:
                        try:
                            state.deck_path.unlink(missing_ok=True)
                            state.follow_ups.append(
                                f"Cleared run deck to force replan: {state.deck_path.as_posix()}"
                            )
                        except OSError as e:
                            state.repo_failures.append(
                                f"{item.bead_id}: failed to clear run deck {state.deck_path}: {e}"
                            )
                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    status="blocked" if followup_ids else None,
                    note="[orchestrator] Diff cap exceeded: "
                    + f"tick_files_changed={tick_files_changed + files_changed} "
                    + f"max={config.diff_caps.max_files_changed}"
                    + extra_notes,
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail="Diff cap exceeded (files changed).",
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": "Diff cap exceeded (files changed).",
                        "changed_paths": list(changed_paths),
                        "diff_cap": {
                            "kind": "files_changed",
                            "files_changed": files_changed,
                            "lines_added": lines_added,
                            "tick_files_changed": tick_files_changed + files_changed,
                            "tick_lines_added": tick_lines_added,
                            "max_files_changed": config.diff_caps.max_files_changed,
                            "max_lines_added": config.diff_caps.max_lines_added,
                        },
                        "followups": list(followup_ids) if followup_ids else [],
                    }
                )
                state.repo_failures.append(f"{item.bead_id}: diff cap exceeded (files changed).")
                state.stop_reason = "blocked"
                reporter.maybe_write_repo_report(branch=run_branch)
                break

            if tick_lines_added + lines_added > config.diff_caps.max_lines_added:
                cap_summary, followup_ids = repo_execution._maybe_decompose_diff_cap_bead(
                    repo_root=repo_policy.path,
                    issue=issue,
                    item=item,
                    run_id=run_id,
                    attempt=attempt,
                    cap_kind="lines added",
                    files_changed=files_changed,
                    lines_added=lines_added,
                    tick_files_changed=tick_files_changed,
                    tick_lines_added=tick_lines_added + lines_added,
                    max_files_changed=config.diff_caps.max_files_changed,
                    max_lines_added=config.diff_caps.max_lines_added,
                    changed_paths=changed_paths,
                )
                extra_notes = ""
                if cap_summary:
                    extra_notes += "\n" + cap_summary
                if followup_ids:
                    extra_notes += "\n" + repo_execution._DECOMPOSE_MARKER + ": " + ", ".join(
                        sorted(followup_ids)
                    )
                    state.follow_ups.append(
                        f"Diff cap decomposition for {item.bead_id}: created "
                        + ", ".join(sorted(followup_ids))
                    )
                    if state.deck_path is not None and not config.replan:
                        try:
                            state.deck_path.unlink(missing_ok=True)
                            state.follow_ups.append(
                                f"Cleared run deck to force replan: {state.deck_path.as_posix()}"
                            )
                        except OSError as e:
                            state.repo_failures.append(
                                f"{item.bead_id}: failed to clear run deck {state.deck_path}: {e}"
                            )
                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    status="blocked" if followup_ids else None,
                    note="[orchestrator] Diff cap exceeded: "
                    + f"tick_lines_added={tick_lines_added + lines_added} "
                    + f"max={config.diff_caps.max_lines_added}"
                    + extra_notes,
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail="Diff cap exceeded (lines added).",
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": "Diff cap exceeded (lines added).",
                        "changed_paths": list(changed_paths),
                        "diff_cap": {
                            "kind": "lines_added",
                            "files_changed": files_changed,
                            "lines_added": lines_added,
                            "tick_files_changed": tick_files_changed,
                            "tick_lines_added": tick_lines_added + lines_added,
                            "max_files_changed": config.diff_caps.max_files_changed,
                            "max_lines_added": config.diff_caps.max_lines_added,
                        },
                        "followups": list(followup_ids) if followup_ids else [],
                    }
                )
                state.repo_failures.append(f"{item.bead_id}: diff cap exceeded (lines added).")
                state.stop_reason = "blocked"
                reporter.maybe_write_repo_report(branch=run_branch)
                break

            dependency_change = repo_execution._classify_dependency_changes(changed_paths)
            if dependency_change.paths and item.contract.env:
                dep_signature = repo_execution._dependency_signature(
                    repo_root=repo_policy.path,
                    paths=dependency_change.paths,
                )
                if dep_signature != state.last_dependency_signature:
                    reporter.emit(
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
                        env_files=[repo_policy.path / path for path in dependency_change.env_files],
                        requirements_files=[
                            repo_policy.path / path for path in dependency_change.requirements_files
                        ],
                        pip_editable=dependency_change.pip_editable,
                    )
                    reporter.emit(
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
                        repo_execution._append_issue_failure_note(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            note=f"[orchestrator] Env refresh failed: {refresh_result.error}",
                            reopen_closed=True,
                        )
                        bead_results.append(
                            repo_execution.BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail=f"Env refresh failed: {refresh_result.error}",
                            )
                        )
                        state.bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": f"Env refresh failed: {refresh_result.error}",
                                "changed_paths": list(changed_paths),
                            }
                        )
                        state.repo_failures.append(
                            f"{item.bead_id}: env refresh failed: {refresh_result.error}"
                        )
                        state.stop_reason = "blocked"
                        reporter.maybe_write_repo_report(branch=run_branch)
                        break
                    state.last_dependency_signature = dep_signature
                    env_preflight_checked = False

            try:
                repo_execution._require_validation_allowlist(item.contract.validation_commands)
            except repo_execution.RepoExecutionError as e:
                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    note=f"[orchestrator] {e}",
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail=str(e),
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": str(e),
                        "changed_paths": list(changed_paths),
                    }
                )
                state.repo_failures.append(f"{item.bead_id}: {e}")
                state.stop_reason = "blocked"
                reporter.maybe_write_repo_report(branch=run_branch)
                break

            validation_timeout_seconds = repo_execution._validation_timeout_seconds(
                commands=item.contract.validation_commands,
                remaining=repo_execution._remaining_bead_time(
                    tick=tick,
                    now=repo_execution._now(),
                    bead_deadline=bead_deadline,
                ),
                configured_timeout_seconds=config.validation_timeout_seconds,
            )
            validation_results = repo_execution.run_validation_commands(
                item.contract.validation_commands,
                cwd=repo_policy.path,
                env=item.contract.env,
                timeout_seconds=validation_timeout_seconds,
            )
            for command, result in validation_results.items():
                state.validation_status_by_command[command] = repo_execution._validation_status(
                    result.exit_code
                )
                repo_execution._append_log(
                    stdout_log_path,
                    f"{repo_execution._now().isoformat()} validation_stdout bead_id={item.bead_id} "
                    f"attempt={attempt} cmd={command} exit={result.exit_code}",
                )
                if result.stdout.strip():
                    repo_execution._append_log(stdout_log_path, result.stdout)
                repo_execution._append_log(
                    stderr_log_path,
                    f"{repo_execution._now().isoformat()} validation_stderr bead_id={item.bead_id} "
                    f"attempt={attempt} cmd={command} exit={result.exit_code}",
                )
                if result.stderr.strip():
                    repo_execution._append_log(stderr_log_path, result.stderr)
            reporter.emit(
                "validation_end",
                bead_id=item.bead_id,
                attempt=attempt,
                results={command: result.exit_code for command, result in validation_results.items()},
            )
            still_failing = sorted(
                command
                for command in runtime_baseline.failing_commands
                if validation_results.get(command) is not None
                and validation_results[command].exit_code != 0
            )
            failed_commands = sorted(
                command for command, result in validation_results.items() if result.exit_code != 0
            )
            if failed_commands:
                preflight_result: Any = None
                if item.contract.env and not env_preflight_checked:
                    env_preflight_checked = True
                    preflight_timeout_seconds = max(1.0, min(validation_timeout_seconds, 120.0))
                    preflight_results = repo_execution.run_validation_commands(
                        (repo_execution._ENV_PREFLIGHT_TORCH_NUMPY_COMMAND,),
                        cwd=repo_policy.path,
                        env=item.contract.env,
                        timeout_seconds=preflight_timeout_seconds,
                    )
                    preflight_result = preflight_results.get(repo_execution._ENV_PREFLIGHT_TORCH_NUMPY_COMMAND)
                    if preflight_result is not None:
                        state.validation_status_by_command[
                            f"env_preflight:{repo_execution._ENV_PREFLIGHT_NAME}"
                        ] = repo_execution._validation_status(preflight_result.exit_code)
                        repo_execution._append_log(
                            stdout_log_path,
                            f"{repo_execution._now().isoformat()} validation_env_preflight_stdout bead_id={item.bead_id} "
                            f"attempt={attempt} env={item.contract.env} check={repo_execution._ENV_PREFLIGHT_NAME} "
                            f"exit={preflight_result.exit_code}",
                        )
                        if preflight_result.stdout.strip():
                            repo_execution._append_log(stdout_log_path, preflight_result.stdout)
                        repo_execution._append_log(
                            stderr_log_path,
                            f"{repo_execution._now().isoformat()} validation_env_preflight_stderr bead_id={item.bead_id} "
                            f"attempt={attempt} env={item.contract.env} check={repo_execution._ENV_PREFLIGHT_NAME} "
                            f"exit={preflight_result.exit_code}",
                        )
                        if preflight_result.stderr.strip():
                            repo_execution._append_log(stderr_log_path, preflight_result.stderr)
                        reporter.emit(
                            "validation_env_preflight_end",
                            bead_id=item.bead_id,
                            attempt=attempt,
                            env=item.contract.env,
                            check=repo_execution._ENV_PREFLIGHT_NAME,
                            exit_code=preflight_result.exit_code,
                        )
                    if preflight_result is not None and preflight_result.exit_code != 0:
                        preflight_summary = repo_execution._summarize_preflight_output(preflight_result)
                        detail = "Validation env preflight failed."
                        repo_execution._append_issue_failure_note(
                            repo_root=repo_policy.path,
                            issue_id=item.bead_id,
                            note="[orchestrator] Validation env preflight failed in "
                            + f"conda env {item.contract.env!r}; cannot proceed with retries.\n"
                            + preflight_summary,
                            reopen_closed=True,
                        )
                        bead_results.append(
                            repo_execution.BeadResult(
                                bead_id=item.bead_id,
                                title=item.title,
                                outcome="failed",
                                detail=detail,
                            )
                        )
                        state.bead_audits.append(
                            {
                                "bead_id": item.bead_id,
                                "title": item.title,
                                "outcome": "failed",
                                "detail": detail,
                                "changed_paths": list(changed_paths),
                                "validation": {
                                    command: repo_execution._validation_status(result.exit_code)
                                    for command, result in validation_results.items()
                                },
                                "validation_env_preflight": {
                                    "check": repo_execution._ENV_PREFLIGHT_NAME,
                                    "status": repo_execution._validation_status(
                                        preflight_result.exit_code
                                    ),
                                },
                            }
                        )
                        state.repo_failures.append(
                            f"{item.bead_id}: validation env preflight failed "
                            f"({item.contract.env}, {repo_execution._ENV_PREFLIGHT_NAME})."
                        )
                        state.stop_reason = "blocked"
                        reporter.maybe_write_repo_report(branch=run_branch)
                        break

                if repo_execution._can_retry_validation(
                    tick=tick,
                    now=repo_execution._now(),
                    bead_deadline=bead_deadline,
                ):
                    validation_context = repo_execution._format_validation_retry_context(
                        attempt=attempt,
                        validation_results=validation_results,
                        baseline_failures=runtime_baseline.failing_commands,
                    )
                    continue

                attempt_note = f" after {attempt} attempt(s)" if attempt > 1 else ""
                if still_failing:
                    detail = "Baseline failing validations still failing (time budget exhausted)."
                    note_prefix = (
                        "[orchestrator] Pre-existing failing validations remain failing; "
                        "time budget exhausted; cannot close.\n"
                    )
                    state.repo_failures.append(
                        f"{item.bead_id}: baseline failing validations still failing{attempt_note}."
                    )
                else:
                    detail = "Validation failed (time budget exhausted)."
                    note_prefix = "[orchestrator] Validation failed; time budget exhausted.\n"
                    state.repo_failures.append(
                        f"{item.bead_id}: validation failed ({', '.join(failed_commands)}){attempt_note}."
                    )

                timeout_summary, followup_ids = repo_execution._maybe_decompose_timeout_bead(
                    repo_root=repo_policy.path,
                    issue=issue,
                    item=item,
                    run_id=run_id,
                    attempt=attempt,
                    failed_commands=failed_commands,
                    baseline_failures=runtime_baseline.failing_commands,
                    validation_results=validation_results,
                    changed_paths=changed_paths,
                )
                extra_notes = ""
                if timeout_summary:
                    extra_notes += "\n" + timeout_summary
                if followup_ids:
                    extra_notes += "\n" + repo_execution._DECOMPOSE_MARKER + ": " + ", ".join(
                        sorted(followup_ids)
                    )
                    state.follow_ups.append(
                        f"Timeout decomposition for {item.bead_id}: created "
                        + ", ".join(sorted(followup_ids))
                    )
                    if state.deck_path is not None and not config.replan:
                        try:
                            state.deck_path.unlink(missing_ok=True)
                            state.follow_ups.append(
                                f"Cleared run deck to force replan: {state.deck_path.as_posix()}"
                            )
                        except OSError as e:
                            state.repo_failures.append(
                                f"{item.bead_id}: failed to clear run deck {state.deck_path}: {e}"
                            )

                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    status="blocked" if followup_ids else None,
                    note=note_prefix
                    + repo_execution._format_validation_summary(validation_results)
                    + extra_notes,
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail=detail,
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": detail,
                        "changed_paths": list(changed_paths),
                        "validation": {
                            command: repo_execution._validation_status(result.exit_code)
                            for command, result in validation_results.items()
                        },
                        "followups": list(followup_ids) if followup_ids else [],
                    }
                )
                state.stop_reason = "blocked"
                reporter.maybe_write_repo_report(branch=run_branch)
                break

            tick_files_changed += files_changed
            tick_lines_added += lines_added
            break

        if state.stop_reason is not None:
            break
        if stop_retry_attempts:
            continue

        if not any(repo_execution._is_behavioral_test_command(command) for command in item.contract.validation_commands):
            repo_execution._append_issue_failure_note(
                repo_root=repo_policy.path,
                issue_id=item.bead_id,
                note="[orchestrator] No behavioral test command executed; cannot close.",
                reopen_closed=True,
            )
            bead_results.append(
                repo_execution.BeadResult(
                    bead_id=item.bead_id,
                    title=item.title,
                    outcome="failed",
                    detail="No behavioral test executed.",
                )
            )
            state.bead_audits.append(
                {
                    "bead_id": item.bead_id,
                    "title": item.title,
                    "outcome": "failed",
                    "detail": "No behavioral test executed.",
                    "changed_paths": list(changed_paths),
                    "validation": {
                        command: repo_execution._validation_status(result.exit_code)
                        for command, result in validation_results.items()
                    },
                }
            )
            state.repo_failures.append(f"{item.bead_id}: no behavioral test executed; cannot close.")
            state.stop_reason = "blocked"
            reporter.maybe_write_repo_report(branch=run_branch)
            break

        if item.contract.enforce_given_when_then:
            missing_gwt = repo_execution._tests_missing_given_when_then(
                repo_root=repo_policy.path,
                changed_paths=changed_paths,
            )
            if missing_gwt:
                formatted = "\n".join(f"- {path}" for path in missing_gwt)
                repo_execution._append_issue_failure_note(
                    repo_root=repo_policy.path,
                    issue_id=item.bead_id,
                    note="[orchestrator] Given/When/Then markers missing in modified tests; "
                    "cannot close.\n"
                    + formatted,
                    reopen_closed=True,
                )
                bead_results.append(
                    repo_execution.BeadResult(
                        bead_id=item.bead_id,
                        title=item.title,
                        outcome="failed",
                        detail="Given/When/Then markers missing in modified tests.",
                    )
                )
                state.bead_audits.append(
                    {
                        "bead_id": item.bead_id,
                        "title": item.title,
                        "outcome": "failed",
                        "detail": "Given/When/Then markers missing in modified tests.",
                        "changed_paths": list(changed_paths),
                        "gwt_missing_paths": missing_gwt,
                        "validation": {
                            command: repo_execution._validation_status(result.exit_code)
                            for command, result in validation_results.items()
                        },
                    }
                )
                state.repo_failures.append(
                    f"{item.bead_id}: Given/When/Then markers missing in modified tests."
                )
                state.stop_reason = "blocked"
                reporter.maybe_write_repo_report(branch=run_branch)
                break

        for path in changed_paths:
            if path.endswith(".ipynb"):
                state.notebooks_touched.add(path)
            if path.endswith(".py"):
                state.extracted_code_touched.add(path)

        dependents_updated = list(issue.dependents)
        if dependents_updated:
            state.follow_ups.append(
                f"Updated downstream bead notes for `{item.bead_id}`: "
                + ", ".join(f"`{dependent}`" for dependent in dependents_updated)
            )

        bead_audit: dict[str, Any] = {
            "bead_id": item.bead_id,
            "title": item.title,
            "outcome": "closed",
            "detail": "Closed successfully.",
            "changed_paths": list(changed_paths),
            "validation": {
                command: repo_execution._validation_status(result.exit_code)
                for command, result in validation_results.items()
            },
            "dependents_updated": dependents_updated,
        }
        state.bead_audits.append(bead_audit)
        reporter.maybe_write_repo_report(branch=run_branch)

        subject = f"beads({item.bead_id}): {item.title}"
        try:
            git_stage_all(repo_root=repo_policy.path)
            commit_hash = git_commit(
                repo_root=repo_policy.path,
                subject=subject,
                body=repo_execution._commit_body(
                    run_id=run_id,
                    item=item,
                    validation=validation_results,
                ),
            )
        except GitError as e:
            repo_execution._append_issue_failure_note(
                repo_root=repo_policy.path,
                issue_id=item.bead_id,
                note=f"[orchestrator] {e}",
                reopen_closed=True,
            )
            bead_results.append(
                repo_execution.BeadResult(
                    bead_id=item.bead_id,
                    title=item.title,
                    outcome="failed",
                    detail=f"git commit failed: {e}",
                )
            )
            bead_audit["outcome"] = "failed"
            bead_audit["detail"] = f"git commit failed: {e}"
            state.repo_failures.append(f"{item.bead_id}: git commit failed: {e}")
            state.stop_reason = "blocked"
            break

        summary_note = issue.notes + ("\n" if issue.notes else "")
        summary_note += (
            f"[orchestrator] Closed in RUN_ID={run_id} on {run_branch}.\n"
            + repo_execution._format_validation_summary(validation_results)
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

        auto_closed_parent_epic = repo_execution._maybe_close_parent_epic(
            repo_root=repo_policy.path,
            closed_issue=issue,
            run_id=run_id,
            run_branch=run_branch,
            bd_show=bd_show,
            bd_update=bd_update,
            bd_close=bd_close,
        )
        if auto_closed_parent_epic is not None:
            bead_audit["auto_closed_parent_epic"] = auto_closed_parent_epic
            state.follow_ups.append(
                "Auto-closed parent epic "
                f"`{auto_closed_parent_epic}` after child `{item.bead_id}` closed."
            )

        try:
            git_stage_all(repo_root=repo_policy.path)
            commit_hash = git_commit_amend_no_edit(repo_root=repo_policy.path)
        except GitError as e:
            state.repo_failures.append(f"{item.bead_id}: git commit amend failed: {e}")
            bead_audit["outcome"] = "failed"
            bead_audit["detail"] = f"git commit amend failed: {e}"
            bead_results.append(
                repo_execution.BeadResult(
                    bead_id=item.bead_id,
                    title=item.title,
                    outcome="failed",
                    detail=f"git commit amend failed: {e}",
                )
            )
            state.stop_reason = "error"
            break

        bead_audit["commit_hash"] = commit_hash
        bead_results.append(
            repo_execution.BeadResult(
                bead_id=item.bead_id,
                title=item.title,
                outcome="closed",
                detail="Closed successfully.",
                commit_hash=commit_hash,
            )
        )
        state.beads_closed += 1
        reporter.emit(
            "bead_end",
            bead_id=item.bead_id,
            outcome="closed",
            commit_hash=commit_hash,
            dependents_updated=dependents_updated,
            auto_closed_parent_epic=bead_audit.get("auto_closed_parent_epic"),
        )

    if state.stop_reason is None:
        state.stop_reason = "completed"
    return tuple(bead_results)
