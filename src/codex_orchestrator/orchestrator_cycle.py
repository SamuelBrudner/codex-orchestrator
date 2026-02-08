from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from codex_orchestrator.ai_policy import AiSettings
from codex_orchestrator.beads_subprocess import BdCliError, bd_doctor, bd_sync
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.repo_execution import (
    DiffCaps,
    RepoExecutionConfig,
    RepoExecutionError,
    RepoTickResult,
    TickBudget,
    execute_repos_tick,
)
from codex_orchestrator.repo_inventory import RepoConfigError, RepoPolicy, load_repo_inventory
from codex_orchestrator.run_closure_review import (
    RunClosureReviewError,
    run_review_only_codex_pass,
    write_final_review,
)
from codex_orchestrator.run_lifecycle import TickResult, ensure_active_run, record_review, tick_run
from codex_orchestrator.run_lock import RunLock, RunLockError
from codex_orchestrator.run_state import RunMode


class OrchestratorCycleError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OrchestratorCycleResult:
    ensure_result: TickResult
    tick_result: TickResult | None
    repo_results: tuple[RepoTickResult, ...]


def _default_max_parallel(repo_count: int) -> int:
    if repo_count <= 0:
        return 1
    cpu_count = os.cpu_count() or 1
    return max(1, min(repo_count, cpu_count, 4))


def _append_run_log(paths: OrchestratorPaths, *, run_id: str, message: str) -> None:
    log_path = paths.run_log_path(run_id)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().astimezone().isoformat()
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"{ts} {message.rstrip()}\n")


def _attempt_beads_maintenance(
    *,
    paths: OrchestratorPaths,
    run_id: str,
    repos: Sequence[RepoPolicy],
) -> None:
    for repo in repos:
        try:
            doctor = bd_doctor(repo_root=repo.path)
            overall_ok = doctor.get("overall_ok")
            checks = doctor.get("checks")
            failed_checks = 0
            if isinstance(checks, list):
                for item in checks:
                    if not isinstance(item, dict):
                        continue
                    status = item.get("status")
                    if isinstance(status, str) and status != "ok":
                        failed_checks += 1
            if overall_ok is False:
                _append_run_log(
                    paths,
                    run_id=run_id,
                    message=(
                        f"beads_doctor repo_id={repo.repo_id} status=warn overall_ok=false "
                        f"failed_checks={failed_checks}"
                    ),
                )
            else:
                _append_run_log(
                    paths,
                    run_id=run_id,
                    message=f"beads_doctor repo_id={repo.repo_id} status=ok",
                )
        except BdCliError as e:
            _append_run_log(
                paths,
                run_id=run_id,
                message=f"beads_doctor repo_id={repo.repo_id} status=error error={e}",
            )

        try:
            bd_sync(repo_root=repo.path)
            _append_run_log(
                paths,
                run_id=run_id,
                message=f"beads_sync repo_id={repo.repo_id} status=ok",
            )
        except BdCliError as e:
            _append_run_log(
                paths,
                run_id=run_id,
                message=f"beads_sync repo_id={repo.repo_id} status=error error={e}",
            )


def run_orchestrator_cycle(
    *,
    cache_dir: Path,
    mode: RunMode,
    ai_settings: AiSettings,
    repo_config_path: Path,
    overlays_dir: Path,
    repo_ids: Sequence[str] | None = None,
    repo_groups: Sequence[str] | None = None,
    max_parallel: int | None = None,
    tick_minutes: float = 45.0,
    idle_ticks_to_end: int = 3,
    manual_ttl_hours: float = 12.0,
    min_minutes_to_start_new_bead: int = 15,
    max_beads_per_tick: int = 3,
    diff_cap_files: int = 25,
    diff_cap_lines: int = 1500,
    replan: bool = False,
    final_review_codex_review: bool = False,
    review_every_beads: int | None = None,
    now: datetime | None = None,
    focus: str | None = None,
) -> OrchestratorCycleResult:
    if tick_minutes <= 0:
        raise OrchestratorCycleError(f"tick_minutes must be > 0, got {tick_minutes}")
    if manual_ttl_hours <= 0:
        raise OrchestratorCycleError(f"manual_ttl_hours must be > 0, got {manual_ttl_hours}")

    if now is None:
        now = datetime.now().astimezone()
    if now.tzinfo is None:
        raise OrchestratorCycleError("run_orchestrator_cycle requires a timezone-aware now datetime.")

    paths = OrchestratorPaths(cache_dir=cache_dir)
    tick_budget = timedelta(minutes=tick_minutes)
    manual_ttl = timedelta(hours=manual_ttl_hours)

    try:
        with RunLock(paths.run_lock_path) as lock:
            ensure_result = ensure_active_run(
                paths=paths,
                mode=mode,
                idle_ticks_to_end=idle_ticks_to_end,
                manual_ttl=manual_ttl,
                now=now,
                run_lock=lock,
            )
            if ensure_result.ended or ensure_result.run_id is None:
                if ensure_result.ended and ensure_result.run_id is not None:
                    try:
                        write_final_review(paths, run_id=ensure_result.run_id, ai_settings=ai_settings)
                        if final_review_codex_review:
                            run_review_only_codex_pass(
                                paths,
                                run_id=ensure_result.run_id,
                                ai_settings=ai_settings,
                                repo_config_path=repo_config_path,
                            )
                    except RunClosureReviewError as e:
                        raise OrchestratorCycleError(str(e)) from e
                return OrchestratorCycleResult(
                    ensure_result=ensure_result,
                    tick_result=None,
                    repo_results=(),
                )

            try:
                inventory = load_repo_inventory(repo_config_path)
            except RepoConfigError as e:
                raise OrchestratorCycleError(str(e)) from e

            repos = inventory.select_repos(repo_ids=repo_ids, repo_groups=repo_groups)
            tick = TickBudget(started_at=now, ends_at=now + tick_budget)
            config = RepoExecutionConfig(
                tick_budget=tick_budget,
                min_minutes_to_start_new_bead=min_minutes_to_start_new_bead,
                max_beads_per_tick=max_beads_per_tick,
                diff_caps=DiffCaps(
                    max_files_changed=diff_cap_files,
                    max_lines_added=diff_cap_lines,
                ),
                replan=replan,
                ai_settings=ai_settings,
                focus=focus,
            )

            if max_parallel is None or max_parallel <= 0:
                max_parallel = _default_max_parallel(len(repos))
            if max_parallel < 1:
                raise OrchestratorCycleError(f"max_parallel must be >= 1, got {max_parallel}")

            repo_results = execute_repos_tick(
                paths=paths,
                run_id=ensure_result.run_id,
                repos=repos,
                overlays_dir=overlays_dir,
                max_parallel=max_parallel,
                tick=tick,
                config=config,
            )
            actionable_work_found = any(r.beads_attempted > 0 for r in repo_results)
            beads_attempted_total = sum(r.beads_attempted for r in repo_results)
            if (
                mode == "manual"
                and not actionable_work_found
                and ensure_result.run_id is not None
            ):
                _attempt_beads_maintenance(
                    paths=paths,
                    run_id=ensure_result.run_id,
                    repos=repos,
                )

            tick_result = tick_run(
                paths=paths,
                mode=mode,
                actionable_work_found=actionable_work_found,
                idle_ticks_to_end=idle_ticks_to_end,
                manual_ttl=manual_ttl,
                beads_attempted_delta=beads_attempted_total,
                now=datetime.now().astimezone(),
                run_lock=lock,
            )
            if tick_result.ended and tick_result.run_id is not None:
                try:
                    write_final_review(paths, run_id=tick_result.run_id, ai_settings=ai_settings)
                    if final_review_codex_review:
                        run_review_only_codex_pass(
                            paths,
                            run_id=tick_result.run_id,
                            ai_settings=ai_settings,
                            repo_config_path=repo_config_path,
                        )
                except RunClosureReviewError as e:
                    raise OrchestratorCycleError(str(e)) from e
            if (
                tick_result.state is not None
                and review_every_beads is not None
                and tick_result.state.review_due(review_every_beads=review_every_beads)
            ):
                _append_run_log(
                    paths,
                    run_id=tick_result.state.run_id,
                    message="review_cadence status=starting",
                )
                try:
                    run_review_only_codex_pass(
                        paths,
                        run_id=tick_result.state.run_id,
                        ai_settings=ai_settings,
                        repo_config_path=repo_config_path,
                        log_stem="cadence_codex_review",
                        log_suffix=f"tick{tick_result.state.tick_count}",
                        prompt_label="cadence",
                    )
                    record_review(
                        paths=paths,
                        run_id=tick_result.state.run_id,
                        now=datetime.now().astimezone(),
                        run_lock=lock,
                    )
                except RunClosureReviewError as e:
                    _append_run_log(
                        paths,
                        run_id=tick_result.state.run_id,
                        message=f"review_cadence status=error error={e}",
                    )
                    raise OrchestratorCycleError(str(e)) from e

            return OrchestratorCycleResult(
                ensure_result=ensure_result,
                tick_result=tick_result,
                repo_results=repo_results,
            )
    except RunLockError as e:
        raise OrchestratorCycleError(str(e)) from e
    except RepoExecutionError as e:
        raise OrchestratorCycleError(str(e)) from e
