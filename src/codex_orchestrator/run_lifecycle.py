from __future__ import annotations

import json
import os
import secrets
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from codex_orchestrator.night_window import DEFAULT_NIGHT_WINDOW
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_lock import RunLock, RunLockError
from codex_orchestrator.run_signoff import (
    RunSignoffError,
    find_latest_ended_run_id,
    validate_run_signoff,
)
from codex_orchestrator.run_state import CurrentRunState, RunMode, RunStateError


class RunLifecycleError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class TickResult:
    run_id: str | None
    started_new: bool
    ended: bool
    end_reason: str | None
    state: CurrentRunState | None


def _generate_run_id(*, now: datetime) -> str:
    now_utc = now.astimezone(timezone.utc)
    ts = now_utc.strftime("%Y%m%d-%H%M%S")
    return f"{ts}-{secrets.token_hex(4)}"


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
        tmp_name = f.name
    os.replace(tmp_name, path)


def _load_current_run_state(*, path: Path, now: datetime) -> CurrentRunState | None:
    try:
        data = _read_json(path)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        raise RunLifecycleError(f"Failed to parse {path}: {e}") from e

    state = CurrentRunState.from_json_dict(data)
    if state.is_expired(now=now):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return None
    return state


def _ensure_run_artifacts(paths: OrchestratorPaths, *, state: CurrentRunState) -> None:
    run_dir = paths.run_dir(state.run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = paths.run_metadata_path(state.run_id)
    if not metadata_path.exists():
        _write_json_atomic(metadata_path, state.to_json_dict())

    log_path = paths.run_log_path(state.run_id)
    if not log_path.exists():
        log_path.write_text("", encoding="utf-8")


def _append_run_log(paths: OrchestratorPaths, *, run_id: str, message: str) -> None:
    log_path = paths.run_log_path(run_id)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(message.rstrip("\n") + "\n")


def _end_run(paths: OrchestratorPaths, *, state: CurrentRunState, now: datetime, reason: str) -> None:
    end_path = paths.run_dir(state.run_id) / "run_end.json"
    _write_json_atomic(
        end_path,
        {"run_id": state.run_id, "ended_at": now.isoformat(), "reason": reason},
    )
    try:
        paths.current_run_path.unlink()
    except FileNotFoundError:
        pass


def _require_held_run_lock(paths: OrchestratorPaths, *, run_lock: RunLock) -> None:
    if run_lock._handle is None:  # pyright: ignore[reportPrivateUsage]
        raise RunLifecycleError("run_lock must be acquired before calling this function.")
    if run_lock.lock_path.resolve() != paths.run_lock_path.resolve():
        raise RunLifecycleError(
            f"run_lock path mismatch: expected {paths.run_lock_path}, got {run_lock.lock_path}"
        )


def _format_latest_ended_run_lookup_error(paths: OrchestratorPaths, *, error: RunSignoffError) -> str:
    lines = [
        "Refusing to start a new run: unable to determine the latest ended run.",
        f"error={error}",
        "next_action=Inspect run_end.json under the runs directory and fix/remove corrupt artifacts.",
        f"runs_dir={paths.runs_dir}",
    ]
    return "\n".join(lines)


def _format_latest_run_not_signed_off_error(
    paths: OrchestratorPaths, *, run_id: str, error: RunSignoffError
) -> str:
    final_review_path = paths.final_review_json_path(run_id)
    signoff_json_path = paths.run_signoff_json_path(run_id)
    lines = [
        "Refusing to start a new run: the latest ended run is not signed off.",
        f"latest_ended_run_id={run_id}",
        f"final_review={final_review_path}",
        f"expected_signoff={signoff_json_path}",
        f"signoff_error={error}",
        "next_action=Review final_review.json then sign off the run:",
        f"  codex-orchestrator signoff --run-id {run_id} --reviewer <name>",
    ]
    return "\n".join(lines)


def _latest_ended_run_id_or_raise(paths: OrchestratorPaths) -> str | None:
    try:
        return find_latest_ended_run_id(paths)
    except RunSignoffError as e:
        raise RunLifecycleError(_format_latest_ended_run_lookup_error(paths, error=e)) from e


def _validate_run_signed_off_or_raise(paths: OrchestratorPaths, *, run_id: str) -> None:
    try:
        validate_run_signoff(paths, run_id=run_id)
    except RunSignoffError as e:
        raise RunLifecycleError(_format_latest_run_not_signed_off_error(paths, run_id=run_id, error=e)) from e


def _require_latest_ended_run_signed_off(paths: OrchestratorPaths) -> None:
    latest_ended_run_id = _latest_ended_run_id_or_raise(paths)
    if latest_ended_run_id is None:
        return
    _validate_run_signed_off_or_raise(paths, run_id=latest_ended_run_id)


def _tick_run_locked(
    *,
    paths: OrchestratorPaths,
    mode: RunMode,
    actionable_work_found: bool,
    idle_ticks_to_end: int,
    manual_ttl: timedelta,
    now: datetime,
) -> TickResult:
    state = _load_current_run_state(path=paths.current_run_path, now=now)

    if state is not None and state.mode != mode:
        try:
            paths.current_run_path.unlink()
        except FileNotFoundError:
            pass
        state = None

    if state is not None:
        end_reason = state.should_end(now=now, idle_ticks_to_end=idle_ticks_to_end)
        if end_reason is not None:
            _append_run_log(
                paths,
                run_id=state.run_id,
                message=f"{now.isoformat()} end_run reason={end_reason}",
            )
            _end_run(paths, state=state, now=now, reason=end_reason)
            return TickResult(
                run_id=state.run_id,
                started_new=False,
                ended=True,
                end_reason=end_reason,
                state=None,
            )

    started_new = False
    if state is None:
        if mode == "automated" and not DEFAULT_NIGHT_WINDOW.contains(now):
            return TickResult(
                run_id=None,
                started_new=False,
                ended=True,
                end_reason="outside_window",
                state=None,
            )
        _require_latest_ended_run_signed_off(paths)
        started_new = True
        run_id = _generate_run_id(now=now)
        window_end_at = DEFAULT_NIGHT_WINDOW.end_for(now) if mode == "automated" else None
        expires_at = window_end_at if window_end_at is not None else now + manual_ttl
        state = CurrentRunState(
            schema_version=1,
            run_id=run_id,
            mode=mode,
            created_at=now,
            last_tick_at=now,
            expires_at=expires_at,
            window_end_at=window_end_at,
            tick_count=0,
            consecutive_idle_ticks=0,
        )

    state = state.on_tick(
        now=now,
        actionable_work_found=actionable_work_found,
        idle_ticks_to_end=idle_ticks_to_end,
        manual_ttl=manual_ttl,
    )

    _ensure_run_artifacts(paths, state=state)
    _write_json_atomic(paths.current_run_path, state.to_json_dict())
    _append_run_log(
        paths,
        run_id=state.run_id,
        message=(
            f"{now.isoformat()} tick={state.tick_count} mode={state.mode} "
            f"actionable_work_found={actionable_work_found} "
            f"consecutive_idle_ticks={state.consecutive_idle_ticks}"
        ),
    )

    end_reason = state.should_end(now=now, idle_ticks_to_end=idle_ticks_to_end)
    if end_reason is not None:
        _append_run_log(
            paths,
            run_id=state.run_id,
            message=f"{now.isoformat()} end_run reason={end_reason}",
        )
        _end_run(paths, state=state, now=now, reason=end_reason)
        return TickResult(
            run_id=state.run_id,
            started_new=started_new,
            ended=True,
            end_reason=end_reason,
            state=None,
        )

    return TickResult(
        run_id=state.run_id,
        started_new=started_new,
        ended=False,
        end_reason=None,
        state=state,
    )


def tick_run(
    *,
    paths: OrchestratorPaths,
    mode: RunMode,
    actionable_work_found: bool = False,
    idle_ticks_to_end: int = 3,
    manual_ttl: timedelta = timedelta(hours=12),
    now: datetime | None = None,
    run_lock: RunLock | None = None,
) -> TickResult:
    if now is None:
        now = datetime.now().astimezone()
    if now.tzinfo is None:
        raise RunLifecycleError("tick_run requires a timezone-aware now datetime.")

    paths.cache_dir.mkdir(parents=True, exist_ok=True)

    try:
        if run_lock is not None:
            _require_held_run_lock(paths, run_lock=run_lock)
            return _tick_run_locked(
                paths=paths,
                mode=mode,
                actionable_work_found=actionable_work_found,
                idle_ticks_to_end=idle_ticks_to_end,
                manual_ttl=manual_ttl,
                now=now,
            )

        with RunLock(paths.run_lock_path):
            return _tick_run_locked(
                paths=paths,
                mode=mode,
                actionable_work_found=actionable_work_found,
                idle_ticks_to_end=idle_ticks_to_end,
                manual_ttl=manual_ttl,
                now=now,
            )
    except RunLockError as e:
        raise RunLifecycleError(str(e)) from e
    except RunStateError as e:
        raise RunLifecycleError(str(e)) from e


def ensure_active_run(
    *,
    paths: OrchestratorPaths,
    mode: RunMode,
    idle_ticks_to_end: int = 3,
    manual_ttl: timedelta = timedelta(hours=12),
    now: datetime | None = None,
    run_lock: RunLock | None = None,
) -> TickResult:
    if now is None:
        now = datetime.now().astimezone()
    if now.tzinfo is None:
        raise RunLifecycleError("ensure_active_run requires a timezone-aware now datetime.")

    paths.cache_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_locked() -> TickResult:
        state = _load_current_run_state(path=paths.current_run_path, now=now)

        if state is not None and state.mode != mode:
            try:
                paths.current_run_path.unlink()
            except FileNotFoundError:
                pass
            state = None

        if state is not None:
            end_reason = state.should_end(now=now, idle_ticks_to_end=idle_ticks_to_end)
            if end_reason is not None:
                _append_run_log(
                    paths,
                    run_id=state.run_id,
                    message=f"{now.isoformat()} end_run reason={end_reason}",
                )
                _end_run(paths, state=state, now=now, reason=end_reason)
                return TickResult(
                    run_id=state.run_id,
                    started_new=False,
                    ended=True,
                    end_reason=end_reason,
                    state=None,
                )

            return TickResult(
                run_id=state.run_id,
                started_new=False,
                ended=False,
                end_reason=None,
                state=state,
            )

        if mode == "automated" and not DEFAULT_NIGHT_WINDOW.contains(now):
            return TickResult(
                run_id=None,
                started_new=False,
                ended=True,
                end_reason="outside_window",
                state=None,
            )

        _require_latest_ended_run_signed_off(paths)
        run_id = _generate_run_id(now=now)
        window_end_at = DEFAULT_NIGHT_WINDOW.end_for(now) if mode == "automated" else None
        expires_at = window_end_at if window_end_at is not None else now + manual_ttl
        state = CurrentRunState(
            schema_version=1,
            run_id=run_id,
            mode=mode,
            created_at=now,
            last_tick_at=now,
            expires_at=expires_at,
            window_end_at=window_end_at,
            tick_count=0,
            consecutive_idle_ticks=0,
        )
        _ensure_run_artifacts(paths, state=state)
        _write_json_atomic(paths.current_run_path, state.to_json_dict())
        _append_run_log(paths, run_id=state.run_id, message=f"{now.isoformat()} start_run mode={mode}")
        return TickResult(
            run_id=state.run_id,
            started_new=True,
            ended=False,
            end_reason=None,
            state=state,
        )

    try:
        if run_lock is not None:
            _require_held_run_lock(paths, run_lock=run_lock)
            return _ensure_locked()

        with RunLock(paths.run_lock_path):
            return _ensure_locked()
    except RunLockError as e:
        raise RunLifecycleError(str(e)) from e
    except RunStateError as e:
        raise RunLifecycleError(str(e)) from e


def end_current_run(
    *,
    paths: OrchestratorPaths,
    reason: str,
    now: datetime | None = None,
    run_lock: RunLock | None = None,
) -> str | None:
    if now is None:
        now = datetime.now().astimezone()
    if now.tzinfo is None:
        raise RunLifecycleError("end_current_run requires a timezone-aware now datetime.")

    paths.cache_dir.mkdir(parents=True, exist_ok=True)

    def _end_locked() -> str | None:
        state = _load_current_run_state(path=paths.current_run_path, now=now)
        if state is None:
            return None
        _append_run_log(
            paths,
            run_id=state.run_id,
            message=f"{now.isoformat()} end_run reason={reason}",
        )
        _end_run(paths, state=state, now=now, reason=reason)
        return state.run_id

    try:
        if run_lock is not None:
            _require_held_run_lock(paths, run_lock=run_lock)
            return _end_locked()

        with RunLock(paths.run_lock_path):
            return _end_locked()
    except RunLockError as e:
        raise RunLifecycleError(str(e)) from e
    except RunStateError as e:
        raise RunLifecycleError(str(e)) from e
