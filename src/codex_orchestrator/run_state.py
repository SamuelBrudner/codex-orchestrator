from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

RunMode = Literal["automated", "manual"]


class RunStateError(ValueError):
    pass


def _parse_datetime(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise RunStateError(f"{field}: expected ISO datetime string, got {type(value).__name__}")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as e:
        raise RunStateError(f"{field}: invalid ISO datetime: {value!r}") from e
    if dt.tzinfo is None:
        raise RunStateError(f"{field}: datetime must be timezone-aware, got {value!r}")
    return dt


def _as_int(value: Any, *, field: str) -> int:
    if not isinstance(value, int):
        raise RunStateError(f"{field}: expected int, got {type(value).__name__}")
    return value


@dataclass(frozen=True, slots=True)
class CurrentRunState:
    schema_version: int
    run_id: str
    mode: RunMode
    created_at: datetime
    last_tick_at: datetime
    expires_at: datetime
    window_end_at: datetime | None
    tick_count: int
    consecutive_idle_ticks: int

    def is_expired(self, *, now: datetime) -> bool:
        if now.tzinfo is None:
            raise RunStateError("now must be timezone-aware.")
        return now >= self.expires_at

    def on_tick(
        self,
        *,
        now: datetime,
        actionable_work_found: bool,
        idle_ticks_to_end: int,
        manual_ttl: timedelta,
    ) -> CurrentRunState:
        if now.tzinfo is None:
            raise RunStateError("now must be timezone-aware.")
        if idle_ticks_to_end < 1:
            raise RunStateError(f"idle_ticks_to_end must be >= 1, got {idle_ticks_to_end}")
        if manual_ttl <= timedelta(0):
            raise RunStateError("manual_ttl must be positive.")

        if actionable_work_found:
            consecutive_idle_ticks = 0
        else:
            consecutive_idle_ticks = self.consecutive_idle_ticks + 1

        if self.mode == "automated" and self.window_end_at is not None:
            expires_at = min(self.window_end_at, self.expires_at)
        else:
            expires_at = now + manual_ttl

        return CurrentRunState(
            schema_version=self.schema_version,
            run_id=self.run_id,
            mode=self.mode,
            created_at=self.created_at,
            last_tick_at=now,
            expires_at=expires_at,
            window_end_at=self.window_end_at,
            tick_count=self.tick_count + 1,
            consecutive_idle_ticks=consecutive_idle_ticks,
        )

    def should_end(self, *, now: datetime, idle_ticks_to_end: int) -> str | None:
        if now.tzinfo is None:
            raise RunStateError("now must be timezone-aware.")
        if self.mode == "automated" and self.window_end_at is not None and now >= self.window_end_at:
            return "window_end"
        if self.consecutive_idle_ticks >= idle_ticks_to_end:
            return "idle_ticks"
        return None

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "mode": self.mode,
            "created_at": self.created_at.isoformat(),
            "last_tick_at": self.last_tick_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "tick_count": self.tick_count,
            "consecutive_idle_ticks": self.consecutive_idle_ticks,
        }
        if self.window_end_at is not None:
            payload["window_end_at"] = self.window_end_at.isoformat()
        return payload

    @classmethod
    def from_json_dict(cls, data: Any) -> CurrentRunState:
        if not isinstance(data, dict):
            raise RunStateError(f"Expected dict for run state, got {type(data).__name__}")

        schema_version = _as_int(data.get("schema_version"), field="schema_version")
        if schema_version != 1:
            raise RunStateError(f"Unsupported schema_version: {schema_version}")

        run_id = data.get("run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            raise RunStateError("run_id: required non-empty string")

        mode = data.get("mode")
        if mode not in ("automated", "manual"):
            raise RunStateError(f"mode: expected 'automated' or 'manual', got {mode!r}")

        created_at = _parse_datetime(data.get("created_at"), field="created_at")
        last_tick_at = _parse_datetime(data.get("last_tick_at"), field="last_tick_at")
        expires_at = _parse_datetime(data.get("expires_at"), field="expires_at")
        window_end_raw = data.get("window_end_at")
        window_end_at = None if window_end_raw is None else _parse_datetime(window_end_raw, field="window_end_at")

        tick_count = _as_int(data.get("tick_count"), field="tick_count")
        consecutive_idle_ticks = _as_int(
            data.get("consecutive_idle_ticks"), field="consecutive_idle_ticks"
        )

        return cls(
            schema_version=schema_version,
            run_id=run_id,
            mode=mode,
            created_at=created_at,
            last_tick_at=last_tick_at,
            expires_at=expires_at,
            window_end_at=window_end_at,
            tick_count=tick_count,
            consecutive_idle_ticks=consecutive_idle_ticks,
        )

