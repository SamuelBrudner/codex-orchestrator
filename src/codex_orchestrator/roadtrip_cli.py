from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from codex_orchestrator.ai_policy import AiPolicyError, AiSettings, enforce_unattended_ai_policy, load_ai_settings
from codex_orchestrator.orchestrator_cycle import OrchestratorCycleError, run_orchestrator_cycle
from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir
from codex_orchestrator.run_closure_review import run_review_only_codex_pass, write_final_review
from codex_orchestrator.run_lifecycle import RunLifecycleError, end_current_run
from codex_orchestrator.run_lock import RunLock
from codex_orchestrator.run_state import CurrentRunState


def _parse_until(value: str) -> datetime:
    raw = str(value).strip()
    try:
        naive = datetime.strptime(raw, "%Y-%m-%d %H:%M")
    except ValueError as e:
        raise SystemExit(
            'codex-roadtrip: --until must be in format "YYYY-MM-DD HH:MM" (local time)'
        ) from e
    tz = datetime.now().astimezone().tzinfo
    if tz is None:  # pragma: no cover
        raise SystemExit("codex-roadtrip: local timezone is unavailable")
    return naive.replace(tzinfo=tz)


def _load_enforced_ai_settings() -> AiSettings:
    config_path = Path("config/orchestrator.toml")
    try:
        settings = load_ai_settings(config_path)
        enforce_unattended_ai_policy(settings, config_path=config_path)
    except AiPolicyError as e:
        raise SystemExit(f"codex-roadtrip: {e}") from e
    return settings


def _maybe_end_stale_manual_run(paths: OrchestratorPaths) -> None:
    if not paths.current_run_path.exists():
        return
    try:
        data = json.loads(paths.current_run_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"codex-roadtrip: failed to read {paths.current_run_path}: {e}") from e

    try:
        state = CurrentRunState.from_json_dict(data)
    except Exception:
        return
    if state.mode != "manual":
        return

    with RunLock(paths.run_lock_path) as lock:
        end_current_run(
            paths=paths,
            reason="superseded_by_roadtrip",
            now=datetime.now().astimezone(),
            run_lock=lock,
        )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="codex-roadtrip",
        description="Run repeated orchestrator cycles under one RUN_ID (manual mode).",
    )
    duration = p.add_mutually_exclusive_group(required=True)
    duration.add_argument("--hours", type=float, default=None, help="Run for N hours.")
    duration.add_argument(
        "--until",
        default=None,
        help='Run until local time "YYYY-MM-DD HH:MM".',
    )

    p.add_argument("--cadence-minutes", type=float, default=45.0, help="Cycle cadence in minutes.")
    p.add_argument("--cache-dir", default=None, help="Override orchestrator cache directory.")
    p.add_argument(
        "--repo-id",
        action="append",
        default=None,
        help="Restrict to a specific repo_id from config/repos.toml (repeatable).",
    )
    p.add_argument(
        "--repo-group",
        action="append",
        default=None,
        help="Restrict to a repo_group from config/repos.toml (repeatable).",
    )
    p.add_argument(
        "--max-parallel",
        type=int,
        default=None,
        help="Max repos to run in parallel (defaults to $MAX_PARALLEL or 1).",
    )
    p.add_argument(
        "--idle-ticks-to-end",
        type=int,
        default=3,
        help="End the run after N consecutive idle ticks.",
    )
    p.add_argument(
        "--manual-ttl-hours",
        type=float,
        default=12.0,
        help="Expiry TTL for manual runs (hours).",
    )
    p.add_argument(
        "--min-minutes-to-start-new-bead",
        type=int,
        default=15,
        help="Do not start new beads if less than this remains.",
    )
    p.add_argument(
        "--max-beads-per-tick",
        type=int,
        default=3,
        help="Cap beads attempted per repo per tick.",
    )
    p.add_argument(
        "--diff-cap-files",
        type=int,
        default=25,
        help="Per-tick max files changed (sum across beads).",
    )
    p.add_argument(
        "--diff-cap-lines",
        type=int,
        default=1500,
        help="Per-tick max lines added (sum across beads).",
    )
    p.add_argument(
        "--replan",
        action="store_true",
        help="Recompute each repo run deck even if one already exists for this RUN_ID+repo_id.",
    )
    p.add_argument(
        "--final-review-codex",
        action="store_true",
        help="After ending a run, optionally run a review-only Codex pass (must produce zero diffs).",
    )
    p.add_argument(
        "--focus",
        default=None,
        help="Natural language focus area for the run (interpreted by Codex during execution).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    settings = _load_enforced_ai_settings()

    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)

    max_parallel: int
    if args.max_parallel is not None:
        max_parallel = int(args.max_parallel)
    else:
        raw = os.environ.get("MAX_PARALLEL", "1")
        try:
            max_parallel = int(raw)
        except ValueError as e:
            raise SystemExit(f"codex-roadtrip: invalid MAX_PARALLEL={raw!r} (expected int)") from e

    cadence_minutes = float(args.cadence_minutes)
    if cadence_minutes <= 0:
        raise SystemExit("codex-roadtrip: --cadence-minutes must be > 0")

    started_at = datetime.now().astimezone()
    if args.hours is not None:
        end_at = started_at + timedelta(hours=float(args.hours))
    else:
        assert args.until is not None
        end_at = _parse_until(str(args.until))
    if end_at <= started_at:
        raise SystemExit("codex-roadtrip: duration is non-positive; check --hours/--until")

    _maybe_end_stale_manual_run(paths)

    run_id: str | None = None
    next_cycle_at = started_at
    while True:
        now = datetime.now().astimezone()
        if now >= end_at:
            break
        if now < next_cycle_at:
            sleep_seconds = (next_cycle_at - now).total_seconds()
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            now = datetime.now().astimezone()

        focus = str(args.focus).strip() if args.focus else None
        try:
            cycle_result = run_orchestrator_cycle(
                cache_dir=cache_dir,
                mode="manual",
                ai_settings=settings,
                repo_config_path=Path("config/repos.toml"),
                overlays_dir=Path("config/bead_contracts"),
                repo_ids=args.repo_id,
                repo_groups=args.repo_group,
                max_parallel=max_parallel,
                tick_minutes=cadence_minutes,
                idle_ticks_to_end=int(args.idle_ticks_to_end),
                manual_ttl_hours=float(args.manual_ttl_hours),
                min_minutes_to_start_new_bead=int(args.min_minutes_to_start_new_bead),
                max_beads_per_tick=int(args.max_beads_per_tick),
                diff_cap_files=int(args.diff_cap_files),
                diff_cap_lines=int(args.diff_cap_lines),
                replan=bool(args.replan),
                final_review_codex_review=bool(args.final_review_codex),
                now=now,
                focus=focus,
            )
        except (OrchestratorCycleError, RunLifecycleError) as e:
            raise SystemExit(f"codex-roadtrip: {e}") from e

        ensure = cycle_result.ensure_result
        if ensure.run_id is not None:
            run_id = ensure.run_id

        if ensure.ended:
            break
        if cycle_result.tick_result is not None and cycle_result.tick_result.ended:
            break

        next_cycle_at = now + timedelta(minutes=cadence_minutes)

    if run_id is not None:
        try:
            with RunLock(paths.run_lock_path) as lock:
                ended_run_id = end_current_run(
                    paths=paths,
                    reason="roadtrip_complete",
                    now=datetime.now().astimezone(),
                    run_lock=lock,
                )
                if ended_run_id is None:
                    ended_run_id = run_id
                if ended_run_id is not None:
                    write_final_review(paths, run_id=ended_run_id, ai_settings=settings)
                    if bool(args.final_review_codex):
                        run_review_only_codex_pass(paths, run_id=ended_run_id, ai_settings=settings)
        except Exception as e:
            raise SystemExit(f"codex-roadtrip: failed to end run: {e}") from e
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
