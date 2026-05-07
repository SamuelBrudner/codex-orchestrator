from __future__ import annotations

import argparse
import os
from pathlib import Path

from codex_orchestrator.cli_common import load_enforced_ai_settings as _load_enforced_ai_settings
from codex_orchestrator.orchestrator_cycle import OrchestratorCycleError, run_orchestrator_cycle
from codex_orchestrator.paths import default_cache_dir
from codex_orchestrator.run_lifecycle import RunLifecycleError


def cmd_run(args: argparse.Namespace) -> int:
    ai_settings = _load_enforced_ai_settings()
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()

    max_parallel: int | None
    if args.max_parallel is not None:
        max_parallel = int(args.max_parallel)
    else:
        raw = os.environ.get("MAX_PARALLEL")
        if raw is None or str(raw).strip() == "":
            max_parallel = None
        else:
            try:
                max_parallel = int(raw)
            except ValueError as e:
                raise SystemExit(f"codex-orchestrator: invalid MAX_PARALLEL={raw!r} (expected int)") from e

    focus = str(args.focus).strip() if args.focus else None
    try:
        result = run_orchestrator_cycle(
            cache_dir=cache_dir,
            mode=args.mode,
            ai_settings=ai_settings,
            repo_config_path=Path("config/repos.toml"),
            overlays_dir=Path("config/bead_contracts"),
            repo_ids=args.repo_id,
            repo_groups=args.repo_group,
            max_parallel=max_parallel,
            tick_minutes=float(args.tick_minutes),
            idle_ticks_to_end=int(args.idle_ticks_to_end),
            manual_ttl_hours=float(args.manual_ttl_hours),
            min_minutes_to_start_new_bead=int(args.min_minutes_to_start_new_bead),
            max_beads_per_tick=int(args.max_beads_per_tick),
            diff_cap_files=int(args.diff_cap_files),
            diff_cap_lines=int(args.diff_cap_lines),
            replan=bool(args.replan),
            final_review_codex_review=bool(args.final_review_codex),
            review_every_beads=int(args.review_every_beads) if args.review_every_beads is not None else None,
            focus=focus,
        )
    except (OrchestratorCycleError, RunLifecycleError) as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e

    ensure = result.ensure_result
    if ensure.ended:
        if ensure.run_id is None:
            print(f"status=skipped reason={ensure.end_reason}")
        else:
            print(f"RUN_ID={ensure.run_id} status=ended reason={ensure.end_reason}")
        return 0

    run_id = ensure.run_id
    assert run_id is not None
    tick_result = result.tick_result
    assert tick_result is not None

    if tick_result.ended:
        print(f"RUN_ID={run_id} status=ended reason={tick_result.end_reason}")
    else:
        tick_count = tick_result.state.tick_count if tick_result.state is not None else "?"
        print(f"RUN_ID={run_id} status=active tick={tick_count} started_new={ensure.started_new}")

    for repo_result in result.repo_results:
        if repo_result.skipped:
            print(f"repo_id={repo_result.repo_id} status=skipped reason={repo_result.skip_reason}")
        else:
            print(
                f"repo_id={repo_result.repo_id} status=ok attempted={repo_result.beads_attempted} "
                f"closed={repo_result.beads_closed} stop_reason={repo_result.stop_reason}"
            )
    return 0
