from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

from codex_orchestrator import __version__
from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir
from codex_orchestrator.repo_execution import DiffCaps, RepoExecutionConfig, TickBudget, execute_repo_tick
from codex_orchestrator.repo_inventory import RepoConfigError, load_repo_inventory
from codex_orchestrator.run_lifecycle import RunLifecycleError, tick_run


def _cmd_tick(args: argparse.Namespace) -> int:
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    try:
        result = tick_run(
            paths=paths,
            mode=args.mode,
            actionable_work_found=bool(args.actionable_work_found),
            idle_ticks_to_end=int(args.idle_ticks_to_end),
            manual_ttl=timedelta(hours=float(args.manual_ttl_hours)),
        )
    except RunLifecycleError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e

    if result.ended:
        print(f"RUN_ID={result.run_id} status=ended reason={result.end_reason}")
    else:
        tick_count = result.state.tick_count if result.state is not None else "?"
        print(
            f"RUN_ID={result.run_id} status=active tick={tick_count} started_new={result.started_new}"
        )
    return 0


def _load_current_run_id(paths: OrchestratorPaths) -> str:
    try:
        data = json.loads(paths.current_run_path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise SystemExit(
            f"codex-orchestrator: no active run found at {paths.current_run_path}; "
            "run `codex-orchestrator tick --mode manual` first, or pass --run-id"
        ) from e
    except json.JSONDecodeError as e:
        raise SystemExit(f"codex-orchestrator: failed to parse {paths.current_run_path}: {e}") from e
    run_id = data.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise SystemExit(f"codex-orchestrator: {paths.current_run_path} missing run_id")
    return run_id


def _cmd_exec_repo(args: argparse.Namespace) -> int:
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = str(args.run_id) if args.run_id else _load_current_run_id(paths)

    try:
        inventory = load_repo_inventory(Path("config/repos.toml"))
    except RepoConfigError as e:
        raise SystemExit(f"codex-orchestrator: invalid config/repos.toml: {e}") from e

    repo_id = str(args.repo_id)
    policy = inventory.repos.get(repo_id)
    if policy is None:
        known = ", ".join(sorted(inventory.repos)) or "<none>"
        raise SystemExit(f"codex-orchestrator: unknown repo_id {repo_id!r} (known: {known})")

    overlay_path = Path("config/bead_contracts") / f"{repo_id}.toml"
    if not overlay_path.exists():
        raise SystemExit(f"codex-orchestrator: missing overlay {overlay_path}")

    tick_minutes = float(args.tick_minutes)
    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(minutes=tick_minutes))
    config = RepoExecutionConfig(
        tick_budget=timedelta(minutes=tick_minutes),
        min_minutes_to_start_new_bead=int(args.min_minutes_to_start_new_bead),
        max_beads_per_tick=int(args.max_beads_per_tick),
        diff_caps=DiffCaps(
            max_files_changed=int(args.diff_cap_files),
            max_lines_added=int(args.diff_cap_lines),
        ),
    )

    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=overlay_path,
        tick=tick,
        config=config,
    )

    if result.skipped:
        print(f"repo_id={repo_id} status=skipped reason={result.skip_reason}")
    else:
        print(
            f"repo_id={repo_id} status=ok closed={result.beads_closed} "
            f"attempted={result.beads_attempted} stop_reason={result.stop_reason}"
        )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="codex-orchestrator",
        description="Global Codex Orchestrator (work-in-progress).",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command")

    tick_parser = subparsers.add_parser("tick", help="Run one orchestrator tick.")
    tick_parser.add_argument(
        "--mode",
        choices=("automated", "manual"),
        default="automated",
        help="Run mode (scheduler=automated, roadtrip=manual).",
    )
    tick_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    tick_parser.add_argument(
        "--idle-ticks-to-end",
        type=int,
        default=3,
        help="End the run after N consecutive idle ticks.",
    )
    tick_parser.add_argument(
        "--manual-ttl-hours",
        type=float,
        default=12.0,
        help="Expiry TTL for manual runs (hours).",
    )
    tick_parser.add_argument(
        "--actionable-work-found",
        action="store_true",
        help="Record that actionable work was found this tick (resets idle counter).",
    )
    tick_parser.set_defaults(func=_cmd_tick)

    exec_repo_parser = subparsers.add_parser("exec-repo", help="Execute one repo deck tick.")
    exec_repo_parser.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    exec_repo_parser.add_argument("--run-id", default=None, help="Override RUN_ID (defaults to current_run.json)")
    exec_repo_parser.add_argument("--cache-dir", default=None, help="Override orchestrator cache directory.")
    exec_repo_parser.add_argument("--tick-minutes", type=float, default=45.0, help="Tick time budget in minutes.")
    exec_repo_parser.add_argument(
        "--min-minutes-to-start-new-bead",
        type=int,
        default=15,
        help="Do not start new beads if less than this remains.",
    )
    exec_repo_parser.add_argument(
        "--max-beads-per-tick",
        type=int,
        default=3,
        help="Cap beads attempted per repo per tick.",
    )
    exec_repo_parser.add_argument(
        "--diff-cap-files",
        type=int,
        default=25,
        help="Per-tick max files changed (sum across beads).",
    )
    exec_repo_parser.add_argument(
        "--diff-cap-lines",
        type=int,
        default=1500,
        help="Per-tick max lines added (sum across beads).",
    )
    exec_repo_parser.set_defaults(func=_cmd_exec_repo)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return int(args.func(args))
