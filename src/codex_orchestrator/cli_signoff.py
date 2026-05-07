from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir
from codex_orchestrator.run_signoff import (
    RunSignoff,
    RunSignoffError,
    find_latest_ended_run_id,
    validate_run_signoff,
    write_run_signoff,
)


def _signoff_paths(args: argparse.Namespace) -> OrchestratorPaths:
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    return OrchestratorPaths(cache_dir=cache_dir)


def _signoff_reviewer(args: argparse.Namespace) -> str:
    reviewer = str(args.reviewer or os.environ.get("CODEX_ORCHESTRATOR_REVIEWER") or "").strip()
    if not reviewer:
        raise SystemExit(
            "codex-orchestrator: reviewer is required (pass --reviewer or set $CODEX_ORCHESTRATOR_REVIEWER)"
        )
    return reviewer


def _signoff_run_id(args: argparse.Namespace, *, paths: OrchestratorPaths) -> str:
    if args.run_id:
        return str(args.run_id)
    try:
        run_id_or_none = find_latest_ended_run_id(paths)
    except RunSignoffError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e
    if run_id_or_none is None:
        raise SystemExit(
            "codex-orchestrator: no ended runs found (pass --run-id or ensure run_end.json exists under "
            f"{paths.runs_dir})"
        )
    return run_id_or_none


def _validate_run_signoff_or_exit(paths: OrchestratorPaths, *, run_id: str) -> None:
    try:
        validate_run_signoff(paths, run_id=run_id)
    except RunSignoffError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e


def _write_run_signoff_or_exit(
    *, paths: OrchestratorPaths, run_id: str, reviewer: str, notes: str | None
) -> RunSignoff:
    reviewed_at = datetime.now().astimezone()
    try:
        return write_run_signoff(
            paths,
            run_id=run_id,
            reviewer=reviewer,
            reviewed_at=reviewed_at,
            notes=notes,
        )
    except RunSignoffError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e


def _signoff_output(
    paths: OrchestratorPaths, *, run_id: str, signoff: RunSignoff
) -> tuple[Path, Path, str, str]:
    return (
        paths.run_signoff_json_path(run_id),
        paths.run_signoff_md_path(run_id),
        signoff.reviewer,
        signoff.reviewed_at.isoformat(),
    )


def _print_signoff_success(
    *, run_id: str, json_path: Path, md_path: Path, reviewer: str, reviewed_at: str
) -> None:
    lines = [
        f"RUN_ID={run_id} status=ok",
        f"json_path={json_path.as_posix()}",
        f"md_path={md_path.as_posix()}",
        f"reviewer={reviewer}",
        f"reviewed_at={reviewed_at}",
    ]
    print("\n".join(lines))


def cmd_signoff(args: argparse.Namespace) -> int:
    paths = _signoff_paths(args)
    reviewer = _signoff_reviewer(args)
    run_id = _signoff_run_id(args, paths=paths)
    notes = str(args.notes) if args.notes is not None else None

    signoff = _write_run_signoff_or_exit(
        paths=paths,
        run_id=run_id,
        reviewer=reviewer,
        notes=notes,
    )
    _validate_run_signoff_or_exit(paths, run_id=run_id)
    json_path, md_path, reviewer_value, reviewed_at = _signoff_output(
        paths, run_id=run_id, signoff=signoff
    )

    _print_signoff_success(
        run_id=run_id,
        json_path=json_path,
        md_path=md_path,
        reviewer=reviewer_value,
        reviewed_at=reviewed_at,
    )
    return 0
