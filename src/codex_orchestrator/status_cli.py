from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_current_run_id(paths: OrchestratorPaths) -> str:
    try:
        data = _read_json(paths.current_run_path)
    except FileNotFoundError as e:
        raise SystemExit(
            f"codex-status: no active run found at {paths.current_run_path}; "
            "pass --run-id or run `codex-orchestrator tick --mode manual` first"
        ) from e
    except json.JSONDecodeError as e:
        raise SystemExit(f"codex-status: failed to parse {paths.current_run_path}: {e}") from e

    run_id = data.get("run_id") if isinstance(data, dict) else None
    if not isinstance(run_id, str) or not run_id.strip():
        raise SystemExit(f"codex-status: {paths.current_run_path} missing run_id")
    return run_id


def _load_repo_summaries(paths: OrchestratorPaths, *, run_id: str) -> list[dict[str, Any]]:
    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise SystemExit(f"codex-status: run dir not found: {run_dir}")
    summaries: list[dict[str, Any]] = []
    for path in sorted(run_dir.glob("*.summary.json")):
        try:
            payload = _read_json(path)
        except Exception:
            continue
        if isinstance(payload, dict):
            summaries.append(payload)
    return summaries


def _format_repo_line(summary: dict[str, Any]) -> str:
    repo_id = summary.get("repo_id", "<unknown>")
    skipped = bool(summary.get("skipped"))
    skip_reason = summary.get("skip_reason")
    stop_reason = summary.get("stop_reason")
    attempted = summary.get("beads_attempted", 0)
    closed = summary.get("beads_closed", 0)
    next_action = summary.get("next_action", "")

    if skipped:
        status = f"skipped:{skip_reason or 'unknown'}"
    else:
        status = f"stop:{stop_reason or 'unknown'}"

    parts = [str(repo_id), status, f"attempted={attempted}", f"closed={closed}"]
    if isinstance(next_action, str) and next_action.strip():
        parts.append(f"next={next_action}")
    return " ".join(parts)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="codex-status",
        description="Show a concise status view for a RUN_ID (one line per repo).",
    )
    p.add_argument("--run-id", default=None, help="RUN_ID to inspect (defaults to current run).")
    p.add_argument("--cache-dir", default=None, help="Override orchestrator cache directory.")
    p.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON (run_summary.json if available, else aggregated per-repo summaries).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = str(args.run_id) if args.run_id else _load_current_run_id(paths)

    run_summary_path = paths.run_summary_path(run_id)
    if args.json and run_summary_path.exists():
        print(run_summary_path.read_text(encoding="utf-8").rstrip("\n"))
        return 0

    summaries = _load_repo_summaries(paths, run_id=run_id)
    if args.json:
        print(json.dumps({"schema_version": 1, "run_id": run_id, "repos": summaries}, indent=2, sort_keys=True))
        return 0

    print(f"RUN_ID={run_id}")
    if not summaries:
        print("(no repo summaries found)")
        return 0
    for s in summaries:
        print(_format_repo_line(s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

