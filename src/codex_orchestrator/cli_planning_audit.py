from __future__ import annotations

import argparse
import sys
from pathlib import Path

from codex_orchestrator.cli_shared import _format_bool, _load_current_run_id
from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir


def _list_planning_audit_repo_ids(run_dir: Path) -> list[str]:
    repo_ids: set[str] = set()
    for path in sorted(run_dir.glob("*.planning_audit.*")):
        name = path.name
        if ".planning_audit." not in name:
            continue
        repo_id = name.split(".planning_audit.", 1)[0].strip()
        if repo_id:
            repo_ids.add(repo_id)
    return sorted(repo_ids)


def _cmd_planning_audit(args: argparse.Namespace) -> int:
    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = str(args.run_id) if args.run_id else _load_current_run_id(paths)
    repo_id = str(args.repo_id)

    if bool(args.no_meta) and args.dump is None:
        raise SystemExit("codex-orchestrator: --no-meta requires --dump md|json")

    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise SystemExit(
            f"codex-orchestrator: run dir not found: {run_dir} "
            "(check --run-id and --cache-dir)"
        )

    json_path = paths.repo_planning_audit_json_path(run_id, repo_id)
    md_path = paths.repo_planning_audit_md_path(run_id, repo_id)
    json_exists = json_path.exists()
    md_exists = md_path.exists()

    if json_exists and md_exists:
        status = "ok"
    elif json_exists or md_exists:
        status = "partial"
    else:
        status = "missing"

    if not bool(args.no_meta):
        print(f"RUN_ID={run_id} repo_id={repo_id} status={status}")
        print(f"json_path={json_path.as_posix()} json_exists={_format_bool(json_exists)}")
        print(f"md_path={md_path.as_posix()} md_exists={_format_bool(md_exists)}")

    def emit_missing_error(*, missing: list[str]) -> int:
        missing_str = ",".join(missing) if missing else "<unknown>"
        known_repos = _list_planning_audit_repo_ids(run_dir)
        known_suffix = f" (available repo_ids: {', '.join(known_repos)})" if known_repos else ""

        out = sys.stderr if bool(args.no_meta) else sys.stdout
        print(f"error=planning_audit_missing missing={missing_str}{known_suffix}", file=out)
        print(
            "next_action="
            f"rerun planning for this repo (regenerates audit): "
            f"codex-orchestrator exec-repo --repo-id {repo_id} --run-id {run_id} --replan",
            file=out,
        )
        return 2

    if args.dump == "md":
        if not md_exists:
            return emit_missing_error(missing=["md"])
        if not bool(args.no_meta):
            print("")
        print(md_path.read_text(encoding="utf-8").rstrip("\n"))
        return 0

    if args.dump == "json":
        if not json_exists:
            return emit_missing_error(missing=["json"])
        if not bool(args.no_meta):
            print("")
        print(json_path.read_text(encoding="utf-8").rstrip("\n"))
        return 0

    if status != "ok" and not bool(args.allow_missing):
        missing: list[str] = []
        if not json_exists:
            missing.append("json")
        if not md_exists:
            missing.append("md")
        return emit_missing_error(missing=missing)

    return 0


def register_planning_audit_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    planning_audit_parser = subparsers.add_parser(
        "planning-audit",
        help="Inspect planning audit artifacts for a RUN_ID + repo_id.",
    )
    planning_audit_parser.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    planning_audit_parser.add_argument(
        "--run-id",
        default=None,
        help="RUN_ID to inspect (defaults to current run).",
    )
    planning_audit_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    planning_audit_parser.add_argument(
        "--dump",
        choices=("md", "json"),
        default=None,
        help="Print the selected artifact contents to stdout.",
    )
    planning_audit_parser.add_argument(
        "--no-meta",
        action="store_true",
        help="Suppress path/existence lines (requires --dump).",
    )
    planning_audit_parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Exit 0 even if one/both artifacts are missing (metadata-only mode).",
    )
    planning_audit_parser.set_defaults(func=_cmd_planning_audit)
