from __future__ import annotations

import argparse

from codex_orchestrator import __version__
from codex_orchestrator.cli_overlay import register_overlay_subcommands
from codex_orchestrator.cli_planning_audit import register_planning_audit_subcommands
from codex_orchestrator.cli_repo_admin import register_repo_admin_subcommands
from codex_orchestrator.cli_run import register_run_subcommands
from codex_orchestrator.cli_shared import _load_current_run_id, _load_enforced_ai_settings


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
    register_run_subcommands(subparsers)
    register_overlay_subcommands(subparsers)
    register_planning_audit_subcommands(subparsers)
    register_repo_admin_subcommands(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 1
    return int(func(args))


__all__ = [
    "_build_parser",
    "_load_current_run_id",
    "_load_enforced_ai_settings",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
