from __future__ import annotations

import json
from pathlib import Path

from codex_orchestrator.ai_policy import (
    AiPolicyError,
    AiSettings,
    enforce_unattended_ai_policy,
    load_ai_settings,
)
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_lifecycle import RunLifecycleError, recover_orphaned_current_run


def _load_enforced_ai_settings() -> AiSettings:
    config_path = Path("config/orchestrator.toml")
    try:
        settings = load_ai_settings(config_path)
        enforce_unattended_ai_policy(settings, config_path=config_path)
    except AiPolicyError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e
    return settings


def _load_current_run_id(paths: OrchestratorPaths) -> str:
    try:
        recover_orphaned_current_run(paths=paths)
    except RunLifecycleError as e:
        raise SystemExit(f"codex-orchestrator: failed orphaned-run recovery: {e}") from e

    try:
        data = json.loads(paths.current_run_path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise SystemExit(
            f"codex-orchestrator: no active run found at {paths.current_run_path}; "
            "run `codex-orchestrator tick --mode manual` first, or pass --run-id"
        ) from e
    except json.JSONDecodeError as e:
        raise SystemExit(
            f"codex-orchestrator: failed to parse {paths.current_run_path}: {e}"
        ) from e
    run_id = data.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise SystemExit(f"codex-orchestrator: {paths.current_run_path} missing run_id")
    return run_id


def _format_bool(value: bool) -> str:
    return "true" if value else "false"
