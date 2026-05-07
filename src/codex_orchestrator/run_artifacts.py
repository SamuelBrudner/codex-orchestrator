from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from codex_orchestrator.audit_trail import write_json_atomic
from codex_orchestrator.paths import OrchestratorPaths


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_json_or_none(path: Path, *, error_type: type[Exception] | None = None) -> Any:
    try:
        return read_json(path)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        if error_type is not None:
            raise error_type(f"Failed to parse JSON in {path}: {e}") from e
        return None
    except OSError as e:
        if error_type is not None:
            raise error_type(f"Failed to read {path}: {e}") from e
        return None


def read_json_object(path: Path) -> dict[str, Any] | None:
    payload = read_json_or_none(path)
    return payload if isinstance(payload, dict) else None


def read_json_objects_field(path: Path, *, field: str) -> list[dict[str, Any]]:
    payload = read_json_object(path)
    if payload is None:
        return []
    raw = payload.get(field)
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def artifact_payload(path: Path) -> dict[str, Any]:
    return {
        "path": path.as_posix(),
        "exists": path.exists(),
    }


def list_run_ids(paths: OrchestratorPaths) -> list[str]:
    if not paths.runs_dir.exists():
        return []
    run_ids = [p.name for p in paths.runs_dir.iterdir() if p.is_dir()]
    return sorted(run_ids, reverse=True)


def load_repo_summaries_from_glob(paths: OrchestratorPaths, *, run_id: str) -> list[dict[str, Any]]:
    run_dir = paths.run_dir(run_id)
    summaries: list[dict[str, Any]] = []
    for summary_path in sorted(run_dir.glob("*.summary.json")):
        payload = read_json_object(summary_path)
        if payload is not None:
            summaries.append(payload)
    return sorted(summaries, key=lambda item: str(item.get("repo_id") or ""))


def load_repo_summaries_for_run(paths: OrchestratorPaths, *, run_id: str) -> list[dict[str, Any]]:
    run_summary_repos = read_json_objects_field(paths.run_summary_path(run_id), field="repos")
    if run_summary_repos:
        return sorted(run_summary_repos, key=lambda item: str(item.get("repo_id") or ""))
    return load_repo_summaries_from_glob(paths, run_id=run_id)


def load_repo_ai_summaries(paths: OrchestratorPaths, *, run_id: str) -> dict[str, dict[str, Any]]:
    run_dir = paths.run_dir(run_id)
    summaries: dict[str, dict[str, Any]] = {}
    for summary_path in sorted(run_dir.glob("*.ai_summary.json")):
        payload = read_json_object(summary_path)
        if payload is None:
            continue
        payload_run_id = payload.get("run_id")
        repo_id = payload.get("repo_id")
        if payload_run_id != run_id:
            continue
        if not isinstance(repo_id, str) or not repo_id.strip():
            continue
        summaries[repo_id.strip()] = payload
    return summaries


def tail_text(path: Path, *, lines: int, byte_limit: int = 2_000_000) -> str:
    if lines <= 0:
        return ""
    try:
        data = path.read_bytes()
    except (FileNotFoundError, OSError):
        return ""

    if len(data) > byte_limit:
        data = data[-byte_limit:]
    text = data.decode("utf-8", errors="ignore")
    split = text.splitlines()
    if not split:
        return ""
    return "\n".join(split[-lines:])


def write_run_summary_from_repo_summaries(paths: OrchestratorPaths, *, run_id: str) -> None:
    write_json_atomic(
        paths.run_summary_path(run_id),
        {
            "schema_version": 1,
            "run_id": run_id,
            "repos": load_repo_summaries_from_glob(paths, run_id=run_id),
        },
    )
