from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from codex_orchestrator import __version__
from codex_orchestrator.ai_policy import AiPolicyError, AiSettings, enforce_unattended_ai_policy, load_ai_settings
from codex_orchestrator.beads_subprocess import BdCliError, bd_init, bd_list_ids, bd_ready
from codex_orchestrator.contract_overlays import (
    ContractOverlay,
    ContractOverlayError,
    ContractOverlayPatch,
    load_contract_overlay,
)
from codex_orchestrator.git_subprocess import GitError, git_current_branch
from codex_orchestrator.orchestrator_cycle import OrchestratorCycleError, run_orchestrator_cycle
from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir
from codex_orchestrator.planner import plan_deck_items
from codex_orchestrator.repo_execution import (
    DiffCaps,
    RepoExecutionConfig,
    TickBudget,
    execute_repo_tick,
)
from codex_orchestrator.repo_inventory import RepoConfigError, load_repo_inventory
from codex_orchestrator.run_closure_review import (
    RunClosureReviewError,
    run_review_only_codex_pass,
    write_final_review,
)
from codex_orchestrator.run_lifecycle import (
    RunLifecycleError,
    recover_orphaned_current_run,
    tick_run,
)
from codex_orchestrator.run_signoff import (
    RunSignoff,
    RunSignoffError,
    find_latest_ended_run_id,
    validate_run_signoff,
    write_run_signoff,
)

_TOML_BARE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_DEFAULT_INIT_DENY_ROOTS: tuple[str, ...] = (
    ".benchmarks",
    ".idea",
    ".pytest_cache",
    ".ruff_cache",
    "__marimo__",
    "__pycache__",
    "data",
    "datasets",
    "executed_notebooks",
    "figs",
    "figures",
    "logs",
    "results",
)


def _load_enforced_ai_settings() -> AiSettings:
    config_path = Path("config/orchestrator.toml")
    try:
        settings = load_ai_settings(config_path)
        enforce_unattended_ai_policy(settings, config_path=config_path)
    except AiPolicyError as e:
        raise SystemExit(f"codex-orchestrator: {e}") from e
    return settings


def _cmd_tick(args: argparse.Namespace) -> int:
    ai_settings = _load_enforced_ai_settings()
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
        if result.run_id is not None:
            try:
                write_final_review(paths, run_id=result.run_id, ai_settings=ai_settings)
                if bool(args.final_review_codex):
                    run_review_only_codex_pass(
                        paths,
                        run_id=result.run_id,
                        ai_settings=ai_settings,
                        repo_config_path=Path("config/repos.toml"),
                    )
            except RunClosureReviewError as e:
                raise SystemExit(f"codex-orchestrator: {e}") from e
        if result.run_id is None:
            print(f"status=skipped reason={result.end_reason}")
        else:
            print(f"RUN_ID={result.run_id} status=ended reason={result.end_reason}")
    else:
        tick_count = result.state.tick_count if result.state is not None else "?"
        print(
            f"RUN_ID={result.run_id} status=active tick={tick_count} "
            f"started_new={result.started_new}"
        )
    return 0


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


def _format_bool(v: bool) -> str:
    return "true" if v else "false"


def _list_planning_audit_repo_ids(run_dir: Path) -> list[str]:
    repo_ids: set[str] = set()
    for p in sorted(run_dir.glob("*.planning_audit.*")):
        name = p.name
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

    status: str
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


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _read_json_objects(path: Path, *, field: str) -> list[dict[str, Any]]:
    payload = _read_json_object(path)
    if payload is None:
        return []
    raw = payload.get(field)
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            out.append(item)
    return out


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _repo_status(summary: dict[str, Any]) -> str:
    if bool(summary.get("skipped")):
        reason = summary.get("skip_reason")
        return f"skipped:{reason or 'unknown'}"
    stop_reason = summary.get("stop_reason")
    return f"stop:{stop_reason or 'unknown'}"


def _repo_failure_examples(summary: dict[str, Any], *, limit: int = 3) -> list[str]:
    examples: list[str] = []

    failures = summary.get("failures")
    if isinstance(failures, list):
        for failure in failures:
            if isinstance(failure, str) and failure.strip():
                examples.append(failure.strip())

    beads = summary.get("beads")
    if isinstance(beads, list):
        for bead in beads:
            if not isinstance(bead, dict):
                continue
            if bead.get("outcome") != "failed":
                continue
            detail = str(bead.get("detail") or "").strip()
            if not detail:
                continue
            bead_id = str(bead.get("bead_id") or "").strip()
            prefix = f"{bead_id}: " if bead_id else ""
            examples.append(prefix + detail)

    # Preserve order while removing duplicates.
    deduped = list(dict.fromkeys(examples))
    return deduped[:limit]


def _repo_has_failure(summary: dict[str, Any]) -> bool:
    if bool(summary.get("skipped")):
        return True
    if str(summary.get("stop_reason") or "") in {"error", "blocked"}:
        return True
    if _repo_failure_examples(summary, limit=1):
        return True
    return False


def _load_repo_summaries_for_run(paths: OrchestratorPaths, *, run_id: str) -> list[dict[str, Any]]:
    run_summary_repos = _read_json_objects(paths.run_summary_path(run_id), field="repos")
    if run_summary_repos:
        return sorted(run_summary_repos, key=lambda item: str(item.get("repo_id") or ""))

    run_dir = paths.run_dir(run_id)
    summaries: list[dict[str, Any]] = []
    for summary_path in sorted(run_dir.glob("*.summary.json")):
        payload = _read_json_object(summary_path)
        if payload is not None:
            summaries.append(payload)
    return sorted(summaries, key=lambda item: str(item.get("repo_id") or ""))


def _list_run_ids(paths: OrchestratorPaths) -> list[str]:
    if not paths.runs_dir.exists():
        return []
    run_ids = [p.name for p in paths.runs_dir.iterdir() if p.is_dir()]
    return sorted(run_ids, reverse=True)


def _maybe_load_current_run_id(paths: OrchestratorPaths) -> str | None:
    try:
        recover_orphaned_current_run(paths=paths)
    except RunLifecycleError as e:
        raise SystemExit(f"codex-orchestrator: failed orphaned-run recovery: {e}") from e

    payload = _read_json_object(paths.current_run_path)
    if payload is None:
        return None
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return None
    return run_id


def _run_artifact_payload(path: Path) -> dict[str, Any]:
    return {
        "path": path.as_posix(),
        "exists": path.exists(),
    }


def _build_run_overview(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    current_run_id: str | None,
) -> dict[str, Any]:
    metadata = _read_json_object(paths.run_metadata_path(run_id))
    run_end = _read_json_object(paths.run_end_path(run_id))
    repos = _load_repo_summaries_for_run(paths, run_id=run_id)

    attempted_total = sum(_as_int(repo.get("beads_attempted")) for repo in repos)
    closed_total = sum(_as_int(repo.get("beads_closed")) for repo in repos)
    failed_repo_count = sum(1 for repo in repos if _repo_has_failure(repo))

    if current_run_id == run_id:
        status = "active"
    elif run_end is not None:
        status = "ended"
    elif metadata is not None:
        status = "incomplete"
    else:
        status = "unknown"

    return {
        "run_id": run_id,
        "status": status,
        "mode": metadata.get("mode") if metadata is not None else None,
        "started_at": metadata.get("created_at") if metadata is not None else None,
        "ended_at": run_end.get("ended_at") if run_end is not None else None,
        "end_reason": run_end.get("reason") if run_end is not None else None,
        "repo_count": len(repos),
        "attempted_total": attempted_total,
        "closed_total": closed_total,
        "failed_repo_count": failed_repo_count,
    }


def _tail_text(path: Path, *, lines: int, byte_limit: int = 2_000_000) -> str:
    if lines <= 0:
        return ""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ""
    except OSError:
        return ""

    if len(data) > byte_limit:
        data = data[-byte_limit:]
    text = data.decode("utf-8", errors="ignore")
    split = text.splitlines()
    if not split:
        return ""
    return "\n".join(split[-lines:])


def _build_repo_detail(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    summary: dict[str, Any],
    tail_lines: int,
) -> dict[str, Any]:
    repo_id = str(summary.get("repo_id") or "<unknown>")
    failure_examples = _repo_failure_examples(summary)
    detail: dict[str, Any] = {
        "repo_id": repo_id,
        "status": _repo_status(summary),
        "attempted": _as_int(summary.get("beads_attempted")),
        "closed": _as_int(summary.get("beads_closed")),
        "next_action": summary.get("next_action"),
        "has_failure": _repo_has_failure(summary),
        "failure_count": len(failure_examples),
        "failure_examples": failure_examples,
        "paths": {
            "summary": _run_artifact_payload(paths.repo_summary_path(run_id, repo_id)),
            "exec_log": _run_artifact_payload(paths.repo_exec_log_path(run_id, repo_id)),
            "stdout_log": _run_artifact_payload(paths.repo_stdout_log_path(run_id, repo_id)),
            "stderr_log": _run_artifact_payload(paths.repo_stderr_log_path(run_id, repo_id)),
            "events": _run_artifact_payload(paths.repo_events_path(run_id, repo_id)),
        },
    }

    if tail_lines > 0:
        detail["log_tails"] = {
            "exec_log": _tail_text(paths.repo_exec_log_path(run_id, repo_id), lines=tail_lines),
            "stderr_log": _tail_text(paths.repo_stderr_log_path(run_id, repo_id), lines=tail_lines),
            "events": _tail_text(paths.repo_events_path(run_id, repo_id), lines=tail_lines),
        }

    return detail


def _build_run_detail(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    current_run_id: str | None,
    repo_ids: tuple[str, ...],
    tail_lines: int,
) -> dict[str, Any]:
    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise SystemExit(
            f"codex-orchestrator: run dir not found: {run_dir} "
            "(check --run-id/--latest and --cache-dir)"
        )

    overview = _build_run_overview(paths, run_id=run_id, current_run_id=current_run_id)
    metadata = _read_json_object(paths.run_metadata_path(run_id))
    run_end = _read_json_object(paths.run_end_path(run_id))

    summaries = _load_repo_summaries_for_run(paths, run_id=run_id)
    if repo_ids:
        repo_filter = set(repo_ids)
        summaries = [summary for summary in summaries if str(summary.get("repo_id") or "") in repo_filter]

    repos = [
        _build_repo_detail(paths, run_id=run_id, summary=summary, tail_lines=tail_lines)
        for summary in summaries
    ]

    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "status": overview["status"],
        "run_dir": run_dir.as_posix(),
        "current": current_run_id == run_id,
        "overview": overview,
        "run_metadata": metadata,
        "run_end": run_end,
        "artifacts": {
            "run_json": _run_artifact_payload(paths.run_metadata_path(run_id)),
            "run_end": _run_artifact_payload(paths.run_end_path(run_id)),
            "run_summary": _run_artifact_payload(paths.run_summary_path(run_id)),
            "run_log": _run_artifact_payload(paths.run_log_path(run_id)),
            "final_review_json": _run_artifact_payload(paths.final_review_json_path(run_id)),
            "final_review_md": _run_artifact_payload(paths.final_review_md_path(run_id)),
            "run_signoff_json": _run_artifact_payload(paths.run_signoff_json_path(run_id)),
            "run_signoff_md": _run_artifact_payload(paths.run_signoff_md_path(run_id)),
        },
        "repos": repos,
        "totals": {
            "repo_count": len(repos),
            "failed_repo_count": sum(1 for repo in repos if bool(repo.get("has_failure"))),
            "attempted_total": sum(_as_int(repo.get("attempted")) for repo in repos),
            "closed_total": sum(_as_int(repo.get("closed")) for repo in repos),
        },
    }
    if tail_lines > 0:
        payload["run_log_tail"] = _tail_text(paths.run_log_path(run_id), lines=tail_lines)

    return payload


def _format_run_overview_line(entry: dict[str, Any]) -> str:
    run_id = entry.get("run_id") or "<unknown>"
    status = entry.get("status") or "unknown"
    started_at = entry.get("started_at") or "-"
    ended_at = entry.get("ended_at") or "-"
    end_reason = entry.get("end_reason") or "-"
    repos = _as_int(entry.get("repo_count"))
    attempted = _as_int(entry.get("attempted_total"))
    closed = _as_int(entry.get("closed_total"))
    failed = _as_int(entry.get("failed_repo_count"))
    mode = entry.get("mode") or "-"
    return (
        f"RUN_ID={run_id} status={status} mode={mode} started_at={started_at} "
        f"ended_at={ended_at} reason={end_reason} repos={repos} "
        f"attempted={attempted} closed={closed} failed={failed}"
    )


def _print_run_detail(payload: dict[str, Any], *, tail_lines: int) -> None:
    run_id = payload.get("run_id") or "<unknown>"
    status = payload.get("status") or "unknown"
    print(f"RUN_ID={run_id} status={status}")
    print(f"run_dir={payload.get('run_dir')}")
    print(f"current={_format_bool(bool(payload.get('current')))}")

    metadata = payload.get("run_metadata")
    if isinstance(metadata, dict):
        for field in (
            "mode",
            "created_at",
            "last_tick_at",
            "expires_at",
            "tick_count",
            "consecutive_idle_ticks",
            "beads_attempted_total",
            "beads_attempted_since_review",
        ):
            if field in metadata:
                print(f"{field}={metadata[field]}")

    run_end = payload.get("run_end")
    if isinstance(run_end, dict):
        if "ended_at" in run_end:
            print(f"ended_at={run_end['ended_at']}")
        if "reason" in run_end:
            print(f"end_reason={run_end['reason']}")

    totals = payload.get("totals")
    if isinstance(totals, dict):
        print(
            f"repos={_as_int(totals.get('repo_count'))} "
            f"failed_repos={_as_int(totals.get('failed_repo_count'))} "
            f"attempted={_as_int(totals.get('attempted_total'))} "
            f"closed={_as_int(totals.get('closed_total'))}"
        )

    artifacts = payload.get("artifacts")
    if isinstance(artifacts, dict):
        for name, artifact in artifacts.items():
            if not isinstance(artifact, dict):
                continue
            exists = _format_bool(bool(artifact.get("exists")))
            path = artifact.get("path")
            print(f"artifact={name} exists={exists} path={path}")

    repos = payload.get("repos")
    if isinstance(repos, list):
        for repo in repos:
            if not isinstance(repo, dict):
                continue
            repo_id = repo.get("repo_id") or "<unknown>"
            status_text = repo.get("status") or "unknown"
            attempted = _as_int(repo.get("attempted"))
            closed = _as_int(repo.get("closed"))
            has_failure = _format_bool(bool(repo.get("has_failure")))
            failure_count = _as_int(repo.get("failure_count"))
            print(
                f"repo_id={repo_id} status={status_text} attempted={attempted} "
                f"closed={closed} failed={has_failure} failures={failure_count}"
            )

            next_action = repo.get("next_action")
            if isinstance(next_action, str) and next_action.strip():
                print(f"next_action={next_action}")

            failure_examples = repo.get("failure_examples")
            if isinstance(failure_examples, list):
                for failure in failure_examples:
                    if isinstance(failure, str) and failure.strip():
                        print(f"failure={failure}")

            paths = repo.get("paths")
            if isinstance(paths, dict):
                for key, artifact in paths.items():
                    if not isinstance(artifact, dict):
                        continue
                    exists = _format_bool(bool(artifact.get("exists")))
                    path = artifact.get("path")
                    print(f"repo_artifact={repo_id}:{key} exists={exists} path={path}")

            if tail_lines > 0:
                log_tails = repo.get("log_tails")
                if isinstance(log_tails, dict):
                    for key in ("exec_log", "stderr_log", "events"):
                        tail = log_tails.get(key)
                        if not isinstance(tail, str):
                            continue
                        print(f"repo_log_tail={repo_id}:{key}")
                        print(tail if tail else "(empty)")

    if tail_lines > 0:
        run_log_tail = payload.get("run_log_tail")
        if isinstance(run_log_tail, str):
            print("run_log_tail=orchestrator.log")
            print(run_log_tail if run_log_tail else "(empty)")


def _cmd_run_info(args: argparse.Namespace) -> int:
    if args.run_id and bool(args.latest):
        raise SystemExit("codex-orchestrator: pass either --run-id or --latest (not both)")
    if int(args.limit) < 1:
        raise SystemExit("codex-orchestrator: --limit must be >= 1")
    if int(args.tail_lines) < 0:
        raise SystemExit("codex-orchestrator: --tail-lines must be >= 0")

    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else default_cache_dir()
    paths = OrchestratorPaths(cache_dir=cache_dir)
    current_run_id = _maybe_load_current_run_id(paths)

    requested_run_id: str | None = None
    if args.run_id:
        requested_run_id = str(args.run_id)
    elif bool(args.latest):
        run_ids = _list_run_ids(paths)
        if not run_ids:
            raise SystemExit(f"codex-orchestrator: no runs found under {paths.runs_dir}")
        requested_run_id = run_ids[0]

    if requested_run_id is None:
        if args.repo_id:
            raise SystemExit("codex-orchestrator: --repo-id requires --run-id or --latest")
        if int(args.tail_lines) > 0:
            raise SystemExit("codex-orchestrator: --tail-lines requires --run-id or --latest")
        run_ids = _list_run_ids(paths)[: int(args.limit)]
        payload = {
            "schema_version": 1,
            "runs": [
                _build_run_overview(paths, run_id=run_id, current_run_id=current_run_id)
                for run_id in run_ids
            ],
        }
        if bool(args.json):
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        if not payload["runs"]:
            print(f"status=empty runs_dir={paths.runs_dir.as_posix()}")
            return 0
        for run in payload["runs"]:
            print(_format_run_overview_line(run))
        return 0

    detail = _build_run_detail(
        paths,
        run_id=requested_run_id,
        current_run_id=current_run_id,
        repo_ids=tuple(args.repo_id or ()),
        tail_lines=int(args.tail_lines),
    )
    if bool(args.json):
        print(json.dumps(detail, indent=2, sort_keys=True))
        return 0
    _print_run_detail(detail, tail_lines=int(args.tail_lines))
    return 0


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


def _cmd_signoff(args: argparse.Namespace) -> int:
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


def _cmd_exec_repo(args: argparse.Namespace) -> int:
    ai_settings = _load_enforced_ai_settings()
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
    focus = str(args.focus).strip() if args.focus else None
    config = RepoExecutionConfig(
        tick_budget=timedelta(minutes=tick_minutes),
        min_minutes_to_start_new_bead=int(args.min_minutes_to_start_new_bead),
        max_beads_per_tick=int(args.max_beads_per_tick),
        diff_caps=DiffCaps(
            max_files_changed=int(args.diff_cap_files),
            max_lines_added=int(args.diff_cap_lines),
        ),
        replan=bool(args.replan),
        ai_settings=ai_settings,
        focus=focus,
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


def _cmd_run(args: argparse.Namespace) -> int:
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


def _toml_quote_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{escaped}"'


def _toml_quote_str_list(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(_toml_quote_string(v) for v in values) + "]"


def _toml_quote_path_list(values: tuple[Path, ...]) -> str:
    return _toml_quote_str_list(tuple(p.as_posix() for p in values))


def _render_overlay_patch_lines(patch: ContractOverlayPatch) -> list[str]:
    lines: list[str] = []
    if patch.time_budget_minutes is not None:
        lines.append(f"time_budget_minutes = {patch.time_budget_minutes}")
    if patch.validation_commands is not None:
        lines.append(f"validation_commands = {_toml_quote_str_list(patch.validation_commands)}")
    if patch.env is not None:
        lines.append(f"env = {_toml_quote_string(patch.env)}")
    if patch.allow_env_creation is not None:
        lines.append(f"allow_env_creation = {'true' if patch.allow_env_creation else 'false'}")
    if patch.requires_notebook_execution is not None:
        lines.append(
            "requires_notebook_execution = "
            f"{'true' if patch.requires_notebook_execution else 'false'}"
        )
    if patch.enforce_given_when_then is not None:
        lines.append(
            "enforce_given_when_then = "
            f"{'true' if patch.enforce_given_when_then else 'false'}"
        )
    if patch.enable_planning_audit_issue_creation is not None:
        lines.append(
            "enable_planning_audit_issue_creation = "
            f"{'true' if patch.enable_planning_audit_issue_creation else 'false'}"
        )
    if patch.planning_audit_issue_limit is not None:
        lines.append(f"planning_audit_issue_limit = {patch.planning_audit_issue_limit}")
    if patch.enable_notebook_refactor_issue_creation is not None:
        lines.append(
            "enable_notebook_refactor_issue_creation = "
            f"{'true' if patch.enable_notebook_refactor_issue_creation else 'false'}"
        )
    if patch.notebook_refactor_issue_limit is not None:
        lines.append(f"notebook_refactor_issue_limit = {patch.notebook_refactor_issue_limit}")
    if patch.allowed_roots is not None:
        lines.append(f"allowed_roots = {_toml_quote_path_list(patch.allowed_roots)}")
    if patch.deny_roots is not None:
        lines.append(f"deny_roots = {_toml_quote_path_list(patch.deny_roots)}")
    return lines


def _render_contract_overlay_toml(overlay: ContractOverlay) -> str:
    lines: list[str] = []
    lines.extend(
        [
            f"# Contract overlay for `{overlay.repo_id}`.",
            "#",
            "# `[defaults]` applies to all beads unless overridden.",
            "# `[beads.\"<BEAD_ID>\"]` defines per-bead overrides (keyed by Beads issue id).",
            "",
            "[defaults]",
        ]
    )
    defaults_lines = _render_overlay_patch_lines(overlay.defaults)
    if defaults_lines:
        lines.extend(defaults_lines)
    lines.append("")

    for bead_id in sorted(overlay.beads):
        patch = overlay.beads[bead_id]
        patch_lines = _render_overlay_patch_lines(patch)
        lines.append(f'[beads.{_toml_quote_string(bead_id)}]')
        lines.extend(patch_lines)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _cmd_overlay_dry_run(args: argparse.Namespace) -> int:
    try:
        inventory = load_repo_inventory(Path("config/repos.toml"))
    except RepoConfigError as e:
        raise SystemExit(f"codex-orchestrator: invalid config/repos.toml: {e}") from e

    repo_id = str(args.repo_id)
    repo_policy = inventory.repos.get(repo_id)
    if repo_policy is None:
        known = ", ".join(sorted(inventory.repos)) or "<none>"
        raise SystemExit(f"codex-orchestrator: unknown repo_id {repo_id!r} (known: {known})")

    overlay_path = Path("config/bead_contracts") / f"{repo_id}.toml"
    if not overlay_path.exists():
        print(f"repo_id={repo_id} status=missing_overlay overlay={overlay_path.as_posix()}")
        print(f"next_action=run `codex-orchestrator overlay apply --repo-id {repo_id}`")
        return 1

    try:
        bd_init(repo_root=repo_policy.path)
        known_bead_ids = bd_list_ids(repo_root=repo_policy.path)
        ready_beads = bd_ready(repo_root=repo_policy.path)
    except BdCliError as e:
        raise SystemExit(f"codex-orchestrator: bd error for repo_id={repo_id!r}: {e}") from e

    try:
        planning = plan_deck_items(
            repo_policy=repo_policy,
            overlay_path=overlay_path,
            ready_beads=ready_beads,
            known_bead_ids=known_bead_ids,
        )
    except ContractOverlayError as e:
        print(str(e).rstrip("\n"))
        print(f"next_action=fix {overlay_path.as_posix()} (then re-run dry-run)")
        return 1

    if planning.skipped_beads:
        print(
            f"repo_id={repo_id} status=missing_contract_fields "
            f"ready={len(ready_beads)} queued={len(planning.deck_items)} "
            f"skipped={len(planning.skipped_beads)}"
        )
        for bead in planning.skipped_beads:
            print(f"bead_id={bead.bead_id} title={bead.title!r}")
            print(f"next_action={bead.next_action}")
        return 1

    print(
        f"repo_id={repo_id} status=ok overlay={overlay_path.as_posix()} "
        f"ready={len(ready_beads)} queued={len(planning.deck_items)}"
    )
    return 0


def _cmd_overlay_apply(args: argparse.Namespace) -> int:
    try:
        inventory = load_repo_inventory(Path("config/repos.toml"))
    except RepoConfigError as e:
        raise SystemExit(f"codex-orchestrator: invalid config/repos.toml: {e}") from e

    repo_id = str(args.repo_id)
    repo_policy = inventory.repos.get(repo_id)
    if repo_policy is None:
        known = ", ".join(sorted(inventory.repos)) or "<none>"
        raise SystemExit(f"codex-orchestrator: unknown repo_id {repo_id!r} (known: {known})")

    overlay_path = Path("config/bead_contracts") / f"{repo_id}.toml"
    overlay_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        bd_init(repo_root=repo_policy.path)
        known_bead_ids = bd_list_ids(repo_root=repo_policy.path)
        ready_beads = bd_ready(repo_root=repo_policy.path)
    except BdCliError as e:
        raise SystemExit(f"codex-orchestrator: bd error for repo_id={repo_id!r}: {e}") from e

    overlay: ContractOverlay
    if overlay_path.exists():
        try:
            overlay = load_contract_overlay(
                overlay_path,
                repo_policy=repo_policy,
                known_bead_ids=known_bead_ids,
            )
        except ContractOverlayError as e:
            raise SystemExit(
                f"codex-orchestrator: cannot apply defaults to invalid overlay {overlay_path}: {e}"
            ) from e
    else:
        overlay = ContractOverlay(repo_id=repo_id, defaults=ContractOverlayPatch(), beads={})

    seed_env = str(args.env).strip() if args.env is not None else None
    env_to_write = overlay.defaults.env or seed_env or repo_policy.env
    if env_to_write is None:
        raise SystemExit(
            "codex-orchestrator: env is required for execution contract resolution. "
            f"Set repos.{repo_id}.env in config/repos.toml, or run "
            f"`codex-orchestrator overlay apply --repo-id {repo_id} --env <ENV_NAME>`"
        )

    time_budget_minutes = int(args.time_budget_minutes)
    if time_budget_minutes <= 0:
        raise SystemExit("codex-orchestrator: --time-budget-minutes must be > 0")

    defaults = overlay.defaults
    if defaults.time_budget_minutes is None:
        defaults = replace(defaults, time_budget_minutes=time_budget_minutes)
    if defaults.env is None:
        defaults = replace(defaults, env=env_to_write)
    if defaults.allow_env_creation is None:
        defaults = replace(defaults, allow_env_creation=bool(args.allow_env_creation))
    if defaults.requires_notebook_execution is None:
        defaults = replace(
            defaults, requires_notebook_execution=bool(args.requires_notebook_execution)
        )

    validation_commands: tuple[str, ...] | None = None
    if defaults.validation_commands is None:
        if args.validation_command:
            validation_commands = tuple(
                c for c in (str(item).strip() for item in args.validation_command) if c
            )
        elif repo_policy.validation_commands:
            validation_commands = repo_policy.validation_commands
        if validation_commands:
            defaults = replace(defaults, validation_commands=validation_commands)

    updated = ContractOverlay(repo_id=overlay.repo_id, defaults=defaults, beads=dict(overlay.beads))
    new_text = _render_contract_overlay_toml(updated)
    old_text = overlay_path.read_text(encoding="utf-8") if overlay_path.exists() else None
    if old_text != new_text:
        overlay_path.write_text(new_text, encoding="utf-8")

    # Confirm that ready beads have resolvable required fields.
    try:
        planning = plan_deck_items(
            repo_policy=repo_policy,
            overlay_path=overlay_path,
            ready_beads=ready_beads,
            known_bead_ids=known_bead_ids,
        )
    except ContractOverlayError as e:
        raise SystemExit(f"codex-orchestrator: wrote invalid overlay {overlay_path}: {e}") from e

    if planning.skipped_beads:
        print(
            f"repo_id={repo_id} status=written_but_incomplete overlay={overlay_path.as_posix()} "
            f"ready={len(ready_beads)} queued={len(planning.deck_items)} skipped={len(planning.skipped_beads)}"
        )
        for bead in planning.skipped_beads:
            print(f"bead_id={bead.bead_id} title={bead.title!r}")
            print(f"next_action={bead.next_action}")
        return 1

    print(
        f"repo_id={repo_id} status=ok overlay={overlay_path.as_posix()} "
        f"ready={len(ready_beads)} queued={len(planning.deck_items)}"
    )
    return 0


def _toml_load_untyped(path: Path) -> dict[str, object]:
    try:
        import tomllib  # pyright: ignore[reportMissingImports]
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib  # type: ignore[no-redef]

    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        return {}
    except OSError as e:
        raise SystemExit(f"codex-orchestrator: failed to read {path}: {e}") from e
    except Exception as e:
        raise SystemExit(f"codex-orchestrator: failed to parse TOML in {path}: {e}") from e

    if not isinstance(data, dict):
        raise SystemExit(f"codex-orchestrator: expected TOML table at top-level in {path}")
    return data


def _toml_table_key(key: str) -> str:
    if _TOML_BARE_KEY_RE.match(key):
        return key
    return _toml_quote_string(key)


def _render_init_repo_entry_toml(
    *,
    repo_id: str,
    repo_path: Path,
    base_branch: str,
    env_name: str,
    validation_commands: tuple[str, ...],
) -> str:
    lines = [
        f"[repos.{_toml_table_key(repo_id)}]",
        f"path = {_toml_quote_string(repo_path.as_posix())}",
        f"base_branch = {_toml_quote_string(base_branch)}",
        f"env = {_toml_quote_string(env_name)}",
        'notebook_roots = ["."]',
        'allowed_roots = ["."]',
        "deny_roots = [",
    ]
    lines.extend(f"  {_toml_quote_string(item)}," for item in _DEFAULT_INIT_DENY_ROOTS)
    lines.append("]")
    lines.append('notebook_output_policy = "strip"')
    if validation_commands:
        lines.append(f"validation_commands = {_toml_quote_str_list(validation_commands)}")
    return "\n".join(lines)


def _ensure_repo_inventory_entry(
    *,
    repo_id: str,
    repo_path: Path,
    base_branch: str,
    env_name: str,
    validation_commands: tuple[str, ...],
    allow_existing: bool,
) -> bool:
    config_path = Path("config/repos.toml")
    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        data = _toml_load_untyped(config_path)
        repos = data.get("repos")
        if not isinstance(repos, dict):
            raise SystemExit(
                "codex-orchestrator: config/repos.toml is missing a [repos] table; "
                "fix it before running init-repo."
            )
        if repo_id in repos:
            if allow_existing:
                return False
            raise SystemExit(
                f"codex-orchestrator: repo_id {repo_id!r} already exists in {config_path}. "
                "Use --allow-existing to keep it and only bootstrap overlay/beads."
            )
        existing = config_path.read_text(encoding="utf-8").rstrip()
        entry = _render_init_repo_entry_toml(
            repo_id=repo_id,
            repo_path=repo_path,
            base_branch=base_branch,
            env_name=env_name,
            validation_commands=validation_commands,
        )
        config_path.write_text(existing + "\n\n" + entry + "\n", encoding="utf-8")
        return True

    header = "\n".join(
        [
            "# Repository inventory for codex-orchestrator.",
            "#",
            "# Each repo is keyed by a stable `repo_id` under `[repos.<repo_id>]`.",
            "# Required fields: `path`, `base_branch`.",
            "# Optional fields: `env`, `notebook_roots`, `allowed_roots`, `deny_roots`,",
            "#                  `validation_commands`, `notebook_output_policy`.",
            "",
        ]
    )
    entry = _render_init_repo_entry_toml(
        repo_id=repo_id,
        repo_path=repo_path,
        base_branch=base_branch,
        env_name=env_name,
        validation_commands=validation_commands,
    )
    config_path.write_text(header + entry + "\n", encoding="utf-8")
    return True


def _resolve_init_repo_path(raw_path: str) -> Path:
    repo_path = Path(raw_path).expanduser()
    if not repo_path.is_absolute():
        repo_path = (Path.cwd() / repo_path).resolve()
    else:
        repo_path = repo_path.resolve()

    if not repo_path.exists():
        raise SystemExit(f"codex-orchestrator: --path does not exist: {repo_path}")
    if not repo_path.is_dir():
        raise SystemExit(f"codex-orchestrator: --path must be a directory: {repo_path}")
    return repo_path


def _resolve_init_base_branch(*, repo_root: Path, raw: str | None) -> str:
    if raw is not None and raw.strip():
        return raw.strip()
    try:
        branch = git_current_branch(repo_root=repo_root).strip()
    except GitError as e:
        raise SystemExit(
            "codex-orchestrator: could not detect base branch from git. "
            f"Pass --base-branch explicitly. ({e})"
        ) from e
    if not branch or branch == "HEAD":
        raise SystemExit(
            "codex-orchestrator: could not infer base branch from current HEAD; "
            "pass --base-branch explicitly."
        )
    return branch


def _restore_repos_config(*, config_path: Path, previous_text: str | None) -> None:
    try:
        if previous_text is None:
            config_path.unlink(missing_ok=True)
            return
        config_path.write_text(previous_text, encoding="utf-8")
    except OSError as e:
        raise SystemExit(f"codex-orchestrator: failed to roll back {config_path}: {e}") from e


def _cmd_init_repo(args: argparse.Namespace) -> int:
    repo_id = str(args.repo_id).strip()
    if not repo_id:
        raise SystemExit("codex-orchestrator: --repo-id must be non-empty")

    env_name = str(args.env).strip()
    if not env_name:
        raise SystemExit("codex-orchestrator: --env must be non-empty")

    repo_path = _resolve_init_repo_path(str(args.path))
    base_branch = _resolve_init_base_branch(repo_root=repo_path, raw=args.base_branch)
    validation_commands = tuple(
        c for c in (str(item).strip() for item in (args.validation_command or ())) if c
    )
    config_path = Path("config/repos.toml")
    previous_config_text = (
        config_path.read_text(encoding="utf-8") if config_path.exists() else None
    )

    config_written = _ensure_repo_inventory_entry(
        repo_id=repo_id,
        repo_path=repo_path,
        base_branch=base_branch,
        env_name=env_name,
        validation_commands=validation_commands,
        allow_existing=bool(args.allow_existing),
    )

    overlay_args = argparse.Namespace(
        repo_id=repo_id,
        time_budget_minutes=int(args.time_budget_minutes),
        env=env_name,
        allow_env_creation=bool(args.allow_env_creation),
        requires_notebook_execution=bool(args.requires_notebook_execution),
        validation_command=list(validation_commands) or None,
    )
    try:
        overlay_rc = _cmd_overlay_apply(overlay_args)
    except BaseException:
        if config_written:
            _restore_repos_config(config_path=config_path, previous_text=previous_config_text)
            print(
                f"repo_id={repo_id} status=rolled_back repos_config={config_path.as_posix()} "
                "reason=overlay_apply_exception"
            )
        raise

    if overlay_rc != 0 and config_written:
        _restore_repos_config(config_path=config_path, previous_text=previous_config_text)
        print(
            f"repo_id={repo_id} status=rolled_back repos_config={config_path.as_posix()} "
            "reason=overlay_apply_nonzero"
        )
        return overlay_rc

    status = "config_written" if config_written else "config_exists"
    print(
        f"repo_id={repo_id} status={status} repos_config={config_path.as_posix()} "
        f"path={repo_path.as_posix()} base_branch={base_branch} env={env_name}"
    )
    return overlay_rc


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
    tick_parser.add_argument(
        "--final-review-codex",
        action="store_true",
        help="After ending a run, optionally run a review-only Codex pass (must produce zero diffs).",
    )
    tick_parser.set_defaults(func=_cmd_tick)

    run_parser = subparsers.add_parser("run", help="Execute one orchestrator cycle (all repos).")
    run_parser.add_argument(
        "--mode",
        choices=("automated", "manual"),
        default="automated",
        help="Run mode (scheduler=automated, roadtrip=manual).",
    )
    run_parser.add_argument("--cache-dir", default=None, help="Override orchestrator cache directory.")
    run_parser.add_argument(
        "--repo-id",
        action="append",
        default=None,
        help="Restrict to a specific repo_id from config/repos.toml (repeatable).",
    )
    run_parser.add_argument(
        "--repo-group",
        action="append",
        default=None,
        help="Restrict to a repo_group from config/repos.toml (repeatable).",
    )
    run_parser.add_argument(
        "--max-parallel",
        type=int,
        default=None,
        help="Max repos to run in parallel (defaults to $MAX_PARALLEL or auto).",
    )
    run_parser.add_argument("--tick-minutes", type=float, default=45.0, help="Tick budget in minutes.")
    run_parser.add_argument(
        "--idle-ticks-to-end",
        type=int,
        default=3,
        help="End the run after N consecutive idle ticks.",
    )
    run_parser.add_argument(
        "--manual-ttl-hours",
        type=float,
        default=12.0,
        help="Expiry TTL for manual runs (hours).",
    )
    run_parser.add_argument(
        "--min-minutes-to-start-new-bead",
        type=int,
        default=15,
        help="Do not start new beads if less than this remains.",
    )
    run_parser.add_argument(
        "--max-beads-per-tick",
        type=int,
        default=3,
        help="Cap beads attempted per repo per tick.",
    )
    run_parser.add_argument(
        "--review-every-beads",
        type=int,
        default=None,
        help="Run a review-only Codex pass after N beads are attempted (does not end the run).",
    )
    run_parser.add_argument(
        "--diff-cap-files",
        type=int,
        default=25,
        help="Per-tick max files changed (sum across beads).",
    )
    run_parser.add_argument(
        "--diff-cap-lines",
        type=int,
        default=1500,
        help="Per-tick max lines added (sum across beads).",
    )
    run_parser.add_argument(
        "--replan",
        action="store_true",
        help="Recompute each repo run deck even if one already exists for this RUN_ID+repo_id.",
    )
    run_parser.add_argument(
        "--final-review-codex",
        action="store_true",
        help="After ending a run, optionally run a review-only Codex pass (must produce zero diffs).",
    )
    run_parser.add_argument(
        "--focus",
        default=None,
        help="Natural language focus area for the run (filters planned beads and guides Codex execution).",
    )
    run_parser.set_defaults(func=_cmd_run)

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
    exec_repo_parser.add_argument(
        "--replan",
        action="store_true",
        help="Recompute the run deck even if one already exists for this RUN_ID+repo_id.",
    )
    exec_repo_parser.add_argument(
        "--focus",
        default=None,
        help="Natural language focus area for the run (filters planned beads and guides Codex execution).",
    )
    exec_repo_parser.set_defaults(func=_cmd_exec_repo)

    init_repo_parser = subparsers.add_parser(
        "init-repo",
        help="Initialize a new repository so it can participate in orchestrator runs.",
    )
    init_repo_parser.add_argument("--repo-id", required=True, help="Repo ID to create in config/repos.toml")
    init_repo_parser.add_argument(
        "--path",
        required=True,
        help="Path to the target repository (absolute or relative to current working directory).",
    )
    init_repo_parser.add_argument(
        "--env",
        required=True,
        help="Default conda env name for this repo (written to repos.toml and overlay defaults).",
    )
    init_repo_parser.add_argument(
        "--base-branch",
        default=None,
        help="Base branch for run/<RUN_ID> branches (defaults to current git branch).",
    )
    init_repo_parser.add_argument(
        "--validation-command",
        action="append",
        default=None,
        help="Validation command to add (repeatable).",
    )
    init_repo_parser.add_argument(
        "--time-budget-minutes",
        type=int,
        default=45,
        help="Default per-bead time budget (minutes).",
    )
    init_repo_parser.add_argument(
        "--allow-env-creation",
        action="store_true",
        help="Write allow_env_creation=true in overlay defaults when missing.",
    )
    init_repo_parser.add_argument(
        "--requires-notebook-execution",
        action="store_true",
        help="Write requires_notebook_execution=true in overlay defaults when missing.",
    )
    init_repo_parser.add_argument(
        "--allow-existing",
        action="store_true",
        help="Keep an existing repos.<repo_id> entry and only bootstrap overlay/beads.",
    )
    init_repo_parser.set_defaults(func=_cmd_init_repo)

    signoff_parser = subparsers.add_parser(
        "signoff",
        help="Create run signoff artifacts (run_signoff.json/md) for an ended run.",
    )
    signoff_parser.add_argument(
        "--run-id",
        default=None,
        help="RUN_ID to sign off (defaults to latest ended run).",
    )
    signoff_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    signoff_parser.add_argument(
        "--reviewer",
        default=None,
        help="Reviewer identity (required; or set $CODEX_ORCHESTRATOR_REVIEWER).",
    )
    signoff_parser.add_argument(
        "--notes",
        default=None,
        help="Optional notes to include in run_signoff.md.",
    )
    signoff_parser.set_defaults(func=_cmd_signoff)

    overlay_parser = subparsers.add_parser("overlay", help="Validate/generate contract overlays.")

    def _cmd_overlay_help(_: argparse.Namespace) -> int:
        overlay_parser.print_help()
        return 0

    overlay_parser.set_defaults(func=_cmd_overlay_help)

    overlay_subparsers = overlay_parser.add_subparsers(dest="overlay_command")

    overlay_dry_run = overlay_subparsers.add_parser(
        "dry-run",
        help="Validate overlay + report missing contract fields for ready beads.",
    )
    overlay_dry_run.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    overlay_dry_run.set_defaults(func=_cmd_overlay_dry_run)

    overlay_apply = overlay_subparsers.add_parser(
        "apply",
        help="Create/update config/bead_contracts/<repo_id>.toml with safe defaults.",
    )
    overlay_apply.add_argument("--repo-id", required=True, help="Repo ID from config/repos.toml")
    overlay_apply.add_argument(
        "--time-budget-minutes",
        type=int,
        default=45,
        help="Default per-bead time budget (minutes).",
    )
    overlay_apply.add_argument(
        "--env",
        default=None,
        help="Override default env written to the overlay (defaults to repos.<repo_id>.env).",
    )
    overlay_apply.add_argument(
        "--allow-env-creation",
        action="store_true",
        help="Set allow_env_creation=true in [defaults] if missing.",
    )
    overlay_apply.add_argument(
        "--requires-notebook-execution",
        action="store_true",
        help="Set requires_notebook_execution=true in [defaults] if missing.",
    )
    overlay_apply.add_argument(
        "--validation-command",
        action="append",
        default=None,
        help="Set defaults.validation_commands if missing (repeatable).",
    )
    overlay_apply.set_defaults(func=_cmd_overlay_apply)

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

    run_info_parser = subparsers.add_parser(
        "run-info",
        help="List recent runs or inspect one run's debugging artifacts.",
    )
    run_info_parser.add_argument(
        "--run-id",
        default=None,
        help="Inspect a specific RUN_ID (if omitted, lists recent runs).",
    )
    run_info_parser.add_argument(
        "--latest",
        action="store_true",
        help="Inspect the most recent RUN_ID under cache/runs.",
    )
    run_info_parser.add_argument(
        "--repo-id",
        action="append",
        default=None,
        help="Filter inspected repo summaries by repo_id (repeatable; requires --run-id/--latest).",
    )
    run_info_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum runs to list when no --run-id/--latest is provided.",
    )
    run_info_parser.add_argument(
        "--tail-lines",
        type=int,
        default=0,
        help="Include trailing log lines for run/repo logs (requires --run-id/--latest).",
    )
    run_info_parser.add_argument(
        "--cache-dir",
        default=None,
        help="Override orchestrator cache directory.",
    )
    run_info_parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON output.",
    )
    run_info_parser.set_defaults(func=_cmd_run_info)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
