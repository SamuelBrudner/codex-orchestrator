from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from codex_orchestrator.cli_common import format_bool as _format_bool
from codex_orchestrator.paths import OrchestratorPaths, default_cache_dir
from codex_orchestrator.run_artifacts import (
    artifact_payload,
    list_run_ids,
    load_repo_summaries_for_run,
    read_json_object,
    tail_text,
)
from codex_orchestrator.run_lifecycle import RunLifecycleError, recover_orphaned_current_run
from codex_orchestrator.summary_utils import summary_int


def _as_int(value: Any) -> int:
    return summary_int(value)


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


def _maybe_load_current_run_id(paths: OrchestratorPaths) -> str | None:
    try:
        recover_orphaned_current_run(paths=paths)
    except RunLifecycleError as e:
        raise SystemExit(f"codex-orchestrator: failed orphaned-run recovery: {e}") from e

    payload = read_json_object(paths.current_run_path)
    if payload is None:
        return None
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return None
    return run_id


def _build_run_overview(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    current_run_id: str | None,
) -> dict[str, Any]:
    metadata = read_json_object(paths.run_metadata_path(run_id))
    run_end = read_json_object(paths.run_end_path(run_id))
    repos = load_repo_summaries_for_run(paths, run_id=run_id)

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
            "summary": artifact_payload(paths.repo_summary_path(run_id, repo_id)),
            "exec_log": artifact_payload(paths.repo_exec_log_path(run_id, repo_id)),
            "stdout_log": artifact_payload(paths.repo_stdout_log_path(run_id, repo_id)),
            "stderr_log": artifact_payload(paths.repo_stderr_log_path(run_id, repo_id)),
            "events": artifact_payload(paths.repo_events_path(run_id, repo_id)),
        },
    }

    if tail_lines > 0:
        detail["log_tails"] = {
            "exec_log": tail_text(paths.repo_exec_log_path(run_id, repo_id), lines=tail_lines),
            "stderr_log": tail_text(paths.repo_stderr_log_path(run_id, repo_id), lines=tail_lines),
            "events": tail_text(paths.repo_events_path(run_id, repo_id), lines=tail_lines),
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
    metadata = read_json_object(paths.run_metadata_path(run_id))
    run_end = read_json_object(paths.run_end_path(run_id))

    summaries = load_repo_summaries_for_run(paths, run_id=run_id)
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
            "run_json": artifact_payload(paths.run_metadata_path(run_id)),
            "run_end": artifact_payload(paths.run_end_path(run_id)),
            "run_summary": artifact_payload(paths.run_summary_path(run_id)),
            "run_log": artifact_payload(paths.run_log_path(run_id)),
            "final_review_json": artifact_payload(paths.final_review_json_path(run_id)),
            "final_review_md": artifact_payload(paths.final_review_md_path(run_id)),
            "run_signoff_json": artifact_payload(paths.run_signoff_json_path(run_id)),
            "run_signoff_md": artifact_payload(paths.run_signoff_md_path(run_id)),
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
        payload["run_log_tail"] = tail_text(paths.run_log_path(run_id), lines=tail_lines)

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


def cmd_run_info(args: argparse.Namespace) -> int:
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
        run_ids = list_run_ids(paths)
        if not run_ids:
            raise SystemExit(f"codex-orchestrator: no runs found under {paths.runs_dir}")
        requested_run_id = run_ids[0]

    if requested_run_id is None:
        if args.repo_id:
            raise SystemExit("codex-orchestrator: --repo-id requires --run-id or --latest")
        if int(args.tail_lines) > 0:
            raise SystemExit("codex-orchestrator: --tail-lines requires --run-id or --latest")
        run_ids = list_run_ids(paths)[: int(args.limit)]
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
