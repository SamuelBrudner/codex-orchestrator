from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codex_orchestrator.ai_policy import AiSettings, codex_cli_args_for_settings
from codex_orchestrator.audit_trail import write_json_atomic
from codex_orchestrator.codex_subprocess import codex_exec_full_auto
from codex_orchestrator.git_subprocess import (
    GitError,
    git_is_dirty,
    git_rev_parse,
    git_status_filtered,
    resolve_dirty_ignore_globs,
)
from codex_orchestrator.repo_inventory import RepoConfigError, load_repo_inventory
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import PlannerError, read_run_deck


class RunClosureReviewError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RunClosureArtifacts:
    json_path: Path
    md_path: Path


@dataclass(frozen=True, slots=True)
class CodexReviewLog:
    repo_id: str
    path: Path


def _read_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        raise RunClosureReviewError(f"Failed to parse JSON in {path}: {e}") from e
    except OSError as e:
        raise RunClosureReviewError(f"Failed to read {path}: {e}") from e


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as f:
        f.write(content)
        if not content.endswith("\n"):
            f.write("\n")
        tmp_name = f.name
    os.replace(tmp_name, path)


def _load_repo_summaries(paths: OrchestratorPaths, *, run_id: str) -> list[dict[str, Any]]:
    run_dir = paths.run_dir(run_id)
    summaries: list[dict[str, Any]] = []
    for summary_path in sorted(run_dir.glob("*.summary.json")):
        payload = _read_json(summary_path)
        if isinstance(payload, dict):
            summaries.append(payload)
    summaries.sort(key=lambda s: str(s.get("repo_id") or ""))
    return summaries


def _ensure_run_summary_with_final_review(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    final_review_json: Path,
    final_review_md: Path,
) -> None:
    run_summary_path = paths.run_summary_path(run_id)
    payload = _read_json(run_summary_path)
    if not isinstance(payload, dict):
        payload = {"schema_version": 1, "run_id": run_id, "repos": _load_repo_summaries(paths, run_id=run_id)}
    payload.setdefault("schema_version", 1)
    payload.setdefault("run_id", run_id)
    payload.setdefault("repos", _load_repo_summaries(paths, run_id=run_id))
    payload["final_review"] = {
        "json_path": final_review_json.name,
        "md_path": final_review_md.name,
    }
    write_json_atomic(run_summary_path, payload)


def build_final_review(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    ai_settings: AiSettings | None = None,
) -> dict[str, Any]:
    run_end_path = paths.run_end_path(run_id)
    run_end = _read_json(run_end_path)
    if not isinstance(run_end, dict):
        raise RunClosureReviewError(f"Missing run end marker: {run_end_path}")

    run_meta = _read_json(paths.run_metadata_path(run_id))
    if run_meta is not None and not isinstance(run_meta, dict):
        run_meta = None

    repo_summaries = _load_repo_summaries(paths, run_id=run_id)

    repos_out: list[dict[str, Any]] = []
    total_attempted = 0
    total_closed = 0
    for summary in repo_summaries:
        repo_id = str(summary.get("repo_id") or "")
        deck_path_raw = summary.get("deck_path")
        deck_path = Path(deck_path_raw) if isinstance(deck_path_raw, str) and deck_path_raw else None

        planned_bead_ids: list[str] | None = None
        if deck_path is not None:
            try:
                deck = read_run_deck(deck_path)
            except (PlannerError, OSError):
                deck = None
            if deck is not None:
                planned_bead_ids = [item.bead_id for item in deck.items]

        beads_raw = summary.get("beads", [])
        if beads_raw is None:
            beads_raw = []

        outcomes: dict[str, int] = {}
        audits_by_id: dict[str, dict[str, Any]] = {}
        audit_allowed_keys = {
            "bead_id",
            "title",
            "outcome",
            "detail",
            "commit_hash",
            "changed_paths",
            "validation",
            "dependents_updated",
        }
        if isinstance(beads_raw, list):
            for item in beads_raw:
                if not isinstance(item, dict):
                    continue
                bead_id = item.get("bead_id")
                if not isinstance(bead_id, str) or not bead_id.strip():
                    continue

                outcome = item.get("outcome")
                if isinstance(outcome, str) and outcome.strip():
                    outcomes[outcome] = outcomes.get(outcome, 0) + 1

                filtered: dict[str, Any] = {}
                for k in audit_allowed_keys:
                    if k in item:
                        filtered[k] = item.get(k)
                audits_by_id[bead_id] = filtered

        attempted = int(summary.get("beads_attempted", 0) or 0)
        closed = int(summary.get("beads_closed", 0) or 0)
        total_attempted += attempted
        total_closed += closed

        audits_out: list[dict[str, Any]]
        if planned_bead_ids is not None:
            audits_out = [audits_by_id.get(bead_id, {"bead_id": bead_id}) for bead_id in planned_bead_ids]
        else:
            audits_out = [audits_by_id[k] for k in sorted(audits_by_id)]

        repos_out.append(
            {
                "repo_id": repo_id,
                "repo_path": summary.get("repo_path"),
                "branch": summary.get("branch"),
                "skipped": bool(summary.get("skipped")),
                "skip_reason": summary.get("skip_reason"),
                "stop_reason": summary.get("stop_reason"),
                "next_action": summary.get("next_action"),
                "deck": {
                    "path": deck_path.as_posix() if deck_path is not None else None,
                    "reused_existing_deck": summary.get("reused_existing_deck"),
                    "planned_bead_ids": planned_bead_ids,
                },
                "beads_attempted": attempted,
                "beads_closed": closed,
                "bead_outcomes": {k: outcomes[k] for k in sorted(outcomes)},
                "beads": audits_out,
                "planning_skipped_beads": summary.get("planning_skipped_beads", []),
                "run_report_path": summary.get("run_report_path"),
                "ai_settings": summary.get("ai_settings"),
                "codex_command": summary.get("codex_command"),
                "codex_argv": summary.get("codex_argv"),
                "tool_versions": summary.get("tool_versions"),
                "notes": {
                    "new_beads_created": "unknown",
                    "new_beads_policy": (
                        "New beads created during the run are not currently detected from summaries alone. "
                        "Conservative replanning policy: newly-created beads are not added to an existing run deck."
                    ),
                },
            }
        )

    repos_out.sort(key=lambda r: str(r.get("repo_id") or ""))

    return {
        "schema_version": 1,
        "run_id": run_id,
        "run": {
            "metadata": run_meta,
            "end": run_end,
            "ai_settings": ai_settings.to_json_dict() if ai_settings is not None else None,
        },
        "summary": {
            "repos_total": len(repos_out),
            "beads_attempted_total": total_attempted,
            "beads_closed_total": total_closed,
        },
        "repos": repos_out,
    }


def format_final_review_md(review: Mapping[str, Any]) -> str:
    run_id = str(review.get("run_id") or "")
    run = review.get("run")
    run_end: Mapping[str, Any] | None = None
    if isinstance(run, dict):
        end = run.get("end")
        if isinstance(end, dict):
            run_end = end

    ended_at = run_end.get("ended_at") if run_end is not None else None
    reason = run_end.get("reason") if run_end is not None else None

    lines: list[str] = []
    lines.append(f"# Final Review (RUN_ID={run_id})")
    lines.append("")
    lines.append("## Run End")
    lines.append(f"- Reason: {reason if isinstance(reason, str) and reason else '<unknown>'}")
    lines.append(f"- Ended at: {ended_at if isinstance(ended_at, str) and ended_at else '<unknown>'}")
    lines.append("")

    summary = review.get("summary")
    if isinstance(summary, dict):
        repos_total = summary.get("repos_total")
        beads_attempted = summary.get("beads_attempted_total")
        beads_closed = summary.get("beads_closed_total")
        lines.append("## Totals")
        lines.append(f"- Repos: {repos_total}")
        lines.append(f"- Beads attempted: {beads_attempted}")
        lines.append(f"- Beads closed: {beads_closed}")
        lines.append("")

    repos = review.get("repos")
    if isinstance(repos, list) and repos:
        lines.append("## Repos")
        for repo in repos:
            if not isinstance(repo, dict):
                continue
            repo_id = repo.get("repo_id", "<unknown>")
            skipped = bool(repo.get("skipped"))
            skip_reason = repo.get("skip_reason")
            stop_reason = repo.get("stop_reason")
            next_action = repo.get("next_action")
            attempted = repo.get("beads_attempted", 0)
            closed = repo.get("beads_closed", 0)
            status = f"skipped:{skip_reason}" if skipped else f"stop:{stop_reason}"
            lines.append(f"### {repo_id}")
            lines.append(f"- Status: {status}")
            lines.append(f"- Attempted: {attempted}  Closed: {closed}")

            deck = repo.get("deck")
            if isinstance(deck, dict):
                deck_path = deck.get("path")
                reused = deck.get("reused_existing_deck")
                planned = deck.get("planned_bead_ids")
                lines.append(f"- Deck: {deck_path}  reused_existing_deck={reused}")
                if isinstance(planned, list):
                    planned_str = ", ".join(str(b) for b in planned) if planned else "<none>"
                    lines.append(f"- Planned beads: {planned_str}")

            outcomes = repo.get("bead_outcomes")
            if isinstance(outcomes, dict) and outcomes:
                parts = [f"{k}={outcomes[k]}" for k in sorted(outcomes)]
                lines.append(f"- Bead outcomes: {' '.join(parts)}")

            if isinstance(next_action, str) and next_action.strip():
                lines.append(f"- Next action: {next_action}")
            lines.append("")

    lines.append("## Policy Notes")
    lines.append(
        "- New beads created during the run: unknown (not detected from summaries alone); "
        "conservative replanning keeps run decks frozen."
    )
    return "\n".join(lines).rstrip("\n") + "\n"


def write_final_review(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    ai_settings: AiSettings | None = None,
) -> RunClosureArtifacts:
    json_path = paths.final_review_json_path(run_id)
    md_path = paths.final_review_md_path(run_id)

    if json_path.exists() and md_path.exists():
        _ensure_run_summary_with_final_review(
            paths,
            run_id=run_id,
            final_review_json=json_path,
            final_review_md=md_path,
        )
        return RunClosureArtifacts(json_path=json_path, md_path=md_path)

    review = build_final_review(paths, run_id=run_id, ai_settings=ai_settings)
    if not json_path.exists():
        write_json_atomic(json_path, review)
    if not md_path.exists():
        _write_text_atomic(md_path, format_final_review_md(review))

    _ensure_run_summary_with_final_review(
        paths,
        run_id=run_id,
        final_review_json=json_path,
        final_review_md=md_path,
    )
    return RunClosureArtifacts(json_path=json_path, md_path=md_path)


def _review_only_prompt(*, run_id: str, repo_id: str, label: str | None = None) -> str:
    if label:
        intro = f"You are running a {label} review-only pass."
    else:
        intro = "You are running a review-only pass."
    return "\n".join(
        [
            intro,
            "",
            "Hard constraints:",
            "- Do NOT modify, create, or delete any files.",
            "- Do NOT run shell commands.",
            "- Output a concise review summary to stdout only.",
            "",
            f"Context: RUN_ID={run_id} repo_id={repo_id}",
            "",
            "If docs/runs/<RUN_ID>.md exists in this repo, use it as the primary source. Otherwise, summarize from git history on the current branch.",
            "",
            "Deliverables:",
            "- What was attempted / completed",
            "- Any failures or skips (and why)",
            "- Suggested follow-ups (as bullets)",
        ]
    ).rstrip("\n")


def _load_dirty_ignore_globs(
    repo_config_path: Path | None,
) -> dict[str, tuple[str, ...]]:
    if repo_config_path is None or not repo_config_path.exists():
        return {}
    try:
        inventory = load_repo_inventory(repo_config_path)
    except RepoConfigError as e:
        raise RunClosureReviewError(f"Failed to load repo config for review: {e}") from e
    return {policy.repo_id: policy.dirty_ignore_globs for policy in inventory.list_repos()}


def run_review_only_codex_pass(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    ai_settings: AiSettings,
    timeout_seconds: float = 900.0,
    repo_config_path: Path | None = None,
    log_stem: str = "final_codex_review",
    log_suffix: str | None = None,
    prompt_label: str | None = None,
) -> tuple[CodexReviewLog, ...]:
    run_dir = paths.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    logs: list[CodexReviewLog] = []
    dirty_ignore_by_repo = _load_dirty_ignore_globs(repo_config_path)
    for summary in _load_repo_summaries(paths, run_id=run_id):
        repo_id = str(summary.get("repo_id") or "")
        if not repo_id:
            continue
        if bool(summary.get("skipped")):
            continue
        repo_path = summary.get("repo_path")
        if not isinstance(repo_path, str) or not repo_path.strip():
            continue

        repo_root = Path(repo_path)
        try:
            head_before = git_rev_parse(repo_root=repo_root)
        except GitError as e:
            raise RunClosureReviewError(f"Final Codex review failed for {repo_id}: {e}") from e
        configured_globs = dirty_ignore_by_repo.get(repo_id, ())
        ignore_globs = resolve_dirty_ignore_globs(
            repo_root=repo_root,
            configured=configured_globs,
        ).resolved
        if git_is_dirty(repo_root=repo_root, ignore_globs=ignore_globs):
            status = git_status_filtered(repo_root=repo_root, ignore_globs=ignore_globs)
            changed = ", ".join(sorted({e.path for e in status if e.path})) or "<unknown>"
            raise RunClosureReviewError(
                f"Final Codex review refused for {repo_id}: repo is dirty before review ({changed})."
            )

        prompt = _review_only_prompt(run_id=run_id, repo_id=repo_id, label=prompt_label)
        invocation = codex_exec_full_auto(
            prompt=prompt,
            cwd=repo_root,
            timeout_seconds=timeout_seconds,
            extra_args=codex_cli_args_for_settings(ai_settings),
        )

        suffix = f".{log_suffix}" if log_suffix else ""
        log_path = run_dir / f"{log_stem}.{repo_id}{suffix}.json"
        write_json_atomic(
            log_path,
            {
                "schema_version": 1,
                "run_id": run_id,
                "repo_id": repo_id,
                "repo_path": repo_root.as_posix(),
                "codex_argv": list(invocation.args),
                "exit_code": invocation.exit_code,
                "stdout": invocation.stdout,
                "stderr": invocation.stderr,
                "started_at": invocation.started_at.isoformat(),
                "finished_at": invocation.finished_at.isoformat(),
            },
        )
        logs.append(CodexReviewLog(repo_id=repo_id, path=log_path))

        if invocation.exit_code != 0:
            raise RunClosureReviewError(
                f"Final Codex review failed for {repo_id}: codex exit={invocation.exit_code} "
                f"(see {log_path})."
            )

        try:
            head_after = git_rev_parse(repo_root=repo_root)
        except GitError as e:
            raise RunClosureReviewError(f"Final Codex review failed for {repo_id}: {e}") from e
        if head_after != head_before:
            raise RunClosureReviewError(
                f"Policy violation: final Codex review created commits for {repo_id} "
                f"(head {head_before[:12]} -> {head_after[:12]})."
            )

        if git_is_dirty(repo_root=repo_root, ignore_globs=ignore_globs):
            status = git_status_filtered(repo_root=repo_root, ignore_globs=ignore_globs)
            changed = ", ".join(sorted({e.path for e in status if e.path})) or "<unknown>"
            raise RunClosureReviewError(
                f"Policy violation: final Codex review produced a diff for {repo_id} ({changed})."
            )

    return tuple(logs)
