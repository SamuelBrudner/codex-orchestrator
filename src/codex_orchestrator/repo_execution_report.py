from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from codex_orchestrator.ai_policy import codex_cli_args_for_settings
from codex_orchestrator.audit_trail import (
    format_repo_run_report_md,
    write_json_atomic,
    write_repo_run_report,
)
from codex_orchestrator.git_subprocess import GitError, validate_paths_within_policy


@dataclass(frozen=True, slots=True)
class RepoRunArtifacts:
    run_log_path: Path
    exec_log_path: Path
    stdout_log_path: Path
    stderr_log_path: Path
    events_path: Path
    summary_path: Path
    planning_audit_json_path: Path
    planning_audit_md_path: Path


@dataclass(slots=True)
class RepoExecutionState:
    bead_audits: list[dict[str, Any]] = field(default_factory=list)
    planning_skipped: list[dict[str, str]] = field(default_factory=list)
    validation_status_by_command: dict[str, str] = field(default_factory=dict)
    notebooks_touched: set[str] = field(default_factory=set)
    extracted_code_touched: set[str] = field(default_factory=set)
    repo_failures: list[str] = field(default_factory=list)
    follow_ups: list[str] = field(default_factory=list)
    prompt_records: list[dict[str, object]] = field(default_factory=list)
    run_report_path: Path | None = None
    run_report_committed: bool = False
    deck_path: Path | None = None
    reused_existing_deck: bool | None = None
    planned_scope: list[dict[str, str]] = field(default_factory=list)
    last_dependency_signature: tuple[tuple[str, int, int], ...] | None = None
    failure_snapshot_committed: bool = False
    beads_attempted: int = 0
    beads_closed: int = 0
    stop_reason: str | None = None


def _infer_next_action(
    *,
    skipped: bool,
    skip_reason: str | None,
    stop_reason: str | None,
    bead_audits: list[dict[str, Any]],
) -> str:
    if skipped:
        return {
            "missing_tools": "Install required tools (git, bd, codex) and re-run.",
            "git_dirty": "Clean/stash the repo working tree (or set dirty_ignore_globs) and re-run.",
            "git_detached": "Checkout a branch (not detached HEAD) and re-run.",
            "git_fetch_failed": "Resolve git fetch failure (remotes/network) and re-run.",
            "git_branch_failed": "Resolve git branch setup failure and re-run.",
            "planning_failed": "Inspect planning error in exec log; fix and re-run.",
            "lock_busy": "Another repo tick is running; wait and retry.",
        }.get(skip_reason or "planning_failed", "Inspect logs and re-run.")

    last_failed = next((bead for bead in reversed(bead_audits) if bead.get("outcome") == "failed"), None)
    if last_failed is not None:
        detail = str(last_failed.get("detail") or "").strip()
        if "Baseline failing validations" in detail:
            return "Fix baseline validation failures and re-run."
        if "Validation failed" in detail:
            return "Fix failing validation(s) and re-run."
        if "No behavioral test" in detail:
            return "Add/enable a behavioral test command in validation_commands and re-run."
        if "Given/When/Then" in detail:
            return "Add Given/When/Then markers to modified tests and re-run."
        if "Diff cap exceeded" in detail:
            return "Reduce scope or raise diff caps and re-run."
        if "Safety boundary violation" in detail:
            return "Adjust allowed_roots/deny_roots or reduce scope and re-run."
        return "Inspect failing bead details in logs; fix and re-run."

    if stop_reason == "tick_time_remaining":
        return "Increase tick budget or lower min_minutes_to_start_new_bead and re-run."
    if stop_reason == "bead_cap":
        return "Re-run to continue remaining beads (or raise max_beads_per_tick)."
    if stop_reason == "completed":
        return "Review changes and open PR(s)."
    if stop_reason == "error":
        return "Inspect exec log for error and re-run."
    if stop_reason == "blocked":
        return "Resolve blocker and re-run."
    return "Inspect logs."


def _write_run_summary(paths: Any, *, run_id: str) -> None:
    run_dir = paths.run_dir(run_id)
    summaries: list[dict[str, Any]] = []
    for summary_path in sorted(run_dir.glob("*.summary.json")):
        try:
            data = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, dict):
            summaries.append(data)
    write_json_atomic(
        paths.run_summary_path(run_id),
        {"schema_version": 1, "run_id": run_id, "repos": summaries},
    )


def _load_json_object(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _summary_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _summary_list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _summary_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if text:
            out.append(text)
    return out


def _merge_unique_strings(*lists: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for values in lists:
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            merged.append(value)
    return merged


def _merge_records_by_keys(
    existing: list[dict[str, Any]],
    current: list[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index_by_key: dict[tuple[str, str], int] = {}

    def _record_key(item: Mapping[str, Any]) -> tuple[str, str] | None:
        for key_field in key_fields:
            value = item.get(key_field)
            if isinstance(value, str) and value.strip():
                return (key_field, value.strip())
        return None

    for source in (existing, current):
        for item in source:
            record = dict(item)
            record_key = _record_key(record)
            if record_key is None:
                merged.append(record)
                continue
            idx = index_by_key.get(record_key)
            if idx is None:
                index_by_key[record_key] = len(merged)
                merged.append(record)
                continue
            merged[idx].update(record)
    return merged


def _is_skipped_bead_outcome(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("skipped")


def _merge_bead_audits(existing: list[dict[str, Any]], current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index_by_bead_id: dict[str, int] = {}

    for item in existing:
        record = dict(item)
        bead_id = str(record.get("bead_id") or "").strip()
        if not bead_id:
            merged.append(record)
            continue
        index_by_bead_id[bead_id] = len(merged)
        merged.append(record)

    for item in current:
        record = dict(item)
        bead_id = str(record.get("bead_id") or "").strip()
        if not bead_id:
            merged.append(record)
            continue

        idx = index_by_bead_id.get(bead_id)
        if idx is None:
            index_by_bead_id[bead_id] = len(merged)
            merged.append(record)
            continue

        prior = dict(merged[idx])
        merged_record = dict(prior)
        merged_record.update(record)

        prior_outcome = prior.get("outcome")
        current_outcome = record.get("outcome")
        if _is_skipped_bead_outcome(current_outcome) and isinstance(prior_outcome, str):
            if prior_outcome and not _is_skipped_bead_outcome(prior_outcome):
                merged_record["outcome"] = prior_outcome
                if "detail" in prior:
                    merged_record["detail"] = prior.get("detail")
                else:
                    merged_record.pop("detail", None)

        merged[idx] = merged_record

    return merged


def _merge_notebook_refactors(existing: Any, current: Any) -> dict[str, list[str]]:
    existing_map = existing if isinstance(existing, dict) else {}
    current_map = current if isinstance(current, dict) else {}
    return {
        "notebooks": _merge_unique_strings(
            _summary_string_list(existing_map.get("notebooks")),
            _summary_string_list(current_map.get("notebooks")),
        ),
        "extracted_code": _merge_unique_strings(
            _summary_string_list(existing_map.get("extracted_code")),
            _summary_string_list(current_map.get("extracted_code")),
        ),
    }


def _merge_high_level_context(
    existing: Any,
    current: Any,
    *,
    planning_skipped: list[dict[str, Any]],
) -> dict[str, Any] | None:
    existing_map = existing if isinstance(existing, dict) else {}
    current_map = current if isinstance(current, dict) else {}

    focus = current_map.get("focus")
    if not isinstance(focus, str) or not focus.strip():
        focus = existing_map.get("focus")
    if not isinstance(focus, str) or not focus.strip():
        focus = None

    safety = current_map.get("safety")
    if not isinstance(safety, dict):
        safety = existing_map.get("safety") if isinstance(existing_map.get("safety"), dict) else None

    replan_requested = current_map.get("replan_requested")
    if not isinstance(replan_requested, bool):
        replan_requested = (
            existing_map.get("replan_requested")
            if isinstance(existing_map.get("replan_requested"), bool)
            else None
        )

    reused_existing_deck = current_map.get("reused_existing_deck")
    if not isinstance(reused_existing_deck, bool):
        reused_existing_deck = (
            existing_map.get("reused_existing_deck")
            if isinstance(existing_map.get("reused_existing_deck"), bool)
            else None
        )

    planned_beads = _merge_records_by_keys(
        _summary_list_of_dicts(existing_map.get("planned_beads")),
        _summary_list_of_dicts(current_map.get("planned_beads")),
        key_fields=("bead_id",),
    )

    if (
        focus is None
        and not planned_beads
        and safety is None
        and replan_requested is None
        and reused_existing_deck is None
        and not planning_skipped
    ):
        return None

    return {
        "focus": focus,
        "planned_beads": planned_beads,
        "replan_requested": replan_requested,
        "reused_existing_deck": reused_existing_deck,
        "planning_skipped_count": len(planning_skipped),
        "safety": safety,
    }


def _merge_repo_summary(existing: dict[str, Any] | None, current: dict[str, Any]) -> dict[str, Any]:
    if existing is None:
        return current

    merged_beads = _merge_bead_audits(
        _summary_list_of_dicts(existing.get("beads")),
        _summary_list_of_dicts(current.get("beads")),
    )
    merged_planning_skipped = _merge_records_by_keys(
        _summary_list_of_dicts(existing.get("planning_skipped_beads")),
        _summary_list_of_dicts(current.get("planning_skipped_beads")),
        key_fields=("bead_id",),
    )
    merged_prompts = _merge_records_by_keys(
        _summary_list_of_dicts(existing.get("prompts")),
        _summary_list_of_dicts(current.get("prompts")),
        key_fields=("path", "bead_id"),
    )
    merged_validations = _merge_records_by_keys(
        _summary_list_of_dicts(existing.get("validations")),
        _summary_list_of_dicts(current.get("validations")),
        key_fields=("command",),
    )

    merged = dict(existing)
    merged.update(current)
    merged["beads_attempted"] = _summary_int(existing.get("beads_attempted")) + _summary_int(
        current.get("beads_attempted")
    )
    merged["beads_closed"] = _summary_int(existing.get("beads_closed")) + _summary_int(
        current.get("beads_closed")
    )
    merged["beads"] = merged_beads
    merged["planning_skipped_beads"] = merged_planning_skipped
    merged["failures"] = _merge_unique_strings(
        _summary_string_list(existing.get("failures")),
        _summary_string_list(current.get("failures")),
    )
    merged["follow_ups"] = _merge_unique_strings(
        _summary_string_list(existing.get("follow_ups")),
        _summary_string_list(current.get("follow_ups")),
    )
    merged["prompts"] = merged_prompts
    merged["validations"] = merged_validations
    merged["notebook_refactors"] = _merge_notebook_refactors(
        existing.get("notebook_refactors"),
        current.get("notebook_refactors"),
    )
    merged["high_level_context"] = _merge_high_level_context(
        existing.get("high_level_context"),
        current.get("high_level_context"),
        planning_skipped=merged_planning_skipped,
    )
    if current.get("run_report_path") is None and isinstance(existing.get("run_report_path"), str):
        merged["run_report_path"] = existing.get("run_report_path")
    return merged


class RepoRunReporter:
    def __init__(
        self,
        *,
        paths: Any,
        run_id: str,
        repo_policy: Any,
        config: Any,
        tool_versions: dict[str, Any],
        artifacts: RepoRunArtifacts,
        state: RepoExecutionState,
        emit: Any,
        now_fn: Any,
        append_log_fn: Any,
    ) -> None:
        self.paths = paths
        self.run_id = run_id
        self.repo_policy = repo_policy
        self.config = config
        self.tool_versions = tool_versions
        self.artifacts = artifacts
        self.state = state
        self.emit = emit
        self.now_fn = now_fn
        self.append_log_fn = append_log_fn

    def build_summary_payload(
        self,
        *,
        branch: str | None,
        skipped: bool,
        skip_reason: str | None,
        stop_reason_value: str | None,
        beads_attempted_count: int,
        beads_closed_count: int,
    ) -> dict[str, Any]:
        next_action = _infer_next_action(
            skipped=skipped,
            skip_reason=skip_reason,
            stop_reason=stop_reason_value,
            bead_audits=self.state.bead_audits,
        )
        codex_argv = (
            "codex",
            "exec",
            "--full-auto",
            *codex_cli_args_for_settings(self.config.ai_settings),
        )
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "repo_id": self.repo_policy.repo_id,
            "repo_path": self.repo_policy.path.as_posix(),
            "branch": branch,
            "skipped": skipped,
            "skip_reason": skip_reason,
            "stop_reason": stop_reason_value,
            "beads_attempted": beads_attempted_count,
            "beads_closed": beads_closed_count,
            "deck_path": self.state.deck_path.as_posix() if self.state.deck_path is not None else None,
            "reused_existing_deck": self.state.reused_existing_deck,
            "planning_audit": {
                "json_path": self.artifacts.planning_audit_json_path.as_posix(),
                "md_path": self.artifacts.planning_audit_md_path.as_posix(),
                "json_exists": self.artifacts.planning_audit_json_path.exists(),
                "md_exists": self.artifacts.planning_audit_md_path.exists(),
            },
            "run_report_path": self.state.run_report_path.as_posix()
            if self.state.run_report_path is not None
            else None,
            "beads": self.state.bead_audits,
            "planning_skipped_beads": self.state.planning_skipped,
            "failures": self.state.repo_failures,
            "follow_ups": self.state.follow_ups,
            "prompts": self.state.prompt_records,
            "validations": [
                {"command": command, "status": status}
                for command, status in sorted(self.state.validation_status_by_command.items())
            ],
            "notebook_refactors": {
                "notebooks": sorted(self.state.notebooks_touched),
                "extracted_code": sorted(self.state.extracted_code_touched),
            },
            "high_level_context": {
                "focus": self.config.focus,
                "planned_beads": list(self.state.planned_scope),
                "replan_requested": bool(self.config.replan),
                "reused_existing_deck": self.state.reused_existing_deck,
                "planning_skipped_count": len(self.state.planning_skipped),
                "safety": {
                    "max_beads_per_tick": self.config.max_beads_per_tick,
                    "min_minutes_to_start_new_bead": self.config.min_minutes_to_start_new_bead,
                    "diff_cap_files": self.config.diff_caps.max_files_changed,
                    "diff_cap_lines": self.config.diff_caps.max_lines_added,
                },
            },
            "ai_settings": self.config.ai_settings.to_json_dict(),
            "codex_command": shlex.join(codex_argv),
            "codex_argv": list(codex_argv),
            "tool_versions": self.tool_versions,
            "next_action": next_action,
        }

    def maybe_write_repo_report(
        self,
        *,
        branch: str | None,
        summary: Mapping[str, Any] | None = None,
    ) -> Path | None:
        if branch is None:
            return None

        rel_report = f"docs/runs/{self.run_id}.md"
        try:
            validate_paths_within_policy(
                paths=[rel_report],
                allowed_roots=self.repo_policy.allowed_roots,
                deny_roots=self.repo_policy.deny_roots,
            )
        except GitError as e:
            self.state.repo_failures.append(f"Run report not written: {e}")
            self.emit("run_report_skipped", reason=str(e))
            return None

        live_summary = (
            dict(summary)
            if summary is not None
            else _merge_repo_summary(
                _load_json_object(self.artifacts.summary_path),
                self.build_summary_payload(
                    branch=branch,
                    skipped=False,
                    skip_reason=None,
                    stop_reason_value=self.state.stop_reason,
                    beads_attempted_count=self.state.beads_attempted,
                    beads_closed_count=self.state.beads_closed,
                ),
            )
        )
        planning_audit = live_summary.get("planning_audit")
        if isinstance(planning_audit, dict):
            planning_audit = dict(planning_audit)
            for key in ("json_path", "md_path"):
                raw = planning_audit.get(key)
                if not isinstance(raw, str) or not raw:
                    continue
                try:
                    planning_audit[key] = Path(raw).relative_to(self.paths.cache_dir).as_posix()
                except ValueError:
                    planning_audit[key] = raw
        else:
            planning_audit = None

        content = format_repo_run_report_md(
            repo_id=self.repo_policy.repo_id,
            run_id=self.run_id,
            branch=branch,
            high_level_context=live_summary.get("high_level_context")
            if isinstance(live_summary.get("high_level_context"), dict)
            else None,
            planning_audit=planning_audit,
            ai_settings=live_summary.get("ai_settings")
            if isinstance(live_summary.get("ai_settings"), dict)
            else self.config.ai_settings.to_json_dict(),
            codex_command=str(live_summary.get("codex_command") or ""),
            prompts=_summary_list_of_dicts(live_summary.get("prompts")),
            beads=_summary_list_of_dicts(live_summary.get("beads")),
            planning_skipped=_summary_list_of_dicts(live_summary.get("planning_skipped_beads")),
            notebook_refactors=live_summary.get("notebook_refactors")
            if isinstance(live_summary.get("notebook_refactors"), dict)
            else {"notebooks": [], "extracted_code": []},
            validations=_summary_list_of_dicts(live_summary.get("validations")),
            failures=_summary_string_list(live_summary.get("failures")),
            follow_ups=_summary_string_list(live_summary.get("follow_ups")),
            tool_versions=self.tool_versions,
            generated_at=self.now_fn(),
        )

        try:
            self.state.run_report_path = write_repo_run_report(
                repo_root=self.repo_policy.path,
                run_id=self.run_id,
                content=content,
            )
        except OSError as e:
            self.state.repo_failures.append(f"Run report write failed: {e}")
            self.emit("run_report_failed", error=str(e))
            return None
        self.emit("run_report_written", path=str(self.state.run_report_path))
        return self.state.run_report_path

    def finalize(self, result: Any) -> Any:
        current_summary = self.build_summary_payload(
            branch=result.branch,
            skipped=result.skipped,
            skip_reason=result.skip_reason,
            stop_reason_value=result.stop_reason,
            beads_attempted_count=result.beads_attempted,
            beads_closed_count=result.beads_closed,
        )
        existing_summary = _load_json_object(self.artifacts.summary_path)
        summary = _merge_repo_summary(existing_summary, current_summary)
        write_json_atomic(self.artifacts.summary_path, summary)
        _write_run_summary(self.paths, run_id=self.run_id)
        self.append_log_fn(
            self.artifacts.run_log_path,
            f"{self.now_fn().isoformat()} repo_end repo_id={self.repo_policy.repo_id} "
            f"skipped={result.skipped} skip_reason={result.skip_reason} "
            f"stop_reason={result.stop_reason} attempted={result.beads_attempted} "
            f"closed={result.beads_closed}",
        )
        self.emit(
            "repo_end",
            skipped=result.skipped,
            skip_reason=result.skip_reason,
            stop_reason=result.stop_reason,
            beads_attempted=result.beads_attempted,
            beads_closed=result.beads_closed,
            next_action=current_summary["next_action"],
        )
        return result
