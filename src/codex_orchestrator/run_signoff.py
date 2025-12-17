from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from codex_orchestrator.audit_trail import write_json_atomic, write_text_atomic
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.run_closure_review import RunClosureReviewError, write_final_review


class RunSignoffError(RuntimeError):
    pass


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        raise RunSignoffError(f"Failed to parse JSON in {path}: {e}") from e
    except OSError as e:
        raise RunSignoffError(f"Failed to read {path}: {e}") from e


def _parse_datetime(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise RunSignoffError(f"{field}: expected ISO datetime string, got {type(value).__name__}")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as e:
        raise RunSignoffError(f"{field}: invalid ISO datetime: {value!r}") from e
    if dt.tzinfo is None:
        raise RunSignoffError(f"{field}: datetime must be timezone-aware, got {value!r}")
    return dt


def _sha256_file(path: Path) -> str:
    try:
        data = path.read_bytes()
    except FileNotFoundError as e:
        raise RunSignoffError(f"Missing file for hashing: {path}") from e
    except OSError as e:
        raise RunSignoffError(f"Failed to read file for hashing: {path}: {e}") from e
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True, slots=True)
class RunSignoff:
    schema_version: int
    run_id: str
    reviewer: str
    reviewed_at: datetime
    final_review_json: str
    final_review_md: str
    final_review_sha256: str
    notes: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "reviewer": self.reviewer,
            "reviewed_at": self.reviewed_at.isoformat(),
            "final_review": {
                "json": self.final_review_json,
                "md": self.final_review_md,
                "sha256": self.final_review_sha256,
            },
        }
        if self.notes is not None:
            out["notes"] = self.notes
        return out

    @classmethod
    def from_json_dict(cls, data: Any) -> RunSignoff:
        if not isinstance(data, dict):
            raise RunSignoffError(f"Expected dict for run signoff, got {type(data).__name__}")

        schema_version = data.get("schema_version")
        if not isinstance(schema_version, int) or schema_version != 1:
            raise RunSignoffError(
                f"schema_version: expected 1, got {schema_version!r}"
            )

        run_id = data.get("run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            raise RunSignoffError("run_id: required non-empty string")

        reviewer = data.get("reviewer")
        if not isinstance(reviewer, str) or not reviewer.strip():
            raise RunSignoffError("reviewer: required non-empty string")

        reviewed_at = _parse_datetime(data.get("reviewed_at"), field="reviewed_at")

        final_review = data.get("final_review")
        if not isinstance(final_review, dict):
            raise RunSignoffError("final_review: required object")
        json_name = final_review.get("json")
        md_name = final_review.get("md")
        sha = final_review.get("sha256")
        if not isinstance(json_name, str) or not json_name.strip():
            raise RunSignoffError("final_review.json: required non-empty string")
        if not isinstance(md_name, str) or not md_name.strip():
            raise RunSignoffError("final_review.md: required non-empty string")
        if not isinstance(sha, str) or not sha.strip():
            raise RunSignoffError("final_review.sha256: required non-empty string")

        notes = data.get("notes")
        if notes is not None and not isinstance(notes, str):
            raise RunSignoffError(f"notes: expected string, got {type(notes).__name__}")

        return cls(
            schema_version=schema_version,
            run_id=run_id,
            reviewer=reviewer,
            reviewed_at=reviewed_at,
            final_review_json=json_name,
            final_review_md=md_name,
            final_review_sha256=sha,
            notes=notes,
        )


def find_latest_ended_run_id(paths: OrchestratorPaths) -> str | None:
    runs_dir = paths.runs_dir
    if not runs_dir.exists():
        return None

    candidates: list[tuple[datetime, str]] = []
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        run_id = run_dir.name
        end_path = paths.run_end_path(run_id)
        end_payload = _read_json(end_path)
        if not isinstance(end_payload, dict):
            continue
        ended_at = _parse_datetime(end_payload.get("ended_at"), field=f"{end_path}.ended_at")
        candidates.append((ended_at, run_id))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates[-1][1]


def _ensure_final_review_exists(paths: OrchestratorPaths, *, run_id: str) -> None:
    json_path = paths.final_review_json_path(run_id)
    md_path = paths.final_review_md_path(run_id)
    if json_path.exists() and md_path.exists():
        return
    try:
        write_final_review(paths, run_id=run_id, ai_settings=None)
    except RunClosureReviewError as e:
        raise RunSignoffError(f"Unable to generate final review for RUN_ID={run_id}: {e}") from e


def write_run_signoff(
    paths: OrchestratorPaths,
    *,
    run_id: str,
    reviewer: str,
    reviewed_at: datetime,
    notes: str | None = None,
) -> RunSignoff:
    if reviewed_at.tzinfo is None:
        raise RunSignoffError("reviewed_at must be timezone-aware")
    if not reviewer.strip():
        raise RunSignoffError("reviewer must be non-empty")

    if not paths.run_end_path(run_id).exists():
        raise RunSignoffError(
            f"Cannot sign off RUN_ID={run_id}: missing end marker {paths.run_end_path(run_id)}"
        )

    _ensure_final_review_exists(paths, run_id=run_id)

    final_json = paths.final_review_json_path(run_id)
    final_md = paths.final_review_md_path(run_id)
    sha = _sha256_file(final_json)

    signoff = RunSignoff(
        schema_version=1,
        run_id=run_id,
        reviewer=reviewer,
        reviewed_at=reviewed_at,
        final_review_json=final_json.name,
        final_review_md=final_md.name,
        final_review_sha256=sha,
        notes=notes.strip() if isinstance(notes, str) and notes.strip() else None,
    )

    write_json_atomic(paths.run_signoff_json_path(run_id), signoff.to_json_dict())
    write_text_atomic(paths.run_signoff_md_path(run_id), format_run_signoff_md(signoff))
    return signoff


def load_run_signoff(paths: OrchestratorPaths, *, run_id: str) -> RunSignoff | None:
    payload = _read_json(paths.run_signoff_json_path(run_id))
    if payload is None:
        return None
    return RunSignoff.from_json_dict(payload)


def validate_run_signoff(paths: OrchestratorPaths, *, run_id: str) -> RunSignoff:
    signoff_json_path = paths.run_signoff_json_path(run_id)
    signoff = load_run_signoff(paths, run_id=run_id)
    if signoff is None:
        raise RunSignoffError(f"Missing run signoff artifact: {signoff_json_path}")
    if signoff.run_id != run_id:
        raise RunSignoffError(
            f"Signoff RUN_ID mismatch: expected {run_id!r}, got {signoff.run_id!r}"
        )

    final_json_path = paths.final_review_json_path(run_id)
    if not final_json_path.exists():
        raise RunSignoffError(f"Missing final review JSON for RUN_ID={run_id}: {final_json_path}")

    final_md_path = paths.final_review_md_path(run_id)
    if not final_md_path.exists():
        raise RunSignoffError(f"Missing final review Markdown for RUN_ID={run_id}: {final_md_path}")

    signoff_md_path = paths.run_signoff_md_path(run_id)
    if not signoff_md_path.exists():
        raise RunSignoffError(f"Missing run signoff Markdown artifact: {signoff_md_path}")

    if signoff.final_review_json != final_json_path.name:
        raise RunSignoffError(
            "Signoff final_review.json filename mismatch: "
            f"expected {final_json_path.name!r}, got {signoff.final_review_json!r}"
        )
    if signoff.final_review_md != final_md_path.name:
        raise RunSignoffError(
            "Signoff final_review.md filename mismatch: "
            f"expected {final_md_path.name!r}, got {signoff.final_review_md!r}"
        )

    sha = _sha256_file(final_json_path)
    if sha != signoff.final_review_sha256:
        raise RunSignoffError(
            "Run signoff no longer matches current final review content: "
            f"expected sha256={signoff.final_review_sha256}, got sha256={sha}"
        )
    return signoff


def format_run_signoff_md(signoff: RunSignoff) -> str:
    lines: list[str] = []
    lines.append(f"# Run Sign-off (RUN_ID={signoff.run_id})")
    lines.append("")
    lines.append("## Reviewer")
    lines.append(f"- Reviewer: {signoff.reviewer}")
    lines.append(f"- Reviewed at: {signoff.reviewed_at.isoformat()}")
    lines.append("")
    lines.append("## Final Review")
    lines.append(f"- JSON: `{signoff.final_review_json}`")
    lines.append(f"- Markdown: `{signoff.final_review_md}`")
    lines.append(f"- SHA256(final_review.json): `{signoff.final_review_sha256}`")
    if signoff.notes is not None:
        lines.append("")
        lines.append("## Notes")
        lines.append(signoff.notes)
    return "\n".join(lines).rstrip("\n") + "\n"
