from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any


class AuditTrailError(RuntimeError):
    pass


def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
        tmp_name = f.name
    os.replace(tmp_name, path)


def write_text_atomic(path: Path, content: str) -> None:
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


def append_jsonl(path: Path, event: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(dict(event), sort_keys=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(payload + "\n")


def append_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(message.rstrip("\n") + "\n")


def _run_version_command(args: tuple[str, ...], *, cwd: Path, timeout_seconds: float) -> str | None:
    try:
        completed = subprocess.run(
            list(args),
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError:
        return None
    except subprocess.TimeoutExpired:
        return "<timeout>"

    output = (completed.stdout or "").strip() or (completed.stderr or "").strip()
    if completed.returncode != 0:
        return output.splitlines()[0].strip() if output else None
    return output.splitlines()[0].strip() if output else None


def collect_tool_versions(*, safe_cwd: Path) -> dict[str, str]:
    safe_cwd.mkdir(parents=True, exist_ok=True)
    versions: dict[str, str] = {}

    python_version = sys.version.split()[0] if sys.version else "<unknown>"
    versions["python"] = f"Python {python_version} ({sys.executable})"

    for tool, argv in [
        ("git", ("git", "--version")),
        ("bd", ("bd", "--version")),
        ("codex", ("codex", "--version")),
        ("conda", ("conda", "--version")),
    ]:
        v = _run_version_command(argv, cwd=safe_cwd, timeout_seconds=10.0)
        versions[tool] = v if v is not None else "<unavailable>"

    return versions


def format_repo_run_report_md(
    *,
    repo_id: str,
    run_id: str,
    branch: str | None,
    planning_audit: Mapping[str, Any] | None = None,
    ai_settings: Mapping[str, str] | None,
    codex_command: str | None,
    beads: list[Mapping[str, Any]],
    planning_skipped: list[Mapping[str, Any]],
    notebook_refactors: Mapping[str, list[str]],
    validations: list[Mapping[str, Any]],
    failures: list[str],
    follow_ups: list[str],
    tool_versions: Mapping[str, str],
    generated_at: datetime,
) -> str:
    branch_display = branch or f"run/{run_id}"

    lines: list[str] = []
    lines.append(f"# Run Report ({repo_id})")
    lines.append("")
    lines.append("## Summary")
    if beads:
        closed = sum(1 for b in beads if b.get("outcome") == "closed")
        failed = sum(1 for b in beads if b.get("outcome") == "failed")
        skipped = sum(1 for b in beads if str(b.get("outcome", "")).startswith("skipped"))
        summary = (
            f"- Beads: closed={closed} failed={failed} skipped={skipped} total={len(beads)}"
        )
        lines.append(summary)
    else:
        lines.append("- No beads attempted.")
    if failures:
        lines.append(f"- Failures/skips recorded: {len(failures)}")
    lines.append(f"- Generated at: {generated_at.isoformat()}")
    lines.append("")

    lines.append("## Run")
    lines.append(f"- RUN_ID: `{run_id}`")
    lines.append(f"- Branch: `{branch_display}`")
    lines.append("")

    lines.append("## Planning Audit")
    if planning_audit is None:
        lines.append("- <unavailable>")
    else:
        json_path = planning_audit.get("json_path")
        md_path = planning_audit.get("md_path")
        json_exists = planning_audit.get("json_exists")
        md_exists = planning_audit.get("md_exists")

        json_suffix = ""
        if isinstance(json_exists, bool):
            json_suffix = " (exists)" if json_exists else " (missing)"
        md_suffix = ""
        if isinstance(md_exists, bool):
            md_suffix = " (exists)" if md_exists else " (missing)"

        if isinstance(json_path, str) and json_path.strip():
            lines.append(f"- JSON: `{json_path}`{json_suffix}")
        else:
            lines.append("- JSON: <missing>")
        if isinstance(md_path, str) and md_path.strip():
            lines.append(f"- Markdown: `{md_path}`{md_suffix}")
        else:
            lines.append("- Markdown: <missing>")
    lines.append("")

    lines.append("## AI Configuration")
    if ai_settings is None:
        lines.append("- <unavailable>")
    else:
        model = ai_settings.get("model", "<unknown>")
        reasoning_effort = ai_settings.get("reasoning_effort", "<unknown>")
        lines.append(f"- Model: `{model}`")
        lines.append(f"- Reasoning effort: `{reasoning_effort}`")
    if codex_command:
        lines.append(f"- Codex invocation: `{codex_command}`")
    lines.append("")

    lines.append("## Beads Issues Worked")
    if not beads:
        lines.append("- None")
    else:
        for b in beads:
            bead_id = b.get("bead_id", "<unknown>")
            title = b.get("title", "")
            outcome = b.get("outcome", "")
            detail = b.get("detail", "")
            suffix = f" — {title}" if title else ""
            extra = f" ({detail})" if detail else ""
            lines.append(f"- `{bead_id}`{suffix}: `{outcome}`{extra}")
    lines.append("")

    lines.append("## Notebook Refactors")
    notebooks = notebook_refactors.get("notebooks", [])
    extracted = notebook_refactors.get("extracted_code", [])
    lines.append("- Notebooks")
    if notebooks:
        lines.extend([f"  - `{p}`" for p in notebooks])
    else:
        lines.append("  - None")
    lines.append("- Extracted code locations")
    if extracted:
        lines.extend([f"  - `{p}`" for p in extracted])
    else:
        lines.append("  - None")
    lines.append("")

    lines.append("## Tests / Commands Executed")
    if not validations:
        lines.append("- None")
    else:
        for v in validations:
            cmd = v.get("command", "<unknown>")
            status = v.get("status", "<unknown>")
            lines.append(f"- `{cmd}`: {status}")
    lines.append("")

    lines.append("## Failures or Skipped Steps")
    if not failures and not planning_skipped:
        lines.append("- None")
    else:
        for msg in failures:
            lines.append(f"- {msg}")
        if planning_skipped:
            lines.append("- Planner skipped beads (next action):")
            for sb in planning_skipped:
                bead_id = sb.get("bead_id", "<unknown>")
                title = sb.get("title", "")
                next_action = sb.get("next_action", "")
                suffix = f" — {title}" if title else ""
                extra = f" ({next_action})" if next_action else ""
                lines.append(f"  - `{bead_id}`{suffix}{extra}")
    lines.append("")

    lines.append("## Follow-ups")
    if not follow_ups:
        lines.append("- None")
    else:
        for item in follow_ups:
            lines.append(f"- {item}")
    lines.append("")

    lines.append("## Tool Versions")
    for key in ("bd", "codex", "git", "python", "conda"):
        value = tool_versions.get(key, "<unavailable>")
        lines.append(f"- {key}: {value}")
    lines.append("")

    return "\n".join(lines)


def write_repo_run_report(*, repo_root: Path, run_id: str, content: str) -> Path:
    report_path = repo_root / "docs" / "runs" / f"{run_id}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(content, encoding="utf-8")
    return report_path
