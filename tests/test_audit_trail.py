from __future__ import annotations

from datetime import datetime, timezone

from codex_orchestrator.audit_trail import format_repo_run_report_md


def _tool_versions() -> dict[str, str]:
    return {
        "bd": "bd 0.0.0",
        "codex": "codex 0.0.0",
        "git": "git version 0.0.0",
        "python": "Python 3.11.0",
        "conda": "conda 0.0.0",
    }


def test_run_report_includes_aims_and_design_rationale_section() -> None:
    report = format_repo_run_report_md(
        repo_id="repo_x",
        run_id="run-123",
        branch="run/run-123",
        high_level_context={
            "focus": "stabilize final review and run report readability",
            "planned_beads": [{"bead_id": "bd-1", "title": "Improve report sectioning"}],
            "replan_requested": True,
            "reused_existing_deck": False,
            "planning_skipped_count": 2,
            "safety": {
                "max_beads_per_tick": 3,
                "min_minutes_to_start_new_bead": 15,
                "diff_cap_files": 25,
                "diff_cap_lines": 1500,
            },
        },
        planning_audit={
            "json_path": "runs/run-123/repo_x.planning_audit.json",
            "md_path": "runs/run-123/repo_x.planning_audit.md",
            "json_exists": True,
            "md_exists": True,
        },
        ai_settings={"model": "gpt-5.3-codex", "reasoning_effort": "xhigh"},
        codex_command="codex exec --full-auto --model gpt-5.3-codex",
        prompts=[],
        beads=[],
        planning_skipped=[],
        notebook_refactors={"notebooks": [], "extracted_code": []},
        validations=[],
        failures=[],
        follow_ups=[],
        tool_versions=_tool_versions(),
        generated_at=datetime(2026, 2, 18, 10, 30, tzinfo=timezone.utc),
    )

    assert "## Aims and Design Rationale" in report
    assert "stabilize final review and run report readability" in report
    assert "- Planned scope: `bd-1`" in report
    assert "Replanned deck this tick (`--replan`)" in report
    assert "max_beads_per_tick=3" in report
    assert "Planner skipped 2 bead(s)" in report


def test_run_report_uses_default_aim_when_context_missing() -> None:
    report = format_repo_run_report_md(
        repo_id="repo_x",
        run_id="run-123",
        branch="run/run-123",
        planning_audit=None,
        ai_settings={"model": "gpt-5.3-codex", "reasoning_effort": "xhigh"},
        codex_command=None,
        prompts=[],
        beads=[],
        planning_skipped=[],
        notebook_refactors={"notebooks": [], "extracted_code": []},
        validations=[],
        failures=[],
        follow_ups=[],
        tool_versions=_tool_versions(),
        generated_at=datetime(2026, 2, 18, 10, 30, tzinfo=timezone.utc),
    )

    assert "## Aims and Design Rationale" in report
    assert "Close planned beads with minimal, validation-backed changes and clear auditability." in report
