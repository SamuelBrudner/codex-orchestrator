from __future__ import annotations

from codex_orchestrator.repo_execution import BeadResult
from codex_orchestrator.roadtrip_cli import _count_bead_outcomes


def test_count_bead_outcomes_counts_closed_and_failed() -> None:
    beads = (
        BeadResult(
            bead_id="bd-1",
            title="Closed bead",
            outcome="closed",
            detail="ok",
            commit_hash="abc123",
        ),
        BeadResult(
            bead_id="bd-2",
            title="Failed bead",
            outcome="failed",
            detail="no changes detected",
            commit_hash=None,
        ),
        BeadResult(
            bead_id="bd-3",
            title="Already closed elsewhere",
            outcome="skipped_closed",
            detail="skipped",
            commit_hash=None,
        ),
    )

    closed, failed = _count_bead_outcomes(beads)
    assert closed == 1
    assert failed == 1

