from __future__ import annotations

from pathlib import Path

import pytest

from codex_orchestrator.planning_audit import PlanningAuditError, build_planning_audit
from codex_orchestrator.repo_inventory import RepoPolicy


def _policy(*, tmp_path: Path, allowed_roots: tuple[Path, ...] = (Path("."),)) -> RepoPolicy:
    return RepoPolicy(
        repo_id="test_repo",
        path=tmp_path,
        base_branch="main",
        env=None,
        notebook_roots=(Path("notebooks"),),
        allowed_roots=allowed_roots,
        deny_roots=(),
        validation_commands=("pytest -q",),
        notebook_output_policy="strip",
    )


def test_planning_audit_records_binary_python_files_as_partial(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "binary.py").write_bytes(b"\xff\xfe\x00\x00")

    audit = build_planning_audit(run_id="run-1", repo_policy=_policy(tmp_path=tmp_path))

    assert audit["audit_status"] == "partial"
    scan = audit["signals"]["scan"]
    assert scan["python_files_total"] == 1
    assert scan["python_files_scanned"] == 1
    assert scan["read_failures"][0]["status"] == "binary"


def test_planning_audit_inventory_is_bounded_and_records_truncation(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    for idx in range(10):
        (tmp_path / "src" / f"f{idx}.txt").write_text("x\n", encoding="utf-8")

    audit = build_planning_audit(
        run_id="run-1",
        repo_policy=_policy(tmp_path=tmp_path),
        max_files=3,
    )

    assert audit["collection"]["truncated"] is True
    assert audit["audit_status"] == "partial"
    assert "Inventory truncated" in " ".join(audit["audit_notes"])


def test_planning_audit_fails_loud_on_invalid_limits(tmp_path: Path) -> None:
    with pytest.raises(PlanningAuditError):
        build_planning_audit(run_id="run-1", repo_policy=_policy(tmp_path=tmp_path), max_files=0)
