from __future__ import annotations

from pathlib import Path

from codex_orchestrator.beads_subprocess import BdIssue, BdIssueSummary


def test_ensure_notebook_refactor_issues_creates_and_blocks(tmp_path: Path, monkeypatch) -> None:
    import codex_orchestrator.notebook_refactor_issues as nb

    def _bd_list(*, repo_root: Path) -> tuple[BdIssueSummary, ...]:
        return (
            BdIssueSummary(
                issue_id="bd-10",
                title="notebook-refactor: notebooks/existing.ipynb",
                status="open",
            ),
        )

    created: list[tuple[str, str]] = []

    def _bd_create(
        *,
        repo_root: Path,
        title: str,
        issue_type: str = "task",
        priority: int = 2,
        labels=None,
        description=None,
        acceptance_criteria=None,
        design=None,
        estimate_minutes=None,
        deps=None,
    ) -> BdIssue:
        issue_id = "bd-11"
        created.append((issue_id, title))
        return BdIssue(
            issue_id=issue_id,
            title=title,
            status="open",
            notes="",
            dependencies=(),
            dependents=(),
        )

    captured_notes: dict[str, str] = {}

    def _bd_update(*, repo_root: Path, issue_id: str, status=None, notes=None) -> BdIssue:
        title = next((t for i, t in created if i == issue_id), "<unknown>")
        captured_notes[issue_id] = str(notes or "")
        return BdIssue(
            issue_id=issue_id,
            title=title,
            status="open",
            notes=str(notes or ""),
            dependencies=(),
            dependents=(),
        )

    def _bd_show(*, repo_root: Path, issue_id: str) -> BdIssue:
        return BdIssue(
            issue_id=issue_id,
            title="Downstream bead",
            status="open",
            notes="",
            dependencies=("bd-10",),
            dependents=(),
        )

    dep_add_calls: list[tuple[str, str, str]] = []

    def _bd_dep_add(*, repo_root: Path, issue_id: str, depends_on_id: str, dep_type: str = "blocks") -> None:
        dep_add_calls.append((issue_id, depends_on_id, dep_type))

    monkeypatch.setattr(nb, "bd_list", _bd_list)
    monkeypatch.setattr(nb, "bd_create", _bd_create)
    monkeypatch.setattr(nb, "bd_update", _bd_update)
    monkeypatch.setattr(nb, "bd_show", _bd_show)
    monkeypatch.setattr(nb, "bd_dep_add", _bd_dep_add)

    result = nb.ensure_notebook_refactor_issues(
        repo_root=tmp_path,
        notebook_paths=("notebooks/existing.ipynb", "notebooks/new.ipynb"),
        limit=10,
        time_budget_minutes=45,
        validation_commands=("pytest -q", "pytest -q", "python -m compileall -q src"),
        notebook_output_policy="strip",
        block_bead_ids=("bd-1",),
    )

    assert result.notebook_paths == ("notebooks/existing.ipynb", "notebooks/new.ipynb")
    assert result.issue_ids == ("bd-10", "bd-11")
    assert result.created_issues == (
        nb.NotebookRefactorIssue(
            issue_id="bd-11",
            title="notebook-refactor: notebooks/new.ipynb",
        ),
    )

    assert dep_add_calls == [("bd-1", "bd-11", "blocks")]

    assert "time_budget_minutes: 45" in captured_notes["bd-11"]
    assert "`pytest -q`" in captured_notes["bd-11"]
    assert "`python -m compileall -q src`" in captured_notes["bd-11"]

