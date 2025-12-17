from __future__ import annotations

import subprocess
from pathlib import Path

from codex_orchestrator.notebook_refactor_issues import detect_changed_notebooks


def _git(repo_root: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )


def test_detect_changed_notebooks_includes_modified_and_untracked(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test User")

    notebooks_dir = tmp_path / "notebooks"
    notebooks_dir.mkdir()

    tracked = notebooks_dir / "tracked.ipynb"
    tracked.write_text("{}", encoding="utf-8")
    _git(tmp_path, "add", tracked.as_posix())
    _git(tmp_path, "commit", "-m", "init")

    tracked.write_text('{"cells": []}', encoding="utf-8")

    untracked = notebooks_dir / "untracked.ipynb"
    untracked.write_text("{}", encoding="utf-8")

    outside = tmp_path / "outside.ipynb"
    outside.write_text("{}", encoding="utf-8")

    changed = detect_changed_notebooks(repo_root=tmp_path, notebook_roots=(Path("notebooks"),))
    assert changed == ("notebooks/tracked.ipynb", "notebooks/untracked.ipynb")

