from __future__ import annotations

from pathlib import Path

import codex_orchestrator.beads_subprocess as beads_subprocess


def test_bd_init_skips_when_db_exists(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    beads_dir = repo_root / ".beads"
    beads_dir.mkdir(parents=True)
    (beads_dir / "beads.db").write_text("", encoding="utf-8")

    called = {"value": False}

    def _fake_run_bd(*args, **kwargs) -> str:
        called["value"] = True
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    beads_subprocess.bd_init(repo_root=repo_root)

    assert called["value"] is False


def test_bd_init_runs_when_db_missing(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    calls: dict[str, object] = {}

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0) -> str:
        calls["args"] = args
        calls["cwd"] = cwd
        calls["timeout_seconds"] = timeout_seconds
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    beads_subprocess.bd_init(repo_root=repo_root)

    assert calls["args"] == ["init", "--quiet"]
    assert calls["cwd"] == repo_root


def test_bd_update_accepts_single_item_list(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0) -> str:
        assert args[:2] == ["update", "bd-1"]
        assert cwd == repo_root
        return (
            '[{"id": "bd-1", "title": "Test bead", "status": "open", '
            '"notes": "", "dependencies": [], "dependents": []}]'
        )

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    issue = beads_subprocess.bd_update(repo_root=repo_root, issue_id="bd-1", status="open")

    assert issue.issue_id == "bd-1"
    assert issue.title == "Test bead"
