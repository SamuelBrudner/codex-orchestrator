from __future__ import annotations

from pathlib import Path

import pytest

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

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        calls["args"] = args
        calls["cwd"] = cwd
        calls["timeout_seconds"] = timeout_seconds
        calls["ok_exit_codes"] = ok_exit_codes
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    beads_subprocess.bd_init(repo_root=repo_root)

    assert calls["args"] == ["init", "--quiet"]
    assert calls["cwd"] == repo_root


def test_bd_update_accepts_single_item_list(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
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


def test_bd_doctor_allows_exit_one_json(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == ["doctor", "--json"]
        assert cwd == repo_root
        assert tuple(ok_exit_codes) == (0, 1)
        return '{"overall_ok": false, "checks": []}'

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    out = beads_subprocess.bd_doctor(repo_root=repo_root)

    assert out["overall_ok"] is False


def test_bd_sync_ignores_non_json_output(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == ["sync", "--json"]
        assert cwd == repo_root
        return "Exporting beads to JSONL...\n✓ Exported 0 issues\n"

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    out = beads_subprocess.bd_sync(repo_root=repo_root)

    assert out == {}


def test_bd_ready_uses_explicit_limit(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == [
            "ready",
            "--json",
            "--limit",
            str(beads_subprocess.DEFAULT_BD_READY_LIMIT),
        ]
        assert cwd == repo_root
        return (
            '[{"id":"bd-1","title":"Ready bead","labels":["x"],'
            '"description":"d","issue_type":"task"}]'
        )

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    out = beads_subprocess.bd_ready(repo_root=repo_root)

    assert len(out) == 1
    assert out[0].bead_id == "bd-1"
    assert out[0].title == "Ready bead"
    assert out[0].issue_type == "task"


def test_bd_ready_rejects_non_positive_limit(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    with pytest.raises(beads_subprocess.BdCliError, match="bd ready limit must be >= 1"):
        beads_subprocess.bd_ready(repo_root=repo_root, limit=0)


def test_bd_show_parses_parent_and_dependency_links(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == ["show", "bd-1", "--json"]
        assert cwd == repo_root
        return json_payload

    json_payload = (
        '[{"id":"bd-1","title":"Child bead","status":"open","notes":"",'
        '"parent":"bd-epic",'
        '"dependencies":[{"id":"bd-epic","dependency_type":"parent-child","status":"open","issue_type":"epic"}],'
        '"dependents":[{"id":"bd-2","dependency_type":"blocks","status":"open","issue_type":"task"}]}]'
    )

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    issue = beads_subprocess.bd_show(repo_root=repo_root, issue_id="bd-1")

    assert issue.parent_id == "bd-epic"
    assert issue.dependencies == ("bd-epic",)
    assert issue.dependents == ("bd-2",)
    assert issue.dependency_links[0].issue_id == "bd-epic"
    assert issue.dependency_links[0].dependency_type == "parent-child"
    assert issue.dependent_links[0].issue_id == "bd-2"
    assert issue.dependent_links[0].dependency_type == "blocks"


def test_bd_show_infers_parent_from_parent_child_dependency(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == ["show", "bd-1", "--json"]
        assert cwd == repo_root
        return (
            '[{"id":"bd-1","title":"Child bead","status":"open","notes":"",'
            '"dependencies":[{"id":"bd-epic","dependency_type":"parent-child"}],'
            '"dependents":[]}]'
        )

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    issue = beads_subprocess.bd_show(repo_root=repo_root, issue_id="bd-1")

    assert issue.parent_id == "bd-epic"
