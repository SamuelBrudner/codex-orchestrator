from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

import codex_orchestrator.beads_subprocess as beads_subprocess


def _caps(
    *,
    workspace_layout: str,
    supports_bootstrap: bool = False,
    supports_init_from_jsonl: bool = False,
    supports_sync: bool = False,
    supports_structured_doctor_output: bool | None = None,
    version: str | None = "bd version test",
) -> beads_subprocess.BdCapabilities:
    return beads_subprocess.BdCapabilities(
        version=version,
        workspace_layout=workspace_layout,
        supports_bootstrap=supports_bootstrap,
        supports_init_from_jsonl=supports_init_from_jsonl,
        supports_sync=supports_sync,
        supports_structured_doctor_output=supports_structured_doctor_output,
    )


def test_bd_prepare_workspace_skips_when_legacy_db_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: _caps(workspace_layout="legacy_db"),
    )

    called = {"value": False}

    def _fake_run_bd(*args, **kwargs) -> str:
        called["value"] = True
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    result = beads_subprocess.bd_prepare_workspace(repo_root=repo_root)

    assert result.workspace_layout == "legacy_db"
    assert called["value"] is False


def test_bd_prepare_workspace_skips_when_modern_workspace_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: _caps(workspace_layout="modern_embeddeddolt"),
    )

    called = {"value": False}

    def _fake_run_bd(*args, **kwargs) -> str:
        called["value"] = True
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    result = beads_subprocess.bd_prepare_workspace(repo_root=repo_root)

    assert result.workspace_layout == "modern_embeddeddolt"
    assert called["value"] is False


def test_bd_prepare_workspace_uses_bootstrap_for_tracked_jsonl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    capabilities = iter(
        [
            _caps(workspace_layout="jsonl_only", supports_bootstrap=True),
            _caps(workspace_layout="modern_embeddeddolt", supports_bootstrap=True),
        ]
    )

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: next(capabilities),
    )

    calls: list[list[str]] = []

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    result = beads_subprocess.bd_prepare_workspace(repo_root=repo_root)

    assert calls == [["bootstrap"]]
    assert result.workspace_layout == "modern_embeddeddolt"


def test_bd_prepare_workspace_uses_init_from_jsonl_when_bootstrap_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    capabilities = iter(
        [
            _caps(workspace_layout="jsonl_only", supports_init_from_jsonl=True),
            _caps(workspace_layout="modern_embeddeddolt", supports_init_from_jsonl=True),
        ]
    )

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: next(capabilities),
    )

    calls: list[list[str]] = []

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    result = beads_subprocess.bd_prepare_workspace(repo_root=repo_root)

    assert calls == [["init", "--from-jsonl", "--quiet"]]
    assert result.workspace_layout == "modern_embeddeddolt"


def test_bd_prepare_workspace_falls_back_to_init_from_jsonl_when_bootstrap_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    capabilities = iter(
        [
            _caps(
                workspace_layout="jsonl_only",
                supports_bootstrap=True,
                supports_init_from_jsonl=True,
            ),
            _caps(workspace_layout="modern_embeddeddolt", supports_init_from_jsonl=True),
        ]
    )

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: next(capabilities),
    )

    calls: list[list[str]] = []

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        calls.append(list(args))
        if list(args) == ["bootstrap"]:
            raise beads_subprocess.BdCliError("bootstrap failed")
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    result = beads_subprocess.bd_prepare_workspace(repo_root=repo_root)

    assert calls == [["bootstrap"], ["init", "--from-jsonl", "--quiet"]]
    assert result.workspace_layout == "modern_embeddeddolt"


def test_bd_prepare_workspace_uses_plain_init_for_missing_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    capabilities = iter(
        [
            _caps(workspace_layout="missing"),
            _caps(workspace_layout="modern_embeddeddolt"),
        ]
    )

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: next(capabilities),
    )

    calls: list[list[str]] = []

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    result = beads_subprocess.bd_prepare_workspace(repo_root=repo_root)

    assert calls == [["init", "--quiet"]]
    assert result.workspace_layout == "modern_embeddeddolt"


def test_bd_prepare_workspace_rejects_unknown_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    beads_dir = repo_root / ".beads"
    beads_dir.mkdir(parents=True)
    (beads_dir / "mystery.txt").write_text("mystery\n", encoding="utf-8")

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: _caps(workspace_layout="unknown"),
    )

    with pytest.raises(beads_subprocess.BdCliError, match="unrecognized .beads layout"):
        beads_subprocess.bd_prepare_workspace(repo_root=repo_root)


def test_bd_init_delegates_to_prepare_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    called = {"value": False}

    def _prepare(*, repo_root: Path) -> beads_subprocess.BdCapabilities:
        called["value"] = True
        return _caps(workspace_layout="modern_embeddeddolt")

    monkeypatch.setattr(beads_subprocess, "bd_prepare_workspace", _prepare)

    beads_subprocess.bd_init(repo_root=repo_root)

    assert called["value"] is True


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

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: _caps(workspace_layout="modern_embeddeddolt"),
    )

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == ["doctor", "--json"]
        assert cwd == repo_root
        assert tuple(ok_exit_codes) == (0, 1)
        return '{"overall_ok": false, "checks": []}'

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    out = beads_subprocess.bd_doctor(repo_root=repo_root)

    assert out.status == "warn"
    assert out.overall_ok is False
    assert out.capabilities.supports_structured_doctor_output is True


def test_bd_doctor_tolerates_text_output_as_unsupported(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: _caps(workspace_layout="modern_embeddeddolt"),
    )

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == ["doctor", "--json"]
        return "Note: 'bd doctor' is not yet supported in embedded mode.\n"

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    out = beads_subprocess.bd_doctor(repo_root=repo_root)

    assert out.status == "unsupported"
    assert out.message == "doctor_json_output_unavailable"
    assert "embedded mode" in out.raw_output
    assert out.capabilities.supports_structured_doctor_output is False


def test_bd_sync_reports_unsupported_when_command_missing(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: _caps(workspace_layout="modern_embeddeddolt", supports_sync=False),
    )

    out = beads_subprocess.bd_sync(repo_root=repo_root)

    assert out.status == "unsupported"
    assert out.message == "sync_command_unavailable"


def test_bd_sync_allows_non_json_output(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setattr(
        beads_subprocess,
        "bd_detect_capabilities",
        lambda *, repo_root: _caps(workspace_layout="legacy_db", supports_sync=True),
    )

    def _fake_run_bd(args, *, cwd, timeout_seconds=60.0, ok_exit_codes=(0,)) -> str:
        assert args == ["sync", "--json"]
        assert cwd == repo_root
        return "Exporting beads to JSONL...\nExported 0 issues\n"

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    out = beads_subprocess.bd_sync(repo_root=repo_root)

    assert out.status == "ok"
    assert "Exporting beads" in out.raw_output


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


def test_bd_ready_tolerates_warning_preamble_before_json(tmp_path: Path, monkeypatch) -> None:
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
        return "\n".join(
            [
                "Warning: daemon failed to start",
                "LEGACY DATABASE DETECTED!",
                "",
                '[{"id":"bd-1","title":"Test bead","labels":["infra"],"issue_type":"task"}]',
            ]
        )

    monkeypatch.setattr(beads_subprocess, "_run_bd", _fake_run_bd)

    out = beads_subprocess.bd_ready(repo_root=repo_root)

    assert len(out) == 1
    assert out[0].bead_id == "bd-1"
    assert out[0].title == "Test bead"
    assert out[0].labels == ("infra",)
    assert out[0].issue_type == "task"


def test_bd_prepare_workspace_bootstraps_real_bd_from_tracked_jsonl(tmp_path: Path) -> None:
    if shutil.which("bd") is None:
        pytest.skip("bd is not installed")

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo_root, check=True)

    beads_dir = repo_root / ".beads"
    beads_dir.mkdir()
    source_jsonl = Path(__file__).resolve().parents[1] / ".beads" / "issues.jsonl"
    if not source_jsonl.exists():
        pytest.skip("repo does not have tracked .beads/issues.jsonl fixture")
    (beads_dir / "issues.jsonl").write_text(source_jsonl.read_text(encoding="utf-8"), encoding="utf-8")

    capabilities = beads_subprocess.bd_prepare_workspace(repo_root=repo_root)
    ready = beads_subprocess.bd_ready(repo_root=repo_root, limit=10)

    assert capabilities.workspace_layout in {"legacy_db", "modern_embeddeddolt"}
    assert ready
