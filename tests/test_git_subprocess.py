from __future__ import annotations

import subprocess
from pathlib import Path

from codex_orchestrator.git_subprocess import (
    detect_dirty_ignore_globs,
    git_is_dirty,
    git_remove_ignored_untracked,
    git_status_filtered,
    resolve_dirty_ignore_globs,
)


def _git(repo_root: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(tmp_path: Path) -> Path:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / "tracked.txt").write_text("hello\n", encoding="utf-8")
    _git(repo_root, "add", "tracked.txt")
    _git(repo_root, "commit", "-m", "init")
    return repo_root


def test_git_is_dirty_ignores_untracked_globs(tmp_path: Path) -> None:
    repo_root = _init_repo(tmp_path)
    cache_dir = repo_root / ".pytest_cache"
    cache_dir.mkdir()
    (cache_dir / "foo.txt").write_text("data\n", encoding="utf-8")

    assert git_is_dirty(repo_root=repo_root) is True
    assert git_is_dirty(repo_root=repo_root, ignore_globs=(".pytest_cache/**",)) is False


def test_git_remove_ignored_untracked_removes(tmp_path: Path) -> None:
    repo_root = _init_repo(tmp_path)
    cache_dir = repo_root / ".pytest_cache"
    cache_dir.mkdir()
    (cache_dir / "foo.txt").write_text("data\n", encoding="utf-8")

    removed = git_remove_ignored_untracked(
        repo_root=repo_root,
        ignore_globs=(".pytest_cache/**",),
    )
    assert removed
    assert not (cache_dir / "foo.txt").exists()
    assert git_is_dirty(repo_root=repo_root, ignore_globs=(".pytest_cache/**",)) is False


def test_git_status_filtered_keeps_tracked_changes(tmp_path: Path) -> None:
    repo_root = _init_repo(tmp_path)
    (repo_root / "tracked.txt").write_text("changed\n", encoding="utf-8")

    status = git_status_filtered(repo_root=repo_root, ignore_globs=(".pytest_cache/**",))
    assert any(entry.path == "tracked.txt" for entry in status)


def test_git_status_filtered_ignores_tracked_globs(tmp_path: Path) -> None:
    repo_root = _init_repo(tmp_path)
    beads_dir = repo_root / ".beads"
    beads_dir.mkdir()
    issues = beads_dir / "issues.jsonl"
    issues.write_text("[]\n", encoding="utf-8")
    _git(repo_root, "add", ".beads/issues.jsonl")
    _git(repo_root, "commit", "-m", "track beads")

    issues.write_text("[{\"id\": \"bd-1\"}]\n", encoding="utf-8")
    status = git_status_filtered(repo_root=repo_root, ignore_globs=(".beads/**",))
    assert not any(entry.path == ".beads/issues.jsonl" for entry in status)


def test_resolve_dirty_ignore_globs_includes_defaults(tmp_path: Path) -> None:
    repo_root = _init_repo(tmp_path)
    resolved = resolve_dirty_ignore_globs(repo_root=repo_root, configured=())
    assert ".pytest_cache/**" in resolved.resolved


def test_detect_dirty_ignore_globs_skips_tracked_prefix(tmp_path: Path) -> None:
    repo_root = _init_repo(tmp_path)
    data_dir = repo_root / "tests" / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "fixture.txt").write_text("x\n", encoding="utf-8")
    _git(repo_root, "add", "tests/data/fixture.txt")
    _git(repo_root, "commit", "-m", "add fixture")

    (data_dir / "generated.txt").write_text("y\n", encoding="utf-8")
    detected = detect_dirty_ignore_globs(
        repo_root=repo_root,
        candidates=("tests/data/**",),
    )
    assert detected == ()


def test_detect_dirty_ignore_globs_detects_untracked(tmp_path: Path) -> None:
    repo_root = _init_repo(tmp_path)
    data_dir = repo_root / "tests" / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "generated.txt").write_text("y\n", encoding="utf-8")

    detected = detect_dirty_ignore_globs(
        repo_root=repo_root,
        candidates=("tests/data/**",),
    )
    assert detected == ("tests/data/**",)
