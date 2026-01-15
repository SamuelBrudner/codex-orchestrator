from __future__ import annotations

from pathlib import Path

import pytest

from codex_orchestrator.repo_inventory import RepoConfigError, load_repo_inventory


def _write_config(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_repo_inventory_orders_repos_by_repo_id(tmp_path: Path) -> None:
    repo_a = tmp_path / "a"
    repo_b = tmp_path / "b"
    repo_a.mkdir()
    repo_b.mkdir()

    cfg = tmp_path / "repos.toml"
    _write_config(
        cfg,
        "\n".join(
            [
                "[repos.b_repo]",
                f'path = "{repo_b.as_posix()}"',
                'base_branch = "main"',
                "",
                "[repos.a_repo]",
                f'path = "{repo_a.as_posix()}"',
                'base_branch = "develop"',
                "",
            ]
        ),
    )

    inv = load_repo_inventory(cfg)
    assert [r.repo_id for r in inv.list_repos()] == ["a_repo", "b_repo"]


def test_load_repo_inventory_validates_required_fields(tmp_path: Path) -> None:
    repo_a = tmp_path / "a"
    repo_a.mkdir()

    cfg = tmp_path / "repos.toml"
    _write_config(
        cfg,
        "\n".join(
            [
                "[repos.a_repo]",
                f'path = "{repo_a.as_posix()}"',
                # base_branch missing
            ]
        ),
    )

    with pytest.raises(RepoConfigError) as excinfo:
        load_repo_inventory(cfg)
    assert "base_branch" in str(excinfo.value)


def test_repo_groups_unknown_repo_id_fails(tmp_path: Path) -> None:
    repo_a = tmp_path / "a"
    repo_a.mkdir()

    cfg = tmp_path / "repos.toml"
    _write_config(
        cfg,
        "\n".join(
            [
                "[repos.a_repo]",
                f'path = "{repo_a.as_posix()}"',
                'base_branch = "main"',
                "",
                "[repo_groups]",
                'grp = ["missing_repo"]',
                "",
            ]
        ),
    )

    with pytest.raises(RepoConfigError) as excinfo:
        load_repo_inventory(cfg)
    assert "unknown repo_id" in str(excinfo.value)


def test_select_repo_ids_union_and_default_all(tmp_path: Path) -> None:
    repo_a = tmp_path / "a"
    repo_b = tmp_path / "b"
    repo_a.mkdir()
    repo_b.mkdir()

    cfg = tmp_path / "repos.toml"
    _write_config(
        cfg,
        "\n".join(
            [
                "[repos.a_repo]",
                f'path = "{repo_a.as_posix()}"',
                'base_branch = "main"',
                "",
                "[repos.b_repo]",
                f'path = "{repo_b.as_posix()}"',
                'base_branch = "main"',
                "",
                "[repo_groups]",
                'grp = ["a_repo"]',
                "",
            ]
        ),
    )

    inv = load_repo_inventory(cfg)
    assert inv.select_repo_ids() == ["a_repo", "b_repo"]
    assert inv.select_repo_ids(repo_groups=["grp"]) == ["a_repo"]
    assert inv.select_repo_ids(repo_ids=["b_repo"], repo_groups=["grp"]) == ["a_repo", "b_repo"]


def test_load_repo_inventory_requires_orchestrator_outputs_in_allowed_roots(tmp_path: Path) -> None:
    repo_a = tmp_path / "a"
    repo_a.mkdir()

    cfg = tmp_path / "repos.toml"
    _write_config(
        cfg,
        "\n".join(
            [
                "[repos.a_repo]",
                f'path = "{repo_a.as_posix()}"',
                'base_branch = "main"',
                'allowed_roots = ["src"]',
                "",
            ]
        ),
    )

    with pytest.raises(RepoConfigError) as excinfo:
        load_repo_inventory(cfg)

    message = str(excinfo.value)
    assert "allowed_roots" in message
    assert ".beads" in message
    assert "docs/runs" in message


def test_load_repo_inventory_denies_cannot_cover_orchestrator_outputs(tmp_path: Path) -> None:
    repo_a = tmp_path / "a"
    repo_a.mkdir()

    cfg = tmp_path / "repos.toml"
    _write_config(
        cfg,
        "\n".join(
            [
                "[repos.a_repo]",
                f'path = "{repo_a.as_posix()}"',
                'base_branch = "main"',
                'allowed_roots = ["."]',
                'deny_roots = ["docs"]',
                "",
            ]
        ),
    )

    with pytest.raises(RepoConfigError) as excinfo:
        load_repo_inventory(cfg)

    message = str(excinfo.value)
    assert "deny_roots" in message
    assert "docs/runs" in message


def test_load_repo_inventory_parses_dirty_ignore(tmp_path: Path) -> None:
    repo_a = tmp_path / "a"
    repo_a.mkdir()

    cfg = tmp_path / "repos.toml"
    _write_config(
        cfg,
        "\n".join(
            [
                "[repos.a_repo]",
                f'path = "{repo_a.as_posix()}"',
                'base_branch = "main"',
                'dirty_ignore_globs = [".pytest_cache/**"]',
                "dirty_cleanup = true",
                "",
            ]
        ),
    )

    inv = load_repo_inventory(cfg)
    policy = inv.repos["a_repo"]
    assert policy.dirty_ignore_globs == (".pytest_cache/**",)
    assert policy.dirty_cleanup is True
