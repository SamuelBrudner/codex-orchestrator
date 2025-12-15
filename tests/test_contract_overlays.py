from __future__ import annotations

from pathlib import Path

import pytest

from codex_orchestrator.contract_overlays import ContractOverlayError, load_contract_overlay
from codex_orchestrator.repo_inventory import RepoPolicy


def _write_overlay(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _policy(
    *,
    tmp_path: Path,
    allowed_roots: tuple[Path, ...] = (Path("."),),
    deny_roots: tuple[Path, ...] = (),
) -> RepoPolicy:
    return RepoPolicy(
        repo_id="test_repo",
        path=tmp_path,
        base_branch="main",
        env=None,
        notebook_roots=(Path("."),),
        allowed_roots=allowed_roots,
        deny_roots=deny_roots,
        validation_commands=(),
        notebook_output_policy="strip",
    )


def test_valid_overlay_passes(tmp_path: Path) -> None:
    overlay = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay,
        "\n".join(
            [
                "[defaults]",
                "time_budget_minutes = 45",
                'validation_commands = ["pytest -q"]',
                'env = "my_env"',
                "allow_env_creation = false",
                "requires_notebook_execution = false",
                'allowed_roots = ["src"]',
                'deny_roots = ["data"]',
                "",
                '[beads."bd-1"]',
                "time_budget_minutes = 60",
                'allowed_roots = ["src/module"]',
                "",
            ]
        ),
    )

    load_contract_overlay(
        overlay,
        repo_policy=_policy(
            tmp_path=tmp_path,
            allowed_roots=(Path("."),),
            deny_roots=(Path("data"),),
        ),
        known_bead_ids={"bd-1"},
    )


def test_unknown_bead_id_fails(tmp_path: Path) -> None:
    overlay = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay,
        "\n".join(
            [
                "[defaults]",
                "time_budget_minutes = 45",
                "",
                '[beads."bd-2"]',
                "time_budget_minutes = 60",
                "",
            ]
        ),
    )

    with pytest.raises(ContractOverlayError) as excinfo:
        load_contract_overlay(
            overlay,
            repo_policy=_policy(tmp_path=tmp_path),
            known_bead_ids={"bd-1"},
        )
    assert "unknown bead id" in str(excinfo.value)


def test_allowed_roots_expansion_fails(tmp_path: Path) -> None:
    overlay = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay,
        "\n".join(
            [
                "[defaults]",
                'allowed_roots = ["."]',
                "",
            ]
        ),
    )

    with pytest.raises(ContractOverlayError) as excinfo:
        load_contract_overlay(
            overlay,
            repo_policy=_policy(tmp_path=tmp_path, allowed_roots=(Path("src"),)),
            known_bead_ids=set(),
        )
    assert "may only narrow repo policy" in str(excinfo.value)


def test_deny_roots_relaxation_fails(tmp_path: Path) -> None:
    overlay = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay,
        "\n".join(
            [
                "[defaults]",
                'deny_roots = ["data/raw"]',
                "",
            ]
        ),
    )

    with pytest.raises(ContractOverlayError) as excinfo:
        load_contract_overlay(
            overlay,
            repo_policy=_policy(tmp_path=tmp_path, deny_roots=(Path("data"),)),
            known_bead_ids=set(),
        )
    assert "may not relax repo policy" in str(excinfo.value)
