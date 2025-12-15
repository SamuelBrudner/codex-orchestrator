from __future__ import annotations

from pathlib import Path

import pytest

from codex_orchestrator.contract_overlays import load_contract_overlay
from codex_orchestrator.contracts import ContractResolutionError, resolve_execution_contract
from codex_orchestrator.repo_inventory import RepoPolicy


def _write_overlay(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _policy(*, tmp_path: Path) -> RepoPolicy:
    return RepoPolicy(
        repo_id="test_repo",
        path=tmp_path,
        base_branch="main",
        env="repo_env",
        notebook_roots=(Path("notebooks"),),
        allowed_roots=(Path("."),),
        deny_roots=(Path("data"),),
        validation_commands=("pytest -q",),
        notebook_output_policy="strip",
    )


def test_resolve_contract_precedence_and_snapshot(tmp_path: Path) -> None:
    overlay_path = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay_path,
        "\n".join(
            [
                "[defaults]",
                "time_budget_minutes = 45",
                'validation_commands = ["pytest -q", "python -m compileall"]',
                'env = "default_env"',
                "allow_env_creation = false",
                "requires_notebook_execution = false",
                "",
                '[beads."bd-1"]',
                "time_budget_minutes = 60",
                'validation_commands = ["ruff check"]',
                'env = "bead_env"',
                "allow_env_creation = true",
                "requires_notebook_execution = true",
                'allowed_roots = ["src"]',
                'deny_roots = ["data"]',
                "",
            ]
        ),
    )

    policy = _policy(tmp_path=tmp_path)
    overlay = load_contract_overlay(overlay_path, repo_policy=policy, known_bead_ids={"bd-1"})

    contract = resolve_execution_contract(
        repo_policy=policy,
        overlay=overlay,
        bead_id="bd-1",
        overlay_path=overlay_path,
    )

    assert contract.time_budget_minutes == 60
    assert contract.env == "bead_env"
    assert contract.allow_env_creation is True
    assert contract.requires_notebook_execution is True
    assert contract.allowed_roots == (Path("src"),)
    assert contract.deny_roots == (Path("data"),)
    assert contract.notebook_roots == (Path("notebooks"),)
    assert contract.notebook_output_policy == "strip"

    # Validation commands are additive and de-duplicated (policy + defaults + bead-specific).
    assert contract.validation_commands == ("pytest -q", "python -m compileall", "ruff check")
    assert contract.to_json_dict()["validation_commands"] == [
        "pytest -q",
        "python -m compileall",
        "ruff check",
    ]


def test_missing_required_fields_error_is_actionable(tmp_path: Path) -> None:
    overlay_path = tmp_path / "test_repo.toml"
    _write_overlay(
        overlay_path,
        "\n".join(
            [
                "[defaults]",
                'env = "default_env"',
                "",
            ]
        ),
    )

    policy = _policy(tmp_path=tmp_path)
    overlay = load_contract_overlay(overlay_path, repo_policy=policy, known_bead_ids={"bd-1"})

    with pytest.raises(ContractResolutionError) as excinfo:
        resolve_execution_contract(
            repo_policy=policy,
            overlay=overlay,
            bead_id="bd-1",
            overlay_path=overlay_path,
        )

    message = str(excinfo.value)
    assert "missing allow_env_creation" in message or "allow_env_creation" in message
    assert "requires_notebook_execution" in message
    assert "time_budget_minutes" in message
    assert overlay_path.as_posix() in message

