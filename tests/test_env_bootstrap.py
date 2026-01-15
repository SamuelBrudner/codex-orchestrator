from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from codex_orchestrator.env_bootstrap import (
    BootstrapResult,
    bootstrap_repo_env,
)


@pytest.fixture
def mock_repo_root(tmp_path: Path) -> Path:
    repo = tmp_path / "test_repo"
    repo.mkdir()
    (repo / "setup.py").write_text("from setuptools import setup\nsetup(name='test')")
    return repo


def test_bootstrap_env_exists_and_install_succeeds(mock_repo_root: Path) -> None:
    with patch("codex_orchestrator.env_bootstrap._conda_env_exists") as mock_exists, patch(
        "codex_orchestrator.env_bootstrap._install_repo_editable"
    ) as mock_install:
        mock_exists.return_value = True
        mock_install.return_value = (True, "")

        result = bootstrap_repo_env(
            env_name="test_env",
            repo_root=mock_repo_root,
            allow_env_creation=True,
        )

        assert result.env_name == "test_env"
        assert result.env_existed is True
        assert result.env_created is False
        assert result.repo_installed is True
        assert result.install_attempted is True
        assert result.install_succeeded is True
        assert result.error is None

        mock_exists.assert_called_once_with("test_env")
        mock_install.assert_called_once_with("test_env", mock_repo_root)


def test_bootstrap_env_does_not_exist_creation_allowed(mock_repo_root: Path) -> None:
    with patch("codex_orchestrator.env_bootstrap._conda_env_exists") as mock_exists, patch(
        "codex_orchestrator.env_bootstrap._create_conda_env"
    ) as mock_create, patch("codex_orchestrator.env_bootstrap._install_repo_editable") as mock_install:
        mock_exists.return_value = False
        mock_create.return_value = (True, "")
        mock_install.return_value = (True, "")

        result = bootstrap_repo_env(
            env_name="test_env",
            repo_root=mock_repo_root,
            allow_env_creation=True,
        )

        assert result.env_name == "test_env"
        assert result.env_existed is False
        assert result.env_created is True
        assert result.repo_installed is True
        assert result.install_attempted is True
        assert result.install_succeeded is True
        assert result.error is None

        mock_exists.assert_called_once_with("test_env")
        mock_create.assert_called_once_with("test_env")
        mock_install.assert_called_once_with("test_env", mock_repo_root)


def test_bootstrap_env_does_not_exist_creation_not_allowed(mock_repo_root: Path) -> None:
    with patch("codex_orchestrator.env_bootstrap._conda_env_exists") as mock_exists:
        mock_exists.return_value = False

        result = bootstrap_repo_env(
            env_name="test_env",
            repo_root=mock_repo_root,
            allow_env_creation=False,
        )

        assert result.env_name == "test_env"
        assert result.env_existed is False
        assert result.env_created is False
        assert result.repo_installed is False
        assert result.install_attempted is False
        assert result.install_succeeded is False
        assert result.error is not None
        assert "does not exist" in result.error
        assert "allow_env_creation=False" in result.error


def test_bootstrap_env_creation_fails(mock_repo_root: Path) -> None:
    with patch("codex_orchestrator.env_bootstrap._conda_env_exists") as mock_exists, patch(
        "codex_orchestrator.env_bootstrap._create_conda_env"
    ) as mock_create:
        mock_exists.return_value = False
        mock_create.return_value = (False, "conda create failed: some error")

        result = bootstrap_repo_env(
            env_name="test_env",
            repo_root=mock_repo_root,
            allow_env_creation=True,
        )

        assert result.env_name == "test_env"
        assert result.env_existed is False
        assert result.env_created is False
        assert result.repo_installed is False
        assert result.install_attempted is False
        assert result.install_succeeded is False
        assert result.error is not None
        assert "Failed to create conda env" in result.error


def test_bootstrap_install_fails(mock_repo_root: Path) -> None:
    with patch("codex_orchestrator.env_bootstrap._conda_env_exists") as mock_exists, patch(
        "codex_orchestrator.env_bootstrap._install_repo_editable"
    ) as mock_install:
        mock_exists.return_value = True
        mock_install.return_value = (False, "ModuleNotFoundError: No module named 'setuptools'")

        result = bootstrap_repo_env(
            env_name="test_env",
            repo_root=mock_repo_root,
            allow_env_creation=True,
        )

        assert result.env_name == "test_env"
        assert result.env_existed is True
        assert result.env_created is False
        assert result.repo_installed is False
        assert result.install_attempted is True
        assert result.install_succeeded is False
        assert result.error is not None
        assert "Failed to install repo editable" in result.error


def test_conda_env_exists_integration() -> None:
    from codex_orchestrator.env_bootstrap import _conda_env_exists

    with patch("subprocess.run") as mock_run:
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(
            {"envs": ["/Users/test/miniconda3/envs/test_env", "/Users/test/miniconda3/envs/other"]}
        )
        mock_run.return_value = mock_result

        assert _conda_env_exists("test_env") is True
        assert _conda_env_exists("other") is True
        assert _conda_env_exists("nonexistent") is False


def test_conda_env_exists_handles_errors() -> None:
    from codex_orchestrator.env_bootstrap import _conda_env_exists

    with patch("subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired("conda", 30.0)
        assert _conda_env_exists("test_env") is False

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_run.side_effect = None
        mock_run.return_value = mock_result
        assert _conda_env_exists("test_env") is False
