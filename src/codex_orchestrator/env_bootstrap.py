from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


class EnvBootstrapError(RuntimeError):
    pass


@dataclass(frozen=True)
class BootstrapResult:
    env_name: str
    env_existed: bool
    env_created: bool
    repo_installed: bool
    install_attempted: bool
    install_succeeded: bool
    error: str | None = None


def _conda_env_exists(env_name: str) -> bool:
    try:
        result = subprocess.run(
            ["conda", "env", "list", "--json"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30.0,
        )
        if result.returncode != 0:
            return False
        import json
        data = json.loads(result.stdout)
        envs = data.get("envs", [])
        return any(
            env_path.endswith(f"/envs/{env_name}") or env_path.endswith(f"\\envs\\{env_name}")
            for env_path in envs
        )
    except Exception:
        return False


def _create_conda_env(env_name: str, *, timeout_seconds: float = 300.0) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["conda", "create", "-n", env_name, "python=3.10", "-y"],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        if result.returncode == 0:
            return True, ""
        error_msg = (result.stderr or result.stdout or "Unknown error").strip()
        return False, error_msg
    except subprocess.TimeoutExpired:
        return False, f"conda create timed out after {timeout_seconds}s"
    except Exception as e:
        return False, str(e)


def _install_repo_editable(
    env_name: str,
    repo_root: Path,
    *,
    timeout_seconds: float = 300.0,
) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["conda", "run", "-n", env_name, "python", "-m", "pip", "install", "-e", "."],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        if result.returncode == 0:
            return True, ""
        error_msg = (result.stderr or result.stdout or "Unknown error").strip()
        return False, error_msg
    except subprocess.TimeoutExpired:
        return False, f"pip install timed out after {timeout_seconds}s"
    except Exception as e:
        return False, str(e)


def bootstrap_repo_env(
    *,
    env_name: str,
    repo_root: Path,
    allow_env_creation: bool,
) -> BootstrapResult:
    env_existed = _conda_env_exists(env_name)
    env_created = False
    install_attempted = False
    install_succeeded = False
    error: str | None = None

    if not env_existed:
        if not allow_env_creation:
            error = (
                f"Conda env {env_name!r} does not exist and allow_env_creation=False. "
                "Create the env manually or set allow_env_creation=true in the contract overlay."
            )
            return BootstrapResult(
                env_name=env_name,
                env_existed=False,
                env_created=False,
                repo_installed=False,
                install_attempted=False,
                install_succeeded=False,
                error=error,
            )
        logger.info("Creating conda env %s (allow_env_creation=True)", env_name)
        created, create_error = _create_conda_env(env_name)
        if not created:
            error = f"Failed to create conda env {env_name!r}: {create_error}"
            return BootstrapResult(
                env_name=env_name,
                env_existed=False,
                env_created=False,
                repo_installed=False,
                install_attempted=False,
                install_succeeded=False,
                error=error,
            )
        env_created = True
        logger.info("Created conda env %s", env_name)

    logger.info("Installing repo editable in env %s from %s", env_name, repo_root)
    install_attempted = True
    install_succeeded, install_error = _install_repo_editable(env_name, repo_root)

    if not install_succeeded:
        error = f"Failed to install repo editable in env {env_name!r}: {install_error}"
        logger.warning("%s", error)

    return BootstrapResult(
        env_name=env_name,
        env_existed=env_existed,
        env_created=env_created,
        repo_installed=install_succeeded,
        install_attempted=install_attempted,
        install_succeeded=install_succeeded,
        error=error,
    )
