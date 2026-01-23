from __future__ import annotations

import logging
import subprocess
from collections.abc import Sequence
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


@dataclass(frozen=True)
class EnvRefreshResult:
    env_name: str
    env_existed: bool
    env_created: bool
    env_files: tuple[str, ...]
    requirements_files: tuple[str, ...]
    conda_update_attempted: bool
    conda_update_succeeded: bool
    pip_install_attempted: bool
    pip_install_succeeded: bool
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


def _conda_env_update(
    env_name: str,
    *,
    env_file: Path,
    timeout_seconds: float = 600.0,
) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["conda", "env", "update", "-n", env_name, "-f", env_file.as_posix()],
            cwd=env_file.parent,
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
        return False, f"conda env update timed out after {timeout_seconds}s"
    except Exception as e:
        return False, str(e)


def _pip_install_requirements(
    env_name: str,
    *,
    requirements_file: Path,
    timeout_seconds: float = 300.0,
) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            [
                "conda",
                "run",
                "-n",
                env_name,
                "python",
                "-m",
                "pip",
                "install",
                "-r",
                requirements_file.as_posix(),
            ],
            cwd=requirements_file.parent,
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
        return False, f"pip install -r timed out after {timeout_seconds}s"
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


def refresh_repo_env(
    *,
    env_name: str,
    repo_root: Path,
    allow_env_creation: bool,
    env_files: Sequence[Path] = (),
    requirements_files: Sequence[Path] = (),
    pip_editable: bool = False,
) -> EnvRefreshResult:
    env_existed = _conda_env_exists(env_name)
    env_created = False
    conda_update_attempted = False
    conda_update_succeeded = True
    pip_install_attempted = False
    pip_install_succeeded = True
    error: str | None = None

    if not env_existed:
        if not allow_env_creation:
            error = (
                f"Conda env {env_name!r} does not exist and allow_env_creation=False. "
                "Create the env manually or set allow_env_creation=true in the contract overlay."
            )
            return EnvRefreshResult(
                env_name=env_name,
                env_existed=False,
                env_created=False,
                env_files=tuple(p.as_posix() for p in env_files),
                requirements_files=tuple(p.as_posix() for p in requirements_files),
                conda_update_attempted=False,
                conda_update_succeeded=False,
                pip_install_attempted=False,
                pip_install_succeeded=False,
                error=error,
            )
        logger.info("Creating conda env %s (allow_env_creation=True)", env_name)
        created, create_error = _create_conda_env(env_name)
        if not created:
            error = f"Failed to create conda env {env_name!r}: {create_error}"
            return EnvRefreshResult(
                env_name=env_name,
                env_existed=False,
                env_created=False,
                env_files=tuple(p.as_posix() for p in env_files),
                requirements_files=tuple(p.as_posix() for p in requirements_files),
                conda_update_attempted=False,
                conda_update_succeeded=False,
                pip_install_attempted=False,
                pip_install_succeeded=False,
                error=error,
            )
        env_created = True

    if env_files:
        conda_update_attempted = True
        for env_file in env_files:
            ok, update_error = _conda_env_update(env_name, env_file=env_file)
            if not ok:
                error = f"Failed conda env update for {env_file}: {update_error}"
                conda_update_succeeded = False
                return EnvRefreshResult(
                    env_name=env_name,
                    env_existed=env_existed,
                    env_created=env_created,
                    env_files=tuple(p.as_posix() for p in env_files),
                    requirements_files=tuple(p.as_posix() for p in requirements_files),
                    conda_update_attempted=True,
                    conda_update_succeeded=False,
                    pip_install_attempted=False,
                    pip_install_succeeded=False,
                    error=error,
                )

    if requirements_files:
        pip_install_attempted = True
        for req_file in requirements_files:
            ok, install_error = _pip_install_requirements(env_name, requirements_file=req_file)
            if not ok:
                error = f"Failed pip install -r {req_file}: {install_error}"
                pip_install_succeeded = False
                return EnvRefreshResult(
                    env_name=env_name,
                    env_existed=env_existed,
                    env_created=env_created,
                    env_files=tuple(p.as_posix() for p in env_files),
                    requirements_files=tuple(p.as_posix() for p in requirements_files),
                    conda_update_attempted=conda_update_attempted,
                    conda_update_succeeded=conda_update_succeeded,
                    pip_install_attempted=True,
                    pip_install_succeeded=False,
                    error=error,
                )

    if pip_editable:
        pip_install_attempted = True
        ok, install_error = _install_repo_editable(env_name, repo_root)
        if not ok:
            error = f"Failed to install repo editable in env {env_name!r}: {install_error}"
            pip_install_succeeded = False
            return EnvRefreshResult(
                env_name=env_name,
                env_existed=env_existed,
                env_created=env_created,
                env_files=tuple(p.as_posix() for p in env_files),
                requirements_files=tuple(p.as_posix() for p in requirements_files),
                conda_update_attempted=conda_update_attempted,
                conda_update_succeeded=conda_update_succeeded,
                pip_install_attempted=True,
                pip_install_succeeded=False,
                error=error,
            )

    return EnvRefreshResult(
        env_name=env_name,
        env_existed=env_existed,
        env_created=env_created,
        env_files=tuple(p.as_posix() for p in env_files),
        requirements_files=tuple(p.as_posix() for p in requirements_files),
        conda_update_attempted=conda_update_attempted,
        conda_update_succeeded=conda_update_succeeded,
        pip_install_attempted=pip_install_attempted,
        pip_install_succeeded=pip_install_succeeded,
        error=error,
    )
