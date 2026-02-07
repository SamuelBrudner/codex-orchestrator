from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _install_bd_stub(*, tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    bd_path = bin_dir / "bd"
    bd_path.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import os",
                "import sys",
                "",
                "def main() -> int:",
                "    args = sys.argv[1:]",
                "    cmd = args[0] if args else ''",
                "    if cmd == 'init':",
                "        return 0",
                "    if cmd == 'list' and '--json' in args:",
                "        sys.stdout.write(os.environ.get('BD_STUB_LIST_JSON', '[]'))",
                "        return 0",
                "    if cmd == 'ready' and '--json' in args:",
                "        sys.stdout.write(os.environ.get('BD_STUB_READY_JSON', '[]'))",
                "        return 0",
                "    sys.stderr.write('unsupported bd args: ' + ' '.join(args))",
                "    return 2",
                "",
                "if __name__ == '__main__':",
                "    raise SystemExit(main())",
                "",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(bd_path, 0o755)
    return bin_dir


def _install_failing_bd_stub(*, tmp_path: Path) -> Path:
    bin_dir = tmp_path / "bin-fail"
    bin_dir.mkdir(parents=True, exist_ok=True)
    bd_path = bin_dir / "bd"
    bd_path.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import sys",
                "",
                "def main() -> int:",
                "    sys.stderr.write('forced bd failure')",
                "    return 2",
                "",
                "if __name__ == '__main__':",
                "    raise SystemExit(main())",
                "",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(bd_path, 0o755)
    return bin_dir


def _run_cli(
    *,
    cwd: Path,
    args: list[str],
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get("PYTHONPATH", "")
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-m", "codex_orchestrator", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_init_repo_bootstraps_config_and_overlay(tmp_path: Path) -> None:
    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()

    bd_bin = _install_bd_stub(tmp_path=tmp_path)
    env = {
        "PATH": str(bd_bin) + os.pathsep + os.environ.get("PATH", ""),
        "BD_STUB_LIST_JSON": "[]",
        "BD_STUB_READY_JSON": "[]",
    }

    result = _run_cli(
        cwd=tmp_path,
        env_overrides=env,
        args=[
            "init-repo",
            "--repo-id",
            "test_repo",
            "--path",
            target_repo.as_posix(),
            "--env",
            "my_env",
            "--base-branch",
            "main",
            "--validation-command",
            "pytest -q",
        ],
    )
    assert result.returncode == 0
    assert "status=config_written" in result.stdout
    assert "status=ok overlay=config/bead_contracts/test_repo.toml" in result.stdout

    repos_path = tmp_path / "config" / "repos.toml"
    repos_text = repos_path.read_text(encoding="utf-8")
    assert "[repos.test_repo]" in repos_text
    assert f'path = "{target_repo.as_posix()}"' in repos_text
    assert 'base_branch = "main"' in repos_text
    assert 'env = "my_env"' in repos_text
    assert 'validation_commands = ["pytest -q"]' in repos_text

    overlay_path = tmp_path / "config" / "bead_contracts" / "test_repo.toml"
    assert overlay_path.exists()
    overlay_text = overlay_path.read_text(encoding="utf-8")
    assert "[defaults]" in overlay_text
    assert 'env = "my_env"' in overlay_text
    assert 'validation_commands = ["pytest -q"]' in overlay_text


def test_init_repo_existing_repo_id_requires_allow_existing(tmp_path: Path) -> None:
    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()

    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "repos.toml").write_text(
        "\n".join(
            [
                "[repos.test_repo]",
                f'path = "{target_repo.as_posix()}"',
                'base_branch = "main"',
                'env = "my_env"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = _run_cli(
        cwd=tmp_path,
        args=[
            "init-repo",
            "--repo-id",
            "test_repo",
            "--path",
            target_repo.as_posix(),
            "--env",
            "my_env",
            "--base-branch",
            "main",
        ],
    )
    assert result.returncode != 0
    assert "already exists" in result.stderr


def test_init_repo_allow_existing_bootstraps_overlay(tmp_path: Path) -> None:
    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()

    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "repos.toml").write_text(
        "\n".join(
            [
                "[repos.test_repo]",
                f'path = "{target_repo.as_posix()}"',
                'base_branch = "main"',
                'env = "my_env"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    bd_bin = _install_bd_stub(tmp_path=tmp_path)
    env = {
        "PATH": str(bd_bin) + os.pathsep + os.environ.get("PATH", ""),
        "BD_STUB_LIST_JSON": "[]",
        "BD_STUB_READY_JSON": "[]",
    }
    result = _run_cli(
        cwd=tmp_path,
        env_overrides=env,
        args=[
            "init-repo",
            "--repo-id",
            "test_repo",
            "--path",
            target_repo.as_posix(),
            "--env",
            "my_env",
            "--base-branch",
            "main",
            "--allow-existing",
        ],
    )
    assert result.returncode == 0
    assert "status=config_exists" in result.stdout

    repos_text = (tmp_path / "config" / "repos.toml").read_text(encoding="utf-8")
    assert repos_text.count("[repos.test_repo]") == 1

    overlay_path = tmp_path / "config" / "bead_contracts" / "test_repo.toml"
    assert overlay_path.exists()


def test_init_repo_rolls_back_config_when_overlay_bootstrap_fails(tmp_path: Path) -> None:
    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()

    failing_bd_bin = _install_failing_bd_stub(tmp_path=tmp_path)
    env = {
        "PATH": str(failing_bd_bin) + os.pathsep + os.environ.get("PATH", ""),
    }

    result = _run_cli(
        cwd=tmp_path,
        env_overrides=env,
        args=[
            "init-repo",
            "--repo-id",
            "test_repo",
            "--path",
            target_repo.as_posix(),
            "--env",
            "my_env",
            "--base-branch",
            "main",
        ],
    )
    assert result.returncode != 0
    assert "status=rolled_back" in result.stdout

    repos_path = tmp_path / "config" / "repos.toml"
    assert not repos_path.exists()
