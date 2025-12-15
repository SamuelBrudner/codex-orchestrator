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


def _write_repos_config(*, tmp_path: Path, repo_root: Path) -> None:
    config_dir = tmp_path / "config"
    (config_dir / "bead_contracts").mkdir(parents=True, exist_ok=True)
    (config_dir / "repos.toml").write_text(
        "\n".join(
            [
                "[repos.test_repo]",
                f'path = "{repo_root.as_posix()}"',
                'base_branch = "main"',
                'env = "my_env"',
                'validation_commands = ["pytest -q"]',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _run_cli(
    *,
    cwd: Path,
    env_overrides: dict[str, str] | None = None,
    args: list[str],
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


def test_overlay_dry_run_missing_overlay_exits_nonzero(tmp_path: Path) -> None:
    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()
    _write_repos_config(tmp_path=tmp_path, repo_root=target_repo)

    bd_bin = _install_bd_stub(tmp_path=tmp_path)
    env = {
        "PATH": str(bd_bin) + os.pathsep + os.environ.get("PATH", ""),
        "BD_STUB_LIST_JSON": "[]",
        "BD_STUB_READY_JSON": "[]",
    }
    result = _run_cli(
        cwd=tmp_path,
        env_overrides=env,
        args=["overlay", "dry-run", "--repo-id", "test_repo"],
    )
    assert result.returncode != 0
    assert "missing_overlay" in result.stdout
    assert "next_action" in result.stdout


def test_overlay_apply_writes_file_and_dry_run_ok(tmp_path: Path) -> None:
    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()
    _write_repos_config(tmp_path=tmp_path, repo_root=target_repo)

    bd_bin = _install_bd_stub(tmp_path=tmp_path)
    env = {
        "PATH": str(bd_bin) + os.pathsep + os.environ.get("PATH", ""),
        "BD_STUB_LIST_JSON": '[{"id": "bd-1"}]',
        "BD_STUB_READY_JSON": '[{"id": "bd-1", "title": "Test bead"}]',
    }

    apply_result = _run_cli(
        cwd=tmp_path,
        env_overrides=env,
        args=["overlay", "apply", "--repo-id", "test_repo"],
    )
    assert apply_result.returncode == 0

    overlay_path = tmp_path / "config" / "bead_contracts" / "test_repo.toml"
    assert overlay_path.exists()
    overlay_text = overlay_path.read_text(encoding="utf-8")
    assert "[defaults]" in overlay_text
    assert "time_budget_minutes" in overlay_text
    assert "allow_env_creation" in overlay_text
    assert "requires_notebook_execution" in overlay_text

    dry_run_result = _run_cli(
        cwd=tmp_path,
        env_overrides=env,
        args=["overlay", "dry-run", "--repo-id", "test_repo"],
    )
    assert dry_run_result.returncode == 0
    assert "status=ok" in dry_run_result.stdout


def test_overlay_unknown_bead_id_fails_loudly(tmp_path: Path) -> None:
    target_repo = tmp_path / "target_repo"
    target_repo.mkdir()
    _write_repos_config(tmp_path=tmp_path, repo_root=target_repo)

    overlay_path = tmp_path / "config" / "bead_contracts" / "test_repo.toml"
    overlay_path.write_text(
        "\n".join(
            [
                "[defaults]",
                "time_budget_minutes = 45",
                'env = "my_env"',
                "allow_env_creation = false",
                "requires_notebook_execution = false",
                "",
                '[beads."bd-UNKNOWN"]',
                "time_budget_minutes = 60",
                "",
            ]
        ),
        encoding="utf-8",
    )

    bd_bin = _install_bd_stub(tmp_path=tmp_path)
    env = {
        "PATH": str(bd_bin) + os.pathsep + os.environ.get("PATH", ""),
        "BD_STUB_LIST_JSON": '[{"id": "bd-1"}]',
        "BD_STUB_READY_JSON": '[{"id": "bd-1", "title": "Test bead"}]',
    }
    result = _run_cli(
        cwd=tmp_path,
        env_overrides=env,
        args=["overlay", "dry-run", "--repo-id", "test_repo"],
    )
    assert result.returncode != 0
    assert "unknown bead id" in result.stdout
