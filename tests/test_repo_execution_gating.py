from __future__ import annotations

import json
import os
import stat
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from codex_orchestrator.contracts import ResolvedExecutionContract
from codex_orchestrator.paths import OrchestratorPaths
from codex_orchestrator.planner import RunDeck, RunDeckItem, ValidationResult, write_run_deck
from codex_orchestrator.repo_execution import DiffCaps, RepoExecutionConfig, TickBudget, execute_repo_tick
from codex_orchestrator.repo_inventory import RepoPolicy


def _make_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR)


def _write_tool(bin_dir: Path, name: str, lines: list[str]) -> None:
    path = bin_dir / name
    path.write_text("\n".join(lines), encoding="utf-8")
    _make_executable(path)


def _setup_fake_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_tool(
        bin_dir,
        "bd",
        [
            "#!/usr/bin/env python3",
            "from __future__ import annotations",
            "import json",
            "import sys",
            "from pathlib import Path",
            "DB = Path('.fake_beads.json')",
            "def load(): return json.loads(DB.read_text(encoding='utf-8'))",
            "def save(data): DB.write_text(json.dumps(data, indent=2, sort_keys=True)+'\\n', encoding='utf-8')",
            "def main(argv):",
            "  cmd = argv[1]",
            "  if cmd == 'show':",
            "    issue_id = argv[2]",
            "    print(json.dumps(load()[issue_id]))",
            "    return 0",
            "  if cmd == 'update':",
            "    issue_id = argv[2]",
            "    args = argv[3:]",
            "    status = None",
            "    notes = None",
            "    i = 0",
            "    while i < len(args):",
            "      if args[i] == '--status': status = args[i+1]; i += 2; continue",
            "      if args[i] == '--notes': notes = args[i+1]; i += 2; continue",
            "      if args[i] == '--json': i += 1; continue",
            "      i += 1",
            "    data = load()",
            "    issue = data[issue_id]",
            "    if status is not None: issue['status'] = status",
            "    if notes is not None: issue['notes'] = notes",
            "    data[issue_id] = issue",
            "    save(data)",
            "    print(json.dumps(issue))",
            "    return 0",
            "  if cmd == 'close':",
            "    issue_id = argv[2]",
            "    data = load()",
            "    issue = data[issue_id]",
            "    issue['status'] = 'closed'",
            "    data[issue_id] = issue",
            "    save(data)",
            "    print(json.dumps(issue))",
            "    return 0",
            "  if cmd == 'init': return 0",
            "  raise SystemExit(1)",
            "if __name__ == '__main__': raise SystemExit(main(sys.argv))",
        ],
    )
    _write_tool(
        bin_dir,
        "codex",
        [
            "#!/usr/bin/env python3",
            "from __future__ import annotations",
            "import sys",
            "from pathlib import Path",
            "def main(argv):",
            "  _ = sys.stdin.read()",
            "  Path('work.txt').write_text('hello\\n', encoding='utf-8')",
            "  return 0",
            "if __name__ == '__main__': raise SystemExit(main(sys.argv))",
        ],
    )
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))
    return bin_dir


def _git(repo_root: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo_root, check=True, capture_output=True, text=True)


def _setup_repo(tmp_path: Path) -> tuple[Path, RepoPolicy]:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / ".gitignore").write_text(".pytest_cache/\n", encoding="utf-8")
    (repo_root / "test_dummy.py").write_text("def test_ok():\n    assert True\n", encoding="utf-8")
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    policy = RepoPolicy(
        repo_id="test_repo",
        path=repo_root,
        base_branch="main",
        env=None,
        notebook_roots=(Path("."),),
        allowed_roots=(Path("."),),
        deny_roots=(),
        validation_commands=("pytest -q",),
        notebook_output_policy="strip",
    )
    return repo_root, policy


def _write_deck(paths: OrchestratorPaths, *, run_id: str, bead_ids: list[str]) -> None:
    contract = ResolvedExecutionContract(
        time_budget_minutes=10,
        validation_commands=("pytest -q",),
        env="test",
        allow_env_creation=False,
        requires_notebook_execution=False,
        allowed_roots=(Path("."),),
        deny_roots=(),
        notebook_roots=(Path("."),),
        notebook_output_policy="strip",
    )
    baseline = ValidationResult(
        command="pytest -q",
        exit_code=0,
        started_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
    )
    deck = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="test_repo",
        created_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        items=tuple(
            RunDeckItem(
                bead_id=bid,
                title=f"Bead {bid}",
                contract=contract,
                baseline_validation=(baseline,),
            )
            for bid in bead_ids
        ),
    )
    write_run_deck(paths, deck=deck)


def _write_fake_issues(repo_root: Path, bead_ids: list[str]) -> None:
    data: dict[str, object] = {}
    for bid in bead_ids:
        data[bid] = {
            "id": bid,
            "title": f"Bead {bid}",
            "status": "open",
            "notes": "",
            "dependencies": [],
            "dependents": [],
        }
    (repo_root / ".fake_beads.json").write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _git(repo_root, "add", ".fake_beads.json")
    _git(repo_root, "commit", "-m", "beads init")


def test_tick_time_remaining_rule_prevents_start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_fake_tools(tmp_path, monkeypatch)
    repo_root, policy = _setup_repo(tmp_path)
    _write_fake_issues(repo_root, ["bd-1"])

    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "20250101-000000-deadbeef"
    _write_deck(paths, run_id=run_id, bead_ids=["bd-1"])

    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(minutes=10))
    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=tmp_path / "unused_overlay.toml",
        tick=tick,
        config=RepoExecutionConfig(
            tick_budget=timedelta(minutes=10),
            min_minutes_to_start_new_bead=15,
            max_beads_per_tick=3,
            diff_caps=DiffCaps(max_files_changed=50, max_lines_added=500),
        ),
    )
    assert result.skipped is False
    assert result.beads_attempted == 0
    assert result.stop_reason == "tick_time_remaining"

    issues = json.loads((repo_root / ".fake_beads.json").read_text(encoding="utf-8"))
    assert issues["bd-1"]["status"] == "open"


def test_bead_cap_limits_attempts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _setup_fake_tools(tmp_path, monkeypatch)
    repo_root, policy = _setup_repo(tmp_path)
    _write_fake_issues(repo_root, ["bd-1", "bd-2"])

    paths = OrchestratorPaths(cache_dir=tmp_path / "cache")
    run_id = "20250101-000000-deadbeef"
    _write_deck(paths, run_id=run_id, bead_ids=["bd-1", "bd-2"])

    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(minutes=45))
    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=tmp_path / "unused_overlay.toml",
        tick=tick,
        config=RepoExecutionConfig(
            tick_budget=timedelta(minutes=45),
            min_minutes_to_start_new_bead=15,
            max_beads_per_tick=1,
            diff_caps=DiffCaps(max_files_changed=50, max_lines_added=500),
        ),
    )
    assert result.skipped is False
    assert result.beads_attempted == 1
    assert result.stop_reason == "bead_cap"

