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
from codex_orchestrator.repo_execution import (
    DiffCaps,
    RepoExecutionConfig,
    TickBudget,
    execute_repo_tick,
)
from codex_orchestrator.repo_inventory import RepoPolicy


def _make_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR)


def _write_fake_bd(bin_dir: Path) -> None:
    script = bin_dir / "bd"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "from __future__ import annotations",
                "import json",
                "import sys",
                "from pathlib import Path",
                "",
                "DB = Path('.fake_beads.json')",
                "",
                "def load():",
                "    return json.loads(DB.read_text(encoding='utf-8'))",
                "",
                "def save(data):",
                "    DB.write_text(",
                "        json.dumps(data, indent=2, sort_keys=True) + '\\n',",
                "        encoding='utf-8',",
                "    )",
                "",
                "def main(argv):",
                "    if len(argv) < 2:",
                "        raise SystemExit(2)",
                "    cmd = argv[1]",
                "    if cmd == 'show':",
                "        issue_id = argv[2]",
                "        data = load()",
                "        issue = data[issue_id]",
                "        print(json.dumps(issue))",
                "        return 0",
                "    if cmd == 'update':",
                "        issue_id = argv[2]",
                "        args = argv[3:]",
                "        status = None",
                "        notes = None",
                "        i = 0",
                "        while i < len(args):",
                "            if args[i] == '--status':",
                "                status = args[i+1]",
                "                i += 2",
                "                continue",
                "            if args[i] == '--notes':",
                "                notes = args[i+1]",
                "                i += 2",
                "                continue",
                "            if args[i] == '--json':",
                "                i += 1",
                "                continue",
                "            i += 1",
                "        data = load()",
                "        issue = data[issue_id]",
                "        if status is not None:",
                "            issue['status'] = status",
                "        if notes is not None:",
                "            issue['notes'] = notes",
                "        data[issue_id] = issue",
                "        save(data)",
                "        print(json.dumps(issue))",
                "        return 0",
                "    if cmd == 'close':",
                "        issue_id = argv[2]",
                "        data = load()",
                "        issue = data[issue_id]",
                "        issue['status'] = 'closed'",
                "        data[issue_id] = issue",
                "        save(data)",
                "        print(json.dumps(issue))",
                "        return 0",
                "    if cmd == 'init':",
                "        return 0",
                "    raise SystemExit(1)",
                "",
                "if __name__ == '__main__':",
                "    raise SystemExit(main(sys.argv))",
            ]
        ),
        encoding="utf-8",
    )
    _make_executable(script)


def _write_fake_codex(bin_dir: Path) -> None:
    script = bin_dir / "codex"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "from __future__ import annotations",
                "import json",
                "import os",
                "import sys",
                "from pathlib import Path",
                "",
                "def main(argv):",
                "    Path('.fake_codex_argv.json').write_text(",
                "        json.dumps(argv) + '\\n',",
                "        encoding='utf-8',",
                "    )",
                "    # Minimal stub: capture prompt from stdin and create a file to commit.",
                "    prompt = sys.stdin.read()",
                "    Path('.fake_codex_prompt.txt').write_text(prompt, encoding='utf-8')",
                "    if os.environ.get('FAKE_CODEX_NO_CHANGES') != '1':",
                "        Path('work.txt').write_text('hello\\n', encoding='utf-8')",
                "    if os.environ.get('FAKE_CODEX_EDIT_TEST_FILE') == '1':",
                "        p = Path('test_dummy.py')",
                "        text = p.read_text(encoding='utf-8') if p.exists() else ''",
                "        if os.environ.get('FAKE_CODEX_ADD_GWT') == '1':",
                "            markers = '# Given\\n# When\\n# Then\\n'",
                "            if markers not in text:",
                "                text = markers + text",
                "        if not text.endswith('\\n'):",
                "            text += '\\n'",
                "        text += '# touched\\n'",
                "        p.write_text(text, encoding='utf-8')",
                "    if os.environ.get('FAKE_CODEX_EDIT_PYPROJECT') == '1':",
                "        p = Path('pyproject.toml')",
                "        text = p.read_text(encoding='utf-8') if p.exists() else ''",
                "        if not text.endswith('\\n'):",
                "            text += '\\n'",
                "        text += '# dep touch\\n'",
                "        p.write_text(text, encoding='utf-8')",
                "    if os.environ.get('FAKE_CODEX_EDIT_ENV') == '1':",
                "        p = Path('environment.yml')",
                "        text = p.read_text(encoding='utf-8') if p.exists() else ''",
                "        if not text.endswith('\\n'):",
                "            text += '\\n'",
                "        text += '# dep touch\\n'",
                "        p.write_text(text, encoding='utf-8')",
                "    print('ok')",
                "    return 0",
                "",
                "if __name__ == '__main__':",
                "    raise SystemExit(main(sys.argv))",
            ]
        ),
        encoding="utf-8",
    )
    _make_executable(script)


def _write_fake_conda(bin_dir: Path) -> None:
    script = bin_dir / "conda"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "from __future__ import annotations",
                "import json",
                "import os",
                "import subprocess",
                "import sys",
                "from pathlib import Path",
                "",
                "def _log(argv):",
                "    path = os.environ.get('FAKE_CONDA_LOG')",
                "    if not path:",
                "        return",
                "    p = Path(path)",
                "    p.parent.mkdir(parents=True, exist_ok=True)",
                "    with p.open('a', encoding='utf-8') as f:",
                "        f.write(json.dumps(argv) + '\\n')",
                "",
                "def main(argv):",
                "    _log(argv)",
                "    if '--version' in argv[1:]:",
                "        print('conda 0.0.0')",
                "        return 0",
                "    if len(argv) >= 3 and argv[1] == 'env' and argv[2] == 'list':",
                "        print(json.dumps({'envs': ['/fake/envs/test']}))",
                "        return 0",
                "    if len(argv) >= 3 and argv[1] == 'env' and argv[2] == 'update':",
                "        return 0",
                "    if len(argv) >= 2 and argv[1] == 'create':",
                "        return 0",
                "    if len(argv) >= 2 and argv[1] == 'run':",
                "        args = argv[2:]",
                "        i = 0",
                "        while i < len(args):",
                "            if args[i] == '-n':",
                "                i += 2",
                "                continue",
                "            if args[i] == '--no-capture-output':",
                "                i += 1",
                "                continue",
                "            if args[i] == '--':",
                "                i += 1",
                "                break",
                "            if args[i].startswith('-'):",
                "                i += 1",
                "                continue",
                "            break",
                "        cmd = args[i:]",
                "        if not cmd:",
                "            return 2",
                "        if os.environ.get('FAKE_CONDA_FAIL_PYTEST') == '1' and cmd[0] == 'pytest':",
                "            return 1",
                "        if cmd[:4] == ['python', '-m', 'pip', 'install']:",
                "            return 0",
                "        completed = subprocess.run(cmd, check=False)",
                "        return int(completed.returncode)",
                "    return 1",
                "",
                "if __name__ == '__main__':",
                "    raise SystemExit(main(sys.argv))",
            ]
        ),
        encoding="utf-8",
    )
    _make_executable(script)


def _git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return (completed.stdout or "").strip()


def test_execute_repo_tick_closes_bead_and_updates_dependents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / ".gitignore").write_text(".pytest_cache/\n", encoding="utf-8")
    (repo_root / "test_dummy.py").write_text(
        "def test_ok():\n    assert 1 + 1 == 2\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    # Fake Beads issue database in the target repo.
    (repo_root / ".fake_beads.json").write_text(
        json.dumps(
            {
                "bd-1": {
                    "id": "bd-1",
                    "title": "Test bead",
                    "status": "open",
                    "notes": "",
                    "dependencies": [],
                    "dependents": [{"id": "bd-2"}],
                },
                "bd-2": {
                    "id": "bd-2",
                    "title": "Downstream bead",
                    "status": "open",
                    "notes": "",
                    "dependencies": [{"id": "bd-1"}],
                    "dependents": [],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", ".fake_beads.json")
    _git(repo_root, "commit", "-m", "beads init")

    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250101-000000-deadbeef"

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
        stdout="",
        stderr="",
    )
    deck = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="test_repo",
        created_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        items=(
            RunDeckItem(
                bead_id="bd-1",
                title="Test bead",
                contract=contract,
                baseline_validation=(baseline,),
            ),
        ),
    )
    write_run_deck(paths, deck=deck)

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

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_bd(bin_dir)
    _write_fake_codex(bin_dir)
    _write_fake_conda(bin_dir)

    conda_log = tmp_path / "conda_argv.jsonl"
    monkeypatch.setenv("FAKE_CONDA_LOG", conda_log.as_posix())
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))

    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(seconds=30))
    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=tmp_path / "unused_overlay.toml",
        tick=tick,
        config=RepoExecutionConfig(
            tick_budget=timedelta(seconds=30),
            min_minutes_to_start_new_bead=0,
            max_beads_per_tick=3,
            diff_caps=DiffCaps(max_files_changed=10, max_lines_added=100),
        ),
    )
    assert result.skipped is False
    assert result.beads_closed == 1
    assert result.branch == f"run/{run_id}"

    assert paths.repo_summary_path(run_id, "test_repo").exists()
    assert paths.repo_stdout_log_path(run_id, "test_repo").exists()
    assert paths.repo_stderr_log_path(run_id, "test_repo").exists()
    assert paths.repo_events_path(run_id, "test_repo").exists()
    assert paths.run_summary_path(run_id).exists()
    assert (repo_root / "docs" / "runs" / f"{run_id}.md").exists()

    codex_argv = json.loads((repo_root / ".fake_codex_argv.json").read_text(encoding="utf-8"))
    assert "--full-auto" in codex_argv
    assert "--model" in codex_argv
    assert codex_argv[codex_argv.index("--model") + 1] == "gpt-5.3-codex"
    assert "-c" in codex_argv
    assert codex_argv[codex_argv.index("-c") + 1] == 'reasoning_effort="xhigh"'

    codex_prompt = (repo_root / ".fake_codex_prompt.txt").read_text(encoding="utf-8")
    assert "Style:" in codex_prompt
    assert "avoid deep nesting" in codex_prompt
    assert "DataFrame.query" in codex_prompt
    assert "sns.someplot(data=df.query" in codex_prompt

    report_text = (repo_root / "docs" / "runs" / f"{run_id}.md").read_text(encoding="utf-8")
    assert "## Aims and Design Rationale" in report_text
    assert "## Planning Audit" in report_text
    audit_json_rel = f"runs/{run_id}/test_repo.planning_audit.json"
    audit_md_rel = f"runs/{run_id}/test_repo.planning_audit.md"
    assert f"`{audit_json_rel}`" in report_text
    assert f"`{audit_md_rel}`" in report_text
    assert "(missing)" in report_text
    assert "## AI Configuration" in report_text
    assert "- Model: `gpt-5.3-codex`" in report_text
    assert "- Reasoning effort: `xhigh`" in report_text
    assert "reasoning_effort=\"xhigh\"" in report_text

    summary = json.loads(paths.repo_summary_path(run_id, "test_repo").read_text(encoding="utf-8"))
    planning_audit = summary.get("planning_audit")
    assert isinstance(planning_audit, dict)
    expected_json_path = paths.repo_planning_audit_json_path(run_id, "test_repo").as_posix()
    expected_md_path = paths.repo_planning_audit_md_path(run_id, "test_repo").as_posix()
    assert planning_audit.get("json_path") == expected_json_path
    assert planning_audit.get("md_path") == expected_md_path
    assert planning_audit.get("json_exists") is False
    assert planning_audit.get("md_exists") is False

    issues = json.loads((repo_root / ".fake_beads.json").read_text(encoding="utf-8"))
    assert issues["bd-1"]["status"] == "closed"
    assert "RUN_ID=20250101-000000-deadbeef" in issues["bd-1"]["notes"]
    assert issues["bd-2"]["status"] == "open"
    assert "Upstream bd-1 closed" in issues["bd-2"]["notes"]

    message = _git(repo_root, "log", "-1", "--pretty=%B")
    assert message.splitlines()[0] == "beads(bd-1): Test bead"
    assert f"RUN_ID: {run_id}" in message
    assert _git(repo_root, "status", "--porcelain") == ""

    invocations = [
        json.loads(line)
        for line in conda_log.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    run_calls = [argv for argv in invocations if len(argv) >= 2 and argv[1] == "run"]
    assert run_calls, "expected at least one `conda run` invocation"
    assert "-n" in run_calls[0]
    assert run_calls[0][run_calls[0].index("-n") + 1] == "test"
    assert "pytest" in run_calls[0]


def test_execute_repo_tick_auto_closes_parent_epic_when_child_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / ".gitignore").write_text(".pytest_cache/\n", encoding="utf-8")
    (repo_root / "test_dummy.py").write_text(
        "def test_ok():\n    assert 1 + 1 == 2\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    (repo_root / ".fake_beads.json").write_text(
        json.dumps(
            {
                "bd-epic": {
                    "id": "bd-epic",
                    "title": "Parent epic",
                    "status": "in_progress",
                    "notes": "",
                    "issue_type": "epic",
                    "dependencies": [],
                    "dependents": [
                        {"id": "bd-1", "dependency_type": "parent-child"},
                    ],
                },
                "bd-1": {
                    "id": "bd-1",
                    "title": "Child bead",
                    "status": "open",
                    "notes": "",
                    "issue_type": "task",
                    "parent": "bd-epic",
                    "dependencies": [
                        {"id": "bd-epic", "dependency_type": "parent-child"},
                    ],
                    "dependents": [],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", ".fake_beads.json")
    _git(repo_root, "commit", "-m", "beads init")

    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250101-000000-cafefeed"

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
        stdout="",
        stderr="",
    )
    deck = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="test_repo",
        created_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        items=(
            RunDeckItem(
                bead_id="bd-1",
                title="Child bead",
                contract=contract,
                baseline_validation=(baseline,),
            ),
        ),
    )
    write_run_deck(paths, deck=deck)

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

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_bd(bin_dir)
    _write_fake_codex(bin_dir)
    _write_fake_conda(bin_dir)
    pytest_stub = bin_dir / "pytest"
    pytest_stub.write_text("#!/usr/bin/env sh\nexit 0\n", encoding="utf-8")
    _make_executable(pytest_stub)
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))

    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(seconds=30))
    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=tmp_path / "unused_overlay.toml",
        tick=tick,
        config=RepoExecutionConfig(
            tick_budget=timedelta(seconds=30),
            min_minutes_to_start_new_bead=0,
            max_beads_per_tick=3,
            diff_caps=DiffCaps(max_files_changed=10, max_lines_added=100),
        ),
    )

    assert result.skipped is False
    assert result.beads_closed == 1
    issues = json.loads((repo_root / ".fake_beads.json").read_text(encoding="utf-8"))
    assert issues["bd-1"]["status"] == "closed"
    assert issues["bd-epic"]["status"] == "closed"
    assert "Auto-closed epic after all parent-child beads closed" in issues["bd-epic"]["notes"]


def test_execute_repo_tick_refreshes_env_on_dependency_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / ".gitignore").write_text(".pytest_cache/\n", encoding="utf-8")
    (repo_root / "pyproject.toml").write_text("[project]\nname = \"demo\"\n", encoding="utf-8")
    (repo_root / "environment.yml").write_text(
        "name: test\nchannels: []\ndependencies: []\n",
        encoding="utf-8",
    )
    (repo_root / "test_dummy.py").write_text(
        "def test_ok():\n    assert 1 + 1 == 2\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    (repo_root / ".fake_beads.json").write_text(
        json.dumps(
            {
                "bd-1": {
                    "id": "bd-1",
                    "title": "Test bead",
                    "status": "open",
                    "notes": "",
                    "dependencies": [],
                    "dependents": [],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", ".fake_beads.json")
    _git(repo_root, "commit", "-m", "beads init")

    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250102-000000-deadbeef"

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
        stdout="",
        stderr="",
    )
    deck = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="test_repo",
        created_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        items=(
            RunDeckItem(
                bead_id="bd-1",
                title="Test bead",
                contract=contract,
                baseline_validation=(baseline,),
            ),
        ),
    )
    write_run_deck(paths, deck=deck)

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

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_bd(bin_dir)
    _write_fake_codex(bin_dir)
    _write_fake_conda(bin_dir)

    conda_log = tmp_path / "conda_argv.jsonl"
    monkeypatch.setenv("FAKE_CONDA_LOG", conda_log.as_posix())
    monkeypatch.setenv("FAKE_CODEX_EDIT_PYPROJECT", "1")
    monkeypatch.setenv("FAKE_CODEX_EDIT_ENV", "1")
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))

    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(seconds=30))
    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=tmp_path / "unused_overlay.toml",
        tick=tick,
        config=RepoExecutionConfig(
            tick_budget=timedelta(seconds=30),
            min_minutes_to_start_new_bead=0,
            max_beads_per_tick=3,
            diff_caps=DiffCaps(max_files_changed=10, max_lines_added=100),
        ),
    )
    assert result.skipped is False

    invocations = [
        json.loads(line)
        for line in conda_log.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    env_updates = [
        argv
        for argv in invocations
        if len(argv) >= 4 and argv[1] == "env" and argv[2] == "update"
    ]
    assert env_updates, "expected conda env update for environment.yml"
    assert any(
        arg.endswith("environment.yml") for argv in env_updates for arg in argv
    )

    run_calls = [argv for argv in invocations if len(argv) >= 2 and argv[1] == "run"]
    assert any(
        "pip" in argv and "install" in argv and "-e" in argv for argv in run_calls
    ), "expected editable pip install via conda run"


def test_execute_repo_tick_commits_failure_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / ".gitignore").write_text(".pytest_cache/\n", encoding="utf-8")
    (repo_root / "test_dummy.py").write_text(
        "def test_ok():\n    assert 1 + 1 == 2\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    (repo_root / ".fake_beads.json").write_text(
        json.dumps(
            {
                "bd-1": {
                    "id": "bd-1",
                    "title": "Test bead",
                    "status": "open",
                    "notes": "",
                    "dependencies": [],
                    "dependents": [],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", ".fake_beads.json")
    _git(repo_root, "commit", "-m", "beads init")

    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250103-000000-deadbeef"

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
        stdout="",
        stderr="",
    )
    deck = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="test_repo",
        created_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        items=(
            RunDeckItem(
                bead_id="bd-1",
                title="Test bead",
                contract=contract,
                baseline_validation=(baseline,),
            ),
        ),
    )
    write_run_deck(paths, deck=deck)

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

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_bd(bin_dir)
    _write_fake_codex(bin_dir)
    _write_fake_conda(bin_dir)

    conda_log = tmp_path / "conda_argv.jsonl"
    monkeypatch.setenv("FAKE_CONDA_LOG", conda_log.as_posix())
    monkeypatch.setenv("FAKE_CONDA_FAIL_PYTEST", "1")
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))

    started_at = datetime.now().astimezone()
    tick = TickBudget(started_at=started_at, ends_at=started_at + timedelta(seconds=30))
    result = execute_repo_tick(
        paths=paths,
        run_id=run_id,
        repo_policy=policy,
        overlay_path=tmp_path / "unused_overlay.toml",
        tick=tick,
        config=RepoExecutionConfig(
            tick_budget=timedelta(seconds=30),
            min_minutes_to_start_new_bead=0,
            max_beads_per_tick=3,
            diff_caps=DiffCaps(max_files_changed=10, max_lines_added=100),
        ),
    )
    assert result.stop_reason in {"blocked", "error"}
    assert result.beads_closed == 0
    assert result.bead_results[0].outcome == "failed"
    assert _git(repo_root, "status", "--porcelain") == ""

    message = _git(repo_root, "log", "-1", "--pretty=%B")
    assert message.splitlines()[0] == "beads(bd-1): Test bead (failed)"


@pytest.mark.parametrize(
    ("enforce_gwt", "add_markers", "expect_closed"),
    [
        (False, False, True),
        (True, False, False),
        (True, True, True),
    ],
)
def test_execute_repo_tick_given_when_then_enforcement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    enforce_gwt: bool,
    add_markers: bool,
    expect_closed: bool,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _git(repo_root, "init", "-b", "main")
    _git(repo_root, "config", "user.name", "Test")
    _git(repo_root, "config", "user.email", "test@example.com")
    (repo_root / ".gitignore").write_text(".pytest_cache/\n", encoding="utf-8")
    (repo_root / "test_dummy.py").write_text(
        "def test_ok():\n    assert 1 + 1 == 2\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", "-A")
    _git(repo_root, "commit", "-m", "init")

    (repo_root / ".fake_beads.json").write_text(
        json.dumps(
            {
                "bd-1": {
                    "id": "bd-1",
                    "title": "Test bead",
                    "status": "open",
                    "notes": "",
                    "dependencies": [],
                    "dependents": [],
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo_root, "add", ".fake_beads.json")
    _git(repo_root, "commit", "-m", "beads init")

    cache_dir = tmp_path / "cache"
    paths = OrchestratorPaths(cache_dir=cache_dir)
    run_id = "20250101-000000-deadbeef"

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
        enforce_given_when_then=enforce_gwt,
    )
    baseline = ValidationResult(
        command="pytest -q",
        exit_code=0,
        started_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        stdout="",
        stderr="",
    )
    deck = RunDeck(
        schema_version=2,
        run_id=run_id,
        repo_id="test_repo",
        created_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        items=(
            RunDeckItem(
                bead_id="bd-1",
                title="Test bead",
                contract=contract,
                baseline_validation=(baseline,),
            ),
        ),
    )
    write_run_deck(paths, deck=deck)

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

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_bd(bin_dir)
    _write_fake_codex(bin_dir)
    _write_fake_conda(bin_dir)
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))
    monkeypatch.setenv("FAKE_CODEX_EDIT_TEST_FILE", "1")
    if add_markers:
        monkeypatch.setenv("FAKE_CODEX_ADD_GWT", "1")
    else:
        monkeypatch.delenv("FAKE_CODEX_ADD_GWT", raising=False)

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
            max_beads_per_tick=3,
            diff_caps=DiffCaps(max_files_changed=10, max_lines_added=100),
        ),
    )

    issues = json.loads((repo_root / ".fake_beads.json").read_text(encoding="utf-8"))
    if expect_closed:
        assert result.beads_closed == 1
        assert issues["bd-1"]["status"] == "closed"
    else:
        assert result.beads_closed == 0
        assert result.stop_reason == "blocked"
        assert issues["bd-1"]["status"] == "in_progress"
        assert "Given/When/Then" in issues["bd-1"]["notes"]
