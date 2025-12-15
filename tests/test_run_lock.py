from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from codex_orchestrator.run_lock import RunLock, RunLockError


def test_run_lock_prevents_overlapping_processes(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get("PYTHONPATH", "")

    lock_path = tmp_path / "run.lock"

    code = "\n".join(
        [
            "from __future__ import annotations",
            "import sys",
            "import time",
            "from pathlib import Path",
            "from codex_orchestrator.run_lock import RunLock",
            "lock_path = Path(sys.argv[1])",
            "with RunLock(lock_path):",
            "    print('locked', flush=True)",
            "    time.sleep(10)",
        ]
    )

    proc = subprocess.Popen(
        [sys.executable, "-c", code, str(lock_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().strip()
        assert line == "locked"

        with pytest.raises(RunLockError):
            with RunLock(lock_path):
                pass
    finally:
        proc.terminate()
        proc.wait(timeout=5)

    with RunLock(lock_path):
        pass

