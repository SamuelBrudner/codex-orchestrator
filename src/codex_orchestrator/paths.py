from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


def default_cache_dir() -> Path:
    override = os.environ.get("CODEX_ORCHESTRATOR_CACHE_DIR")
    if override:
        return Path(override).expanduser()

    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        return Path(xdg_cache_home).expanduser() / "codex-orchestrator"

    return Path.home() / ".cache" / "codex-orchestrator"


@dataclass(frozen=True, slots=True)
class OrchestratorPaths:
    cache_dir: Path

    @property
    def current_run_path(self) -> Path:
        return self.cache_dir / "current_run.json"

    @property
    def run_lock_path(self) -> Path:
        return self.cache_dir / "run.lock"

    @property
    def runs_dir(self) -> Path:
        return self.cache_dir / "runs"

    @property
    def repo_locks_dir(self) -> Path:
        return self.cache_dir / "repo_locks"

    def run_dir(self, run_id: str) -> Path:
        return self.runs_dir / run_id

    def run_metadata_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "run.json"

    def run_log_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "orchestrator.log"

    def run_summary_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "run_summary.json"

    def repo_lock_path(self, repo_id: str) -> Path:
        return self.repo_locks_dir / f"{repo_id}.lock"

    def repo_exec_log_path(self, run_id: str, repo_id: str) -> Path:
        return self.run_dir(run_id) / f"{repo_id}.exec.log"

    def repo_stdout_log_path(self, run_id: str, repo_id: str) -> Path:
        return self.run_dir(run_id) / f"{repo_id}.stdout.log"

    def repo_stderr_log_path(self, run_id: str, repo_id: str) -> Path:
        return self.run_dir(run_id) / f"{repo_id}.stderr.log"

    def repo_events_path(self, run_id: str, repo_id: str) -> Path:
        return self.run_dir(run_id) / f"{repo_id}.events.jsonl"

    def repo_summary_path(self, run_id: str, repo_id: str) -> Path:
        return self.run_dir(run_id) / f"{repo_id}.summary.json"

    def run_deck_path(self, run_id: str, repo_id: str, *, day: date | datetime) -> Path:
        if isinstance(day, datetime):
            day = day.date()
        date_str = day.strftime("%Y-%m-%d")
        return self.run_dir(run_id) / f"{repo_id}.deck.{date_str}.json"

    def find_existing_run_deck_path(self, run_id: str, repo_id: str) -> Path | None:
        pattern = f"{repo_id}.deck.*.json"
        candidates = sorted(self.run_dir(run_id).glob(pattern))
        return candidates[0] if candidates else None
