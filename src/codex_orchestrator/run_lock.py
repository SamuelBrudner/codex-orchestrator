from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import IO


class RunLockError(RuntimeError):
    pass


_IN_PROCESS_GUARD = threading.Lock()
_IN_PROCESS_HELD: set[str] = set()


@dataclass(slots=True)
class RunLock:
    lock_path: Path
    _handle: IO[str] | None = None
    _uses_flock: bool = True
    _guard_key: str | None = None

    def acquire(self) -> None:
        if self._handle is not None:
            raise RunLockError("RunLock already acquired in this process.")

        guard_key = str(self.lock_path.resolve())
        with _IN_PROCESS_GUARD:
            if guard_key in _IN_PROCESS_HELD:
                raise RunLockError(f"Lock already held in this process: {self.lock_path}")
            _IN_PROCESS_HELD.add(guard_key)
        self._guard_key = guard_key

        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                import fcntl  # pyright: ignore[reportMissingImports]
            except ModuleNotFoundError:  # pragma: no cover
                self._uses_flock = False
                fd = None
                try:
                    fd = os.open(
                        str(self.lock_path),
                        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                        0o644,
                    )
                except FileExistsError as e:
                    raise RunLockError(
                        f"Another orchestrator instance is running (lock: {self.lock_path})"
                    ) from e

                self._handle = os.fdopen(fd, "w", encoding="utf-8")
                self._write_lock_info()
                return

            handle = self.lock_path.open("a+", encoding="utf-8")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as e:
                handle.close()
                raise RunLockError(
                    f"Another orchestrator instance is running (lock: {self.lock_path})"
                ) from e

            self._handle = handle
            self._write_lock_info()
        except Exception:
            with _IN_PROCESS_GUARD:
                _IN_PROCESS_HELD.discard(guard_key)
            self._guard_key = None
            raise

    def _write_lock_info(self) -> None:
        if self._handle is None:
            return

        payload = {
            "pid": os.getpid(),
            "locked_at": datetime.now(timezone.utc).isoformat(),
        }

        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(json.dumps(payload, sort_keys=True))
        self._handle.write("\n")
        self._handle.flush()

    def release(self) -> None:
        if self._handle is None:
            return

        try:
            if not self._uses_flock:
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass
        finally:
            self._handle.close()
            self._handle = None
            if self._guard_key is not None:
                with _IN_PROCESS_GUARD:
                    _IN_PROCESS_HELD.discard(self._guard_key)
                self._guard_key = None

    def __enter__(self) -> RunLock:
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
