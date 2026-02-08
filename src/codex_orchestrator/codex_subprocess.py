from __future__ import annotations

import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


class CodexCliError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CodexInvocation:
    args: tuple[str, ...]
    pid: int | None
    started_at: datetime
    finished_at: datetime
    exit_code: int
    stdout: str
    stderr: str


def codex_exec_full_auto(
    *,
    prompt: str,
    cwd: Path,
    timeout_seconds: float,
    extra_args: tuple[str, ...] = (),
    output_limit_chars: int = 200_000,
    on_start: Callable[[int, tuple[str, ...], datetime], None] | None = None,
) -> CodexInvocation:
    args = ("codex", "exec", "--full-auto", *extra_args)
    started_at = datetime.now().astimezone()
    try:
        proc = subprocess.Popen(
            list(args),
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        pid = int(proc.pid) if proc.pid is not None else None
        if pid is not None and on_start is not None:
            on_start(pid, args, started_at)

        stdout, stderr = proc.communicate(input=prompt, timeout=timeout_seconds)
        finished_at = datetime.now().astimezone()
        stdout = stdout or ""
        stderr = stderr or ""
        exit_code = int(proc.returncode) if proc.returncode is not None else 0
    except FileNotFoundError as e:
        raise CodexCliError("codex CLI not found (install codex and ensure it's on PATH).") from e
    except subprocess.TimeoutExpired as e:
        # `TimeoutExpired` differs slightly between subprocess.run() and Popen.communicate();
        # use both stdout/output attrs defensively.
        pid = int(proc.pid) if "proc" in locals() and proc.pid is not None else None
        try:
            proc.kill()
        except Exception:
            pass
        try:
            remaining_stdout, remaining_stderr = proc.communicate()
        except Exception:
            remaining_stdout, remaining_stderr = "", ""
        finished_at = datetime.now().astimezone()
        partial_stdout = getattr(e, "stdout", None)
        if partial_stdout is None:
            partial_stdout = getattr(e, "output", None)
        partial_stderr = getattr(e, "stderr", None)
        stdout = partial_stdout if isinstance(partial_stdout, str) else ""
        stderr = partial_stderr if isinstance(partial_stderr, str) else ""
        if isinstance(remaining_stdout, str) and remaining_stdout:
            stdout += remaining_stdout
        if isinstance(remaining_stderr, str) and remaining_stderr:
            stderr += remaining_stderr
        exit_code = 124

    if len(stdout) > output_limit_chars:
        stdout = stdout[:output_limit_chars] + f"\n...<truncated {len(stdout) - output_limit_chars} chars>"
    if len(stderr) > output_limit_chars:
        stderr = stderr[:output_limit_chars] + f"\n...<truncated {len(stderr) - output_limit_chars} chars>"

    return CodexInvocation(
        args=args,
        pid=pid,
        started_at=started_at,
        finished_at=finished_at,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
    )
