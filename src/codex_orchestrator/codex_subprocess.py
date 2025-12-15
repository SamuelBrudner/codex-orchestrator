from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


class CodexCliError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CodexInvocation:
    args: tuple[str, ...]
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
) -> CodexInvocation:
    args = ("codex", "exec", "--full-auto", *extra_args)
    started_at = datetime.now().astimezone()
    try:
        completed = subprocess.run(
            list(args),
            cwd=cwd,
            input=prompt,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
        finished_at = datetime.now().astimezone()
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        exit_code = int(completed.returncode)
    except FileNotFoundError as e:
        raise CodexCliError("codex CLI not found (install codex and ensure it's on PATH).") from e
    except subprocess.TimeoutExpired as e:
        finished_at = datetime.now().astimezone()
        stdout = (e.stdout or "") if isinstance(e.stdout, str) else ""
        stderr = (e.stderr or "") if isinstance(e.stderr, str) else ""
        exit_code = 124

    if len(stdout) > output_limit_chars:
        stdout = stdout[:output_limit_chars] + f"\n...<truncated {len(stdout) - output_limit_chars} chars>"
    if len(stderr) > output_limit_chars:
        stderr = stderr[:output_limit_chars] + f"\n...<truncated {len(stderr) - output_limit_chars} chars>"

    return CodexInvocation(
        args=args,
        started_at=started_at,
        finished_at=finished_at,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
    )

