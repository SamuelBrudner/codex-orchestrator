from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


class GitError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class GitStatusEntry:
    xy: str
    path: str
    orig_path: str | None = None


def _run_git(
    args: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float = 60.0,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as e:
        raise GitError("git CLI not found (install git and ensure it's on PATH).") from e
    except subprocess.TimeoutExpired as e:
        raise GitError(f"git {' '.join(args)} timed out after {timeout_seconds:.0f}s.") from e

    if check and completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr or stdout or "<no output>"
        raise GitError(f"git {' '.join(args)} failed (exit={completed.returncode}): {details}")
    return completed


def git_remotes(*, repo_root: Path) -> list[str]:
    completed = _run_git(["remote"], cwd=repo_root, check=True)
    return [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]


def git_fetch(*, repo_root: Path, timeout_seconds: float = 120.0) -> None:
    if not git_remotes(repo_root=repo_root):
        return
    _run_git(["fetch", "--all", "--prune"], cwd=repo_root, timeout_seconds=timeout_seconds, check=True)


def git_head_is_detached(*, repo_root: Path) -> bool:
    completed = _run_git(["symbolic-ref", "-q", "HEAD"], cwd=repo_root, check=False)
    return completed.returncode != 0


def git_status_porcelain(*, repo_root: Path) -> tuple[GitStatusEntry, ...]:
    completed = _run_git(["status", "--porcelain", "-z"], cwd=repo_root, check=True)
    data = completed.stdout or ""
    if not data:
        return ()

    parts = data.split("\0")
    out: list[GitStatusEntry] = []
    idx = 0
    while idx < len(parts):
        raw = parts[idx]
        if not raw:
            break
        if len(raw) < 4:
            raise GitError(f"Unexpected git status entry: {raw!r}")

        xy = raw[:2]
        path = raw[3:]
        orig_path: str | None = None

        if xy[0] in {"R", "C"}:
            if idx + 1 >= len(parts):
                raise GitError(f"Unexpected git status rename entry: {raw!r}")
            orig_path = path
            path = parts[idx + 1]
            idx += 2
        else:
            idx += 1

        out.append(GitStatusEntry(xy=xy, path=path, orig_path=orig_path))

    return tuple(out)


def git_is_dirty(*, repo_root: Path) -> bool:
    return bool(git_status_porcelain(repo_root=repo_root))


def git_current_branch(*, repo_root: Path) -> str:
    completed = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_root, check=True)
    branch = (completed.stdout or "").strip()
    if not branch:
        raise GitError("Unable to determine current branch (empty output).")
    return branch


def git_rev_parse(*, repo_root: Path, ref: str = "HEAD") -> str:
    completed = _run_git(["rev-parse", ref], cwd=repo_root, check=True)
    value = (completed.stdout or "").strip()
    if not value:
        raise GitError(f"git rev-parse {ref!r} returned empty output.")
    return value


def git_branch_exists(*, repo_root: Path, branch: str) -> bool:
    completed = _run_git(
        ["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=repo_root,
        check=False,
    )
    return completed.returncode == 0


def git_checkout(*, repo_root: Path, ref: str) -> None:
    _run_git(["checkout", ref], cwd=repo_root, check=True)


def git_checkout_new_branch(*, repo_root: Path, branch: str, base_ref: str) -> None:
    _run_git(["checkout", "-b", branch, base_ref], cwd=repo_root, check=True)


def git_stage_all(*, repo_root: Path) -> None:
    _run_git(["add", "-A"], cwd=repo_root, check=True)


def git_commit(*, repo_root: Path, subject: str, body: str) -> str:
    _run_git(["commit", "-m", subject, "-m", body], cwd=repo_root, check=True)
    return git_rev_parse(repo_root=repo_root)


def git_diff_numstat(*, repo_root: Path, staged: bool) -> tuple[tuple[str, int, int], ...]:
    args = ["diff", "--numstat"]
    if staged:
        args.append("--staged")
    completed = _run_git(args, cwd=repo_root, check=True)
    out: list[tuple[str, int, int]] = []
    for line in (completed.stdout or "").splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        added_raw, deleted_raw, path = parts
        try:
            added = int(added_raw) if added_raw.isdigit() else 0
        except ValueError:
            added = 0
        try:
            deleted = int(deleted_raw) if deleted_raw.isdigit() else 0
        except ValueError:
            deleted = 0
        out.append((path, added, deleted))
    return tuple(out)


def _within(path: Path, root: Path) -> bool:
    if root == Path(".") or root == Path():
        return True
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def validate_paths_within_policy(
    *,
    paths: Iterable[str],
    allowed_roots: Iterable[Path],
    deny_roots: Iterable[Path],
) -> None:
    allowed = [Path(p) for p in allowed_roots]
    deny = [Path(p) for p in deny_roots]
    errors: list[str] = []
    for raw in sorted({p for p in paths if p}):
        p = Path(raw)
        if p.is_absolute():
            errors.append(f"Path must be repo-relative: {raw!r}")
            continue
        if any(_within(p, d) for d in deny):
            errors.append(f"Path is under deny_roots: {raw!r}")
            continue
        if not any(_within(p, a) for a in allowed):
            errors.append(f"Path is outside allowed_roots: {raw!r}")
            continue
    if errors:
        raise GitError("Safety boundary violation:\n- " + "\n- ".join(errors))
