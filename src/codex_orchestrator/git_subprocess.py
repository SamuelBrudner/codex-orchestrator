from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


class GitError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class GitStatusEntry:
    xy: str
    path: str
    orig_path: str | None = None


DEFAULT_DIRTY_IGNORE_GLOBS: tuple[str, ...] = (
    ".beads/**",
    "bd.sock",
    ".pytest_cache/**",
    "__pycache__/**",
    "*.pyc",
    "*.pyo",
    ".mypy_cache/**",
    ".ruff_cache/**",
    ".hypothesis/**",
    ".tox/**",
    ".nox/**",
    ".coverage",
    ".coverage.*",
    "htmlcov/**",
    ".ipynb_checkpoints/**",
    "*.egg-info/**",
    ".eggs/**",
    "dist/**",
    "build/**",
    ".DS_Store",
)


AUTO_DIRTY_IGNORE_GLOBS: tuple[str, ...] = (
    "tests/data/**",
    "tests/output/**",
    "tests/outputs/**",
    "tests/tmp/**",
    "tests/.cache/**",
)


@dataclass(frozen=True, slots=True)
class DirtyIgnoreResolution:
    resolved: tuple[str, ...]
    detected: tuple[str, ...]


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


def _glob_prefix(pattern: str) -> str:
    for idx, ch in enumerate(pattern):
        if ch in "*?[":
            return pattern[:idx].rstrip("/")
    return pattern.rstrip("/")


def _has_tracked_under(*, repo_root: Path, prefix: str) -> bool:
    if not prefix:
        return False
    completed = _run_git(["ls-files", "-z", "--", prefix], cwd=repo_root, check=True)
    return bool(completed.stdout)


def _dedupe_preserve_order(items: Sequence[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        out.append(item)
        seen.add(item)
    return tuple(out)


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
    completed = _run_git(["status", "--porcelain", "-z", "-uall"], cwd=repo_root, check=True)
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


def _normalize_ignore_globs(ignore_globs: Sequence[str]) -> tuple[str, ...]:
    out: list[str] = []
    for item in ignore_globs:
        if not item:
            continue
        cleaned = item.strip()
        if not cleaned:
            continue
        if cleaned.endswith("/") and not cleaned.endswith("/**"):
            cleaned = cleaned.rstrip("/") + "/**"
        out.append(cleaned)
    return tuple(out)


def _matches_ignore_glob(path: str, ignore_globs: Sequence[str]) -> bool:
    if not ignore_globs:
        return False
    rel = PurePosixPath(path)
    for pattern in ignore_globs:
        if rel.match(pattern):
            return True
        if pattern.endswith("/**"):
            base = pattern[:-3]
            if base and rel.match(base):
                return True
    return False


def detect_dirty_ignore_globs(
    *,
    repo_root: Path,
    candidates: Sequence[str] = AUTO_DIRTY_IGNORE_GLOBS,
) -> tuple[str, ...]:
    entries = git_status_porcelain(repo_root=repo_root)
    untracked = [e.path for e in entries if e.xy == "??" and e.path]
    if not untracked:
        return ()
    detected: list[str] = []
    for pattern in _normalize_ignore_globs(candidates):
        if not any(_matches_ignore_glob(path, (pattern,)) for path in untracked):
            continue
        prefix = _glob_prefix(pattern)
        if prefix and _has_tracked_under(repo_root=repo_root, prefix=prefix):
            continue
        detected.append(pattern)
    return _dedupe_preserve_order(detected)


def resolve_dirty_ignore_globs(
    *,
    repo_root: Path,
    configured: Sequence[str],
) -> DirtyIgnoreResolution:
    configured = _normalize_ignore_globs(configured)
    detected = detect_dirty_ignore_globs(repo_root=repo_root)
    resolved = _dedupe_preserve_order(
        [*configured, *DEFAULT_DIRTY_IGNORE_GLOBS, *detected]
    )
    return DirtyIgnoreResolution(resolved=resolved, detected=detected)


def git_status_filtered(
    *,
    repo_root: Path,
    ignore_globs: Sequence[str] = (),
) -> tuple[GitStatusEntry, ...]:
    entries = git_status_porcelain(repo_root=repo_root)
    ignore_globs = _normalize_ignore_globs(ignore_globs)
    if not ignore_globs:
        return entries
    return tuple(
        e
        for e in entries
        if not (e.path and _matches_ignore_glob(e.path, ignore_globs))
    )


def git_is_dirty(*, repo_root: Path, ignore_globs: Sequence[str] = ()) -> bool:
    return bool(git_status_filtered(repo_root=repo_root, ignore_globs=ignore_globs))


def git_remove_ignored_untracked(
    *,
    repo_root: Path,
    ignore_globs: Sequence[str],
) -> list[str]:
    ignore_globs = _normalize_ignore_globs(ignore_globs)
    if not ignore_globs:
        return []
    removed: list[str] = []
    entries = git_status_porcelain(repo_root=repo_root)
    for entry in entries:
        if entry.xy != "??" or not entry.path:
            continue
        if not _matches_ignore_glob(entry.path, ignore_globs):
            continue
        rel = Path(entry.path)
        if rel.is_absolute() or ".." in rel.parts or ".git" in rel.parts:
            continue
        target = repo_root / rel
        if not target.exists():
            continue
        try:
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink()
        except OSError as e:
            raise GitError(f"Failed to remove ignored path {entry.path!r}: {e}") from e
        removed.append(entry.path)
    return removed


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


def git_diff_numstat(
    *,
    repo_root: Path,
    staged: bool,
    ignore_globs: Sequence[str] = (),
) -> tuple[tuple[str, int, int], ...]:
    args = ["diff", "--numstat"]
    if staged:
        args.append("--staged")
    completed = _run_git(args, cwd=repo_root, check=True)
    ignore_globs = _normalize_ignore_globs(ignore_globs)
    out: list[tuple[str, int, int]] = []
    for line in (completed.stdout or "").splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        added_raw, deleted_raw, path = parts
        if ignore_globs and _matches_ignore_glob(path, ignore_globs):
            continue
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
