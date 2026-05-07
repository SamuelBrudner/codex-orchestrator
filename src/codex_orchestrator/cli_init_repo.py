from __future__ import annotations

import argparse
from pathlib import Path

import tomlkit

from codex_orchestrator.cli_overlay import cmd_overlay_apply as _cmd_overlay_apply
from codex_orchestrator.git_subprocess import GitError, git_current_branch
from codex_orchestrator.toml_writers import (
    load_toml_document,
    render_repo_inventory_entry_toml,
    repo_inventory_entry_table,
)

_DEFAULT_INIT_DENY_ROOTS: tuple[str, ...] = (
    ".benchmarks",
    ".idea",
    ".pytest_cache",
    ".ruff_cache",
    "__marimo__",
    "__pycache__",
    "data",
    "datasets",
    "executed_notebooks",
    "figs",
    "figures",
    "logs",
    "results",
)


def _ensure_repo_inventory_entry(
    *,
    repo_id: str,
    repo_path: Path,
    base_branch: str,
    env_name: str,
    validation_commands: tuple[str, ...],
    allow_existing: bool,
) -> bool:
    config_path = Path("config/repos.toml")
    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        data = load_toml_document(config_path)
        repos = data.get("repos")
        if not isinstance(repos, dict):
            raise SystemExit(
                "codex-orchestrator: config/repos.toml is missing a [repos] table; "
                "fix it before running init-repo."
            )
        if repo_id in repos:
            if allow_existing:
                return False
            raise SystemExit(
                f"codex-orchestrator: repo_id {repo_id!r} already exists in {config_path}. "
                "Use --allow-existing to keep it and only bootstrap overlay/beads."
            )
        repos.add(
            repo_id,
            repo_inventory_entry_table(
                repo_path=repo_path,
                base_branch=base_branch,
                env_name=env_name,
                validation_commands=validation_commands,
                deny_roots=_DEFAULT_INIT_DENY_ROOTS,
            ),
        )
        config_path.write_text(tomlkit.dumps(data).rstrip() + "\n", encoding="utf-8")
        return True

    header = "\n".join(
        [
            "# Repository inventory for codex-orchestrator.",
            "#",
            "# Each repo is keyed by a stable `repo_id` under `[repos.<repo_id>]`.",
            "# Required fields: `path`, `base_branch`.",
            "# Optional fields: `env`, `notebook_roots`, `allowed_roots`, `deny_roots`,",
            "#                  `validation_commands`, `notebook_output_policy`.",
            "",
        ]
    )
    entry = render_repo_inventory_entry_toml(
        repo_id=repo_id,
        repo_path=repo_path,
        base_branch=base_branch,
        env_name=env_name,
        validation_commands=validation_commands,
        deny_roots=_DEFAULT_INIT_DENY_ROOTS,
    )
    config_path.write_text(header + entry + "\n", encoding="utf-8")
    return True


def _resolve_init_repo_path(raw_path: str) -> Path:
    repo_path = Path(raw_path).expanduser()
    if not repo_path.is_absolute():
        repo_path = (Path.cwd() / repo_path).resolve()
    else:
        repo_path = repo_path.resolve()

    if not repo_path.exists():
        raise SystemExit(f"codex-orchestrator: --path does not exist: {repo_path}")
    if not repo_path.is_dir():
        raise SystemExit(f"codex-orchestrator: --path must be a directory: {repo_path}")
    return repo_path


def _resolve_init_base_branch(*, repo_root: Path, raw: str | None) -> str:
    if raw is not None and raw.strip():
        return raw.strip()
    try:
        branch = git_current_branch(repo_root=repo_root).strip()
    except GitError as e:
        raise SystemExit(
            "codex-orchestrator: could not detect base branch from git. "
            f"Pass --base-branch explicitly. ({e})"
        ) from e
    if not branch or branch == "HEAD":
        raise SystemExit(
            "codex-orchestrator: could not infer base branch from current HEAD; "
            "pass --base-branch explicitly."
        )
    return branch


def _restore_repos_config(*, config_path: Path, previous_text: str | None) -> None:
    try:
        if previous_text is None:
            config_path.unlink(missing_ok=True)
            return
        config_path.write_text(previous_text, encoding="utf-8")
    except OSError as e:
        raise SystemExit(f"codex-orchestrator: failed to roll back {config_path}: {e}") from e


def cmd_init_repo(args: argparse.Namespace) -> int:
    repo_id = str(args.repo_id).strip()
    if not repo_id:
        raise SystemExit("codex-orchestrator: --repo-id must be non-empty")

    env_name = str(args.env).strip()
    if not env_name:
        raise SystemExit("codex-orchestrator: --env must be non-empty")

    repo_path = _resolve_init_repo_path(str(args.path))
    base_branch = _resolve_init_base_branch(repo_root=repo_path, raw=args.base_branch)
    validation_commands = tuple(
        c for c in (str(item).strip() for item in (args.validation_command or ())) if c
    )
    config_path = Path("config/repos.toml")
    previous_config_text = (
        config_path.read_text(encoding="utf-8") if config_path.exists() else None
    )

    config_written = _ensure_repo_inventory_entry(
        repo_id=repo_id,
        repo_path=repo_path,
        base_branch=base_branch,
        env_name=env_name,
        validation_commands=validation_commands,
        allow_existing=bool(args.allow_existing),
    )

    overlay_args = argparse.Namespace(
        repo_id=repo_id,
        time_budget_minutes=int(args.time_budget_minutes),
        env=env_name,
        allow_env_creation=bool(args.allow_env_creation),
        requires_notebook_execution=bool(args.requires_notebook_execution),
        validation_command=list(validation_commands) or None,
    )
    try:
        overlay_rc = _cmd_overlay_apply(overlay_args)
    except BaseException:
        if config_written:
            _restore_repos_config(config_path=config_path, previous_text=previous_config_text)
            print(
                f"repo_id={repo_id} status=rolled_back repos_config={config_path.as_posix()} "
                "reason=overlay_apply_exception"
            )
        raise

    if overlay_rc != 0 and config_written:
        _restore_repos_config(config_path=config_path, previous_text=previous_config_text)
        print(
            f"repo_id={repo_id} status=rolled_back repos_config={config_path.as_posix()} "
            "reason=overlay_apply_nonzero"
        )
        return overlay_rc

    status = "config_written" if config_written else "config_exists"
    print(
        f"repo_id={repo_id} status={status} repos_config={config_path.as_posix()} "
        f"path={repo_path.as_posix()} base_branch={base_branch} env={env_name}"
    )
    return overlay_rc
