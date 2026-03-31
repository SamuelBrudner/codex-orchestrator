from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from codex_orchestrator.planner import ReadyBead


class BdCliError(RuntimeError):
    pass


DEFAULT_BD_READY_LIMIT = 200


@dataclass(frozen=True, slots=True)
class BdIssue:
    issue_id: str
    title: str
    status: str
    notes: str
    dependencies: tuple[str, ...]
    dependents: tuple[str, ...]
    priority: int | None = None
    issue_type: str | None = None
    owner: str | None = None
    parent_id: str | None = None
    dependency_links: tuple[BdIssueLink, ...] = ()
    dependent_links: tuple[BdIssueLink, ...] = ()


@dataclass(frozen=True, slots=True)
class BdIssueLink:
    issue_id: str
    dependency_type: str | None = None
    status: str | None = None
    issue_type: str | None = None


@dataclass(frozen=True, slots=True)
class BdIssueSummary:
    issue_id: str
    title: str
    status: str


@dataclass(frozen=True, slots=True)
class BdCapabilities:
    version: str | None
    workspace_layout: str
    supports_bootstrap: bool
    supports_init_from_jsonl: bool
    supports_sync: bool
    supports_structured_doctor_output: bool | None = None


@dataclass(frozen=True, slots=True)
class BdDoctorResult:
    status: str
    capabilities: BdCapabilities
    overall_ok: bool | None = None
    failed_checks: int = 0
    raw_output: str = ""
    message: str | None = None


@dataclass(frozen=True, slots=True)
class BdSyncResult:
    status: str
    capabilities: BdCapabilities
    raw_output: str = ""
    message: str | None = None


_HELP_COMMAND_RE = re.compile(r"(?m)^  ([A-Za-z0-9_-]+)\b")
_ACTIVE_WORKSPACE_LAYOUTS = frozenset({"legacy_db", "modern_embeddeddolt"})
_PREPARABLE_WORKSPACE_LAYOUTS = frozenset({"missing", "empty_beads_dir", "jsonl_only"})


def _run_bd_completed(
    args: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float = 60.0,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["bd", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as e:
        raise BdCliError("bd CLI not found (install beads/bd and ensure it's on PATH).") from e
    except subprocess.TimeoutExpired as e:
        raise BdCliError(f"bd {' '.join(args)} timed out after {timeout_seconds:.0f}s.") from e


def _completed_output(completed: subprocess.CompletedProcess[str]) -> str:
    return completed.stdout or ""


def _completed_details(completed: subprocess.CompletedProcess[str]) -> str:
    stderr = (completed.stderr or "").strip()
    stdout = (completed.stdout or "").strip()
    return stderr or stdout or "<no output>"


def _run_bd(
    args: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float = 60.0,
    ok_exit_codes: Sequence[int] = (0,),
) -> str:
    completed = _run_bd_completed(args, cwd=cwd, timeout_seconds=timeout_seconds)

    if completed.returncode not in set(int(code) for code in ok_exit_codes):
        raise BdCliError(
            f"bd {' '.join(args)} failed (exit={completed.returncode}): {_completed_details(completed)}"
        )

    return _completed_output(completed)


def _read_bd_help(*, repo_root: Path, args: Sequence[str] = ()) -> str:
    try:
        return _run_bd([*args, "--help"], cwd=repo_root, timeout_seconds=10.0)
    except BdCliError:
        return ""


def _read_bd_version(*, repo_root: Path) -> str | None:
    for args in (["version"], ["--version"]):
        try:
            payload = _run_bd(args, cwd=repo_root, timeout_seconds=10.0).strip()
        except BdCliError:
            continue
        if payload:
            return payload.splitlines()[0].strip()
    return None


def _help_includes_command(help_text: str, command: str) -> bool:
    return any(match.group(1) == command for match in _HELP_COMMAND_RE.finditer(help_text))


def _detect_workspace_layout(repo_root: Path) -> str:
    beads_dir = repo_root / ".beads"
    if not beads_dir.exists():
        return "missing"
    if not beads_dir.is_dir():
        return "unknown"

    legacy_db = beads_dir / "beads.db"
    modern_db = beads_dir / "embeddeddolt"
    issues_jsonl = beads_dir / "issues.jsonl"

    has_legacy_db = legacy_db.exists()
    has_modern_db = modern_db.exists()
    has_jsonl = issues_jsonl.exists()

    if has_legacy_db and has_modern_db:
        return "conflict"
    if has_modern_db:
        return "modern_embeddeddolt"
    if has_legacy_db:
        return "legacy_db"
    if has_jsonl:
        return "jsonl_only"

    if not any(beads_dir.iterdir()):
        return "empty_beads_dir"
    return "unknown"


def bd_detect_capabilities(*, repo_root: Path) -> BdCapabilities:
    help_text = _read_bd_help(repo_root=repo_root)
    init_help_text = _read_bd_help(repo_root=repo_root, args=("init",))
    return BdCapabilities(
        version=_read_bd_version(repo_root=repo_root),
        workspace_layout=_detect_workspace_layout(repo_root),
        supports_bootstrap=_help_includes_command(help_text, "bootstrap"),
        supports_init_from_jsonl="--from-jsonl" in init_help_text,
        supports_sync=_help_includes_command(help_text, "sync"),
        supports_structured_doctor_output=None,
    )


def _workspace_is_active(capabilities: BdCapabilities) -> bool:
    return capabilities.workspace_layout in _ACTIVE_WORKSPACE_LAYOUTS


def _workspace_is_preparable(capabilities: BdCapabilities) -> bool:
    return capabilities.workspace_layout in _PREPARABLE_WORKSPACE_LAYOUTS


def _workspace_skip_reason(capabilities: BdCapabilities) -> str:
    layout = capabilities.workspace_layout
    if layout == "jsonl_only":
        return "tracked_jsonl_without_active_database"
    if layout == "missing":
        return "no_beads_workspace"
    if layout == "empty_beads_dir":
        return "empty_beads_workspace"
    return f"workspace_layout={layout}"


def _workspace_error(capabilities: BdCapabilities, *, repo_root: Path) -> BdCliError:
    beads_dir = repo_root / ".beads"
    if capabilities.workspace_layout == "conflict":
        return BdCliError(
            f"bd workspace compatibility error for {repo_root}: both legacy .beads/beads.db and modern "
            f".beads/embeddeddolt are present; resolve the conflict manually before rerunning."
        )

    if capabilities.workspace_layout == "unknown":
        entries = ", ".join(sorted(p.name for p in beads_dir.iterdir())) or "<empty>"
        return BdCliError(
            f"bd workspace compatibility error for {repo_root}: unrecognized .beads layout ({entries}). "
            "Refusing to run plain 'bd init --quiet' because it could create an empty database and hide "
            "existing issue data. Use a supported layout (legacy beads.db, tracked issues.jsonl, or "
            "embedded Dolt) and rerun."
        )

    return BdCliError(f"bd workspace compatibility error for {repo_root}: unsupported layout {capabilities.workspace_layout!r}")


def _detect_unknown_command(error: BdCliError, command: str) -> bool:
    msg = str(error).lower()
    return f'unknown command "{command}"' in msg or f"unknown command '{command}'" in msg


def _parse_json_output(stdout: str) -> Any:
    payload = stdout.strip()
    if not payload:
        return None
    candidates = [payload]
    for marker in ("\n[", "\n{"):
        start = payload.find(marker)
        if start != -1:
            candidates.append(payload[start + 1 :].strip())

    for candidate in candidates:
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    try:
        return json.loads(payload)
    except json.JSONDecodeError as e:
        raise BdCliError(f"Failed to parse bd --json output: {e}") from e


def _parse_issue(data: Any, *, context: str) -> BdIssue:
    if not isinstance(data, dict):
        raise BdCliError(f"{context}: expected object, got {type(data).__name__}")

    issue_id = data.get("id")
    title = data.get("title")
    status = data.get("status")
    notes = data.get("notes", "")
    if notes is None:
        notes = ""
    if not isinstance(issue_id, str) or not issue_id.strip():
        raise BdCliError(f"{context}: missing string id")
    if not isinstance(title, str) or not title.strip():
        raise BdCliError(f"{context}: missing string title")
    if not isinstance(status, str) or not status.strip():
        raise BdCliError(f"{context}: missing string status")
    if not isinstance(notes, str):
        raise BdCliError(f"{context}: notes must be a string")

    def _extract_links(field: str) -> tuple[BdIssueLink, ...]:
        raw = data.get(field, [])
        if raw is None:
            raw = []
        if not isinstance(raw, list):
            raise BdCliError(f"{context}: {field} must be a list")
        links: list[BdIssueLink] = []
        for idx, item in enumerate(raw):
            if not isinstance(item, dict):
                raise BdCliError(
                    f"{context}: {field}[{idx}] expected object, got {type(item).__name__}"
                )
            dep_id = item.get("id")
            if not isinstance(dep_id, str) or not dep_id.strip():
                raise BdCliError(f"{context}: {field}[{idx}].id missing string")
            dep_type = item.get("dependency_type")
            if dep_type is not None and not isinstance(dep_type, str):
                dep_type = None
            dep_status = item.get("status")
            if dep_status is not None and not isinstance(dep_status, str):
                dep_status = None
            dep_issue_type = item.get("issue_type")
            if dep_issue_type is not None and not isinstance(dep_issue_type, str):
                dep_issue_type = None
            links.append(
                BdIssueLink(
                    issue_id=dep_id,
                    dependency_type=dep_type,
                    status=dep_status,
                    issue_type=dep_issue_type,
                )
            )
        return tuple(links)

    dependency_links = _extract_links("dependencies")
    dependent_links = _extract_links("dependents")
    dependencies = tuple(link.issue_id for link in dependency_links)
    dependents = tuple(link.issue_id for link in dependent_links)

    priority_raw = data.get("priority")
    priority: int | None
    if isinstance(priority_raw, bool) or priority_raw is None:
        priority = None
    elif isinstance(priority_raw, int):
        priority = priority_raw
    else:
        priority = None

    issue_type = data.get("issue_type")
    if issue_type is not None and not isinstance(issue_type, str):
        issue_type = None

    owner = data.get("owner")
    if owner is not None and not isinstance(owner, str):
        owner = None

    parent_id = data.get("parent")
    if parent_id is not None and not isinstance(parent_id, str):
        parent_id = None
    if isinstance(parent_id, str):
        parent_id = parent_id.strip() or None
    if parent_id is None:
        parent_candidates = [
            link.issue_id
            for link in dependency_links
            if (link.dependency_type or "").strip().lower() == "parent-child"
        ]
        if len(parent_candidates) == 1:
            parent_id = parent_candidates[0]

    return BdIssue(
        issue_id=issue_id,
        title=title,
        status=status,
        notes=notes,
        dependencies=dependencies,
        dependents=dependents,
        priority=priority,
        issue_type=issue_type,
        owner=owner,
        parent_id=parent_id,
        dependency_links=dependency_links,
        dependent_links=dependent_links,
    )


def _parse_single_issue(data: Any, *, context: str) -> BdIssue:
    if isinstance(data, list):
        if len(data) != 1:
            raise BdCliError(f"{context}: expected single-item list, got {len(data)} items")
        data = data[0]
    return _parse_issue(data, context=context)


def bd_prepare_workspace(*, repo_root: Path) -> BdCapabilities:
    capabilities = bd_detect_capabilities(repo_root=repo_root)
    if _workspace_is_active(capabilities):
        return capabilities

    if not _workspace_is_preparable(capabilities):
        raise _workspace_error(capabilities, repo_root=repo_root)

    if capabilities.workspace_layout == "jsonl_only":
        bootstrap_error: BdCliError | None = None
        if capabilities.supports_bootstrap:
            try:
                _run_bd(["bootstrap"], cwd=repo_root)
            except BdCliError as e:
                bootstrap_error = e
        if not capabilities.supports_bootstrap or bootstrap_error is not None:
            if capabilities.supports_init_from_jsonl:
                _run_bd(["init", "--from-jsonl", "--quiet"], cwd=repo_root)
            elif bootstrap_error is not None:
                raise bootstrap_error
            else:
                raise BdCliError(
                    f"bd workspace compatibility error for {repo_root}: found tracked .beads/issues.jsonl but "
                    "the installed bd does not support 'bootstrap' or 'init --from-jsonl'. Upgrade bd or "
                    "migrate the workspace manually before rerunning."
                )
    else:
        _run_bd(["init", "--quiet"], cwd=repo_root)

    prepared = bd_detect_capabilities(repo_root=repo_root)
    if _workspace_is_active(prepared):
        return prepared
    raise BdCliError(
        f"bd workspace preparation did not produce a recognized active database for {repo_root} "
        f"(layout={prepared.workspace_layout!r})."
    )


def bd_init(*, repo_root: Path) -> None:
    bd_prepare_workspace(repo_root=repo_root)


def bd_ready(*, repo_root: Path, limit: int = DEFAULT_BD_READY_LIMIT) -> list[ReadyBead]:
    if limit < 1:
        raise BdCliError(f"bd ready limit must be >= 1, got {limit}")

    data = _parse_json_output(_run_bd(["ready", "--json", "--limit", str(limit)], cwd=repo_root))
    if data is None:
        return []
    if not isinstance(data, list):
        raise BdCliError(f"bd ready --json: expected a list, got {type(data).__name__}")

    out: list[ReadyBead] = []
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise BdCliError(
                f"bd ready --json: expected objects, got {type(item).__name__} at index {idx}"
            )
        bead_id = item.get("id")
        title = item.get("title")
        if not isinstance(bead_id, str) or not bead_id.strip():
            raise BdCliError(f"bd ready --json: missing string id at index {idx}")
        if not isinstance(title, str) or not title.strip():
            raise BdCliError(f"bd ready --json: missing string title at index {idx}")
        issue_type = item.get("issue_type")
        if issue_type is not None and not isinstance(issue_type, str):
            issue_type = None
        labels_raw = item.get("labels", [])
        if labels_raw is None:
            labels_raw = []
        if not isinstance(labels_raw, list):
            raise BdCliError(f"bd ready --json: labels must be a list at index {idx}")
        labels = tuple(str(lbl) for lbl in labels_raw if isinstance(lbl, str))
        description = item.get("description", "") or ""
        out.append(
            ReadyBead(
                bead_id=bead_id,
                title=title,
                labels=labels,
                description=str(description),
                issue_type=issue_type,
            )
        )
    return out


def bd_doctor(*, repo_root: Path) -> BdDoctorResult:
    try:
        capabilities = bd_detect_capabilities(repo_root=repo_root)
    except BdCliError as e:
        return BdDoctorResult(
            status="error",
            capabilities=BdCapabilities(
                version=None,
                workspace_layout="unknown",
                supports_bootstrap=False,
                supports_init_from_jsonl=False,
                supports_sync=False,
                supports_structured_doctor_output=None,
            ),
            message=str(e),
        )

    if capabilities.workspace_layout in {"missing", "empty_beads_dir", "jsonl_only"}:
        return BdDoctorResult(
            status="skipped",
            capabilities=capabilities,
            message=_workspace_skip_reason(capabilities),
        )
    if capabilities.workspace_layout not in _ACTIVE_WORKSPACE_LAYOUTS:
        return BdDoctorResult(
            status="error",
            capabilities=capabilities,
            message=_workspace_error(capabilities, repo_root=repo_root).args[0],
        )

    try:
        stdout = _run_bd(["doctor", "--json"], cwd=repo_root, ok_exit_codes=(0, 1))
    except BdCliError as e:
        if _detect_unknown_command(e, "doctor"):
            return BdDoctorResult(
                status="unsupported",
                capabilities=capabilities,
                message="doctor_command_unavailable",
            )
        return BdDoctorResult(status="error", capabilities=capabilities, message=str(e))

    try:
        data = _parse_json_output(stdout)
    except BdCliError:
        return BdDoctorResult(
            status="unsupported",
            capabilities=replace(capabilities, supports_structured_doctor_output=False),
            raw_output=stdout,
            message="doctor_json_output_unavailable",
        )

    if data is None:
        return BdDoctorResult(
            status="unsupported",
            capabilities=replace(capabilities, supports_structured_doctor_output=False),
            raw_output=stdout,
            message="doctor_json_output_unavailable",
        )
    if not isinstance(data, dict):
        return BdDoctorResult(
            status="unsupported",
            capabilities=replace(capabilities, supports_structured_doctor_output=False),
            raw_output=stdout,
            message=f"doctor_json_expected_object_got_{type(data).__name__}",
        )

    checks = data.get("checks")
    failed_checks = 0
    if isinstance(checks, list):
        for item in checks:
            if not isinstance(item, dict):
                continue
            status = item.get("status")
            if isinstance(status, str) and status != "ok":
                failed_checks += 1
    overall_ok = data.get("overall_ok")
    return BdDoctorResult(
        status="warn" if overall_ok is False else "ok",
        capabilities=replace(capabilities, supports_structured_doctor_output=True),
        overall_ok=bool(overall_ok) if isinstance(overall_ok, bool) else None,
        failed_checks=failed_checks,
        raw_output=stdout,
    )


def bd_sync(*, repo_root: Path) -> BdSyncResult:
    try:
        capabilities = bd_detect_capabilities(repo_root=repo_root)
    except BdCliError as e:
        return BdSyncResult(
            status="error",
            capabilities=BdCapabilities(
                version=None,
                workspace_layout="unknown",
                supports_bootstrap=False,
                supports_init_from_jsonl=False,
                supports_sync=False,
                supports_structured_doctor_output=None,
            ),
            message=str(e),
        )

    if not capabilities.supports_sync:
        return BdSyncResult(
            status="unsupported",
            capabilities=capabilities,
            message="sync_command_unavailable",
        )
    if capabilities.workspace_layout in {"missing", "empty_beads_dir", "jsonl_only"}:
        return BdSyncResult(
            status="skipped",
            capabilities=capabilities,
            message=_workspace_skip_reason(capabilities),
        )
    if capabilities.workspace_layout not in _ACTIVE_WORKSPACE_LAYOUTS:
        return BdSyncResult(
            status="error",
            capabilities=capabilities,
            message=_workspace_error(capabilities, repo_root=repo_root).args[0],
        )

    try:
        stdout = _run_bd(["sync", "--json"], cwd=repo_root)
    except BdCliError as e:
        if _detect_unknown_command(e, "sync"):
            return BdSyncResult(
                status="unsupported",
                capabilities=capabilities,
                message="sync_command_unavailable",
            )
        return BdSyncResult(status="error", capabilities=capabilities, message=str(e))

    try:
        _parse_json_output(stdout)
    except BdCliError:
        return BdSyncResult(status="ok", capabilities=capabilities, raw_output=stdout)
    return BdSyncResult(status="ok", capabilities=capabilities, raw_output=stdout)


def bd_list(*, repo_root: Path) -> tuple[BdIssueSummary, ...]:
    data = _parse_json_output(_run_bd(["list", "--json"], cwd=repo_root))
    if data is None:
        return ()
    if not isinstance(data, list):
        raise BdCliError(f"bd list --json: expected a list, got {type(data).__name__}")

    out: list[BdIssueSummary] = []
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise BdCliError(
                f"bd list --json: expected objects, got {type(item).__name__} at index {idx}"
            )
        issue_id = item.get("id")
        title = item.get("title")
        status = item.get("status")
        if not isinstance(issue_id, str) or not issue_id.strip():
            raise BdCliError(f"bd list --json: missing string id at index {idx}")

        if (
            not isinstance(title, str)
            or not title.strip()
            or not isinstance(status, str)
            or not status.strip()
        ):
            issue = bd_show(repo_root=repo_root, issue_id=issue_id)
            title = issue.title
            status = issue.status

        out.append(
            BdIssueSummary(issue_id=issue_id, title=str(title), status=str(status))
        )

    return tuple(out)


def bd_list_ids(*, repo_root: Path) -> set[str]:
    data = _parse_json_output(_run_bd(["list", "--json"], cwd=repo_root))
    if data is None:
        return set()
    if not isinstance(data, list):
        raise BdCliError(f"bd list --json: expected a list, got {type(data).__name__}")

    out: set[str] = set()
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise BdCliError(
                f"bd list --json: expected objects, got {type(item).__name__} at index {idx}"
            )
        bead_id = item.get("id")
        if not isinstance(bead_id, str) or not bead_id.strip():
            raise BdCliError(f"bd list --json: missing string id at index {idx}")
        out.add(bead_id)
    return out


def bd_list_open_titles(*, repo_root: Path) -> set[str]:
    data = _parse_json_output(_run_bd(["list", "--json"], cwd=repo_root))
    if data is None:
        return set()
    if not isinstance(data, list):
        raise BdCliError(f"bd list --json: expected a list, got {type(data).__name__}")

    open_titles: set[str] = set()
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise BdCliError(
                f"bd list --json: expected objects, got {type(item).__name__} at index {idx}"
            )
        issue_id = item.get("id")
        if not isinstance(issue_id, str) or not issue_id.strip():
            raise BdCliError(f"bd list --json: missing string id at index {idx}")

        title = item.get("title")
        status = item.get("status")
        if (
            not isinstance(title, str)
            or not title.strip()
            or not isinstance(status, str)
            or not status.strip()
        ):
            issue = bd_show(repo_root=repo_root, issue_id=issue_id)
            title = issue.title
            status = issue.status

        if str(status).strip().lower() == "closed":
            continue
        open_titles.add(str(title))

    return open_titles


def bd_show(*, repo_root: Path, issue_id: str) -> BdIssue:
    data = _parse_json_output(_run_bd(["show", issue_id, "--json"], cwd=repo_root))
    return _parse_single_issue(data, context=f"bd show {issue_id} --json")


def bd_update(
    *,
    repo_root: Path,
    issue_id: str,
    status: str | None = None,
    notes: str | None = None,
) -> BdIssue:
    args: list[str] = ["update", issue_id]
    if status is not None:
        args.extend(["--status", status])
    if notes is not None:
        args.extend(["--notes", notes])
    args.append("--json")
    data = _parse_json_output(_run_bd(args, cwd=repo_root))
    return _parse_single_issue(data, context=f"bd update {issue_id} --json")


def bd_close(*, repo_root: Path, issue_id: str, reason: str) -> BdIssue:
    data = _parse_json_output(
        _run_bd(["close", issue_id, "--reason", reason, "--json"], cwd=repo_root)
    )
    return _parse_single_issue(data, context=f"bd close {issue_id} --json")


def bd_create(
    *,
    repo_root: Path,
    title: str,
    issue_type: str = "task",
    priority: int = 2,
    labels: tuple[str, ...] | None = None,
    description: str | None = None,
    acceptance_criteria: str | None = None,
    design: str | None = None,
    estimate_minutes: int | None = None,
    deps: tuple[str, ...] | None = None,
) -> BdIssue:
    args: list[str] = ["create", title, "-t", issue_type, "-p", str(priority)]
    if labels:
        args.extend(["--labels", ",".join(labels)])
    if description is not None:
        args.extend(["--description", description])
    if acceptance_criteria is not None:
        args.extend(["--acceptance", acceptance_criteria])
    if design is not None:
        args.extend(["--design", design])
    if estimate_minutes is not None and estimate_minutes > 0:
        args.extend(["--estimate", str(estimate_minutes)])
    if deps:
        args.extend(["--deps", ",".join(deps)])
    args.append("--json")
    data = _parse_json_output(_run_bd(args, cwd=repo_root))
    return _parse_single_issue(data, context="bd create --json")


def bd_dep_add(
    *,
    repo_root: Path,
    issue_id: str,
    depends_on_id: str,
    dep_type: str = "blocks",
) -> None:
    _run_bd(
        ["dep", "add", issue_id, depends_on_id, "-t", dep_type, "--json"],
        cwd=repo_root,
    )
