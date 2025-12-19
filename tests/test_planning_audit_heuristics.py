from __future__ import annotations

from pathlib import Path

from codex_orchestrator.planning_audit import build_planning_audit
from codex_orchestrator.repo_inventory import RepoPolicy


def _policy(*, tmp_path: Path, allowed_roots: tuple[Path, ...] = (Path("."),)) -> RepoPolicy:
    return RepoPolicy(
        repo_id="test_repo",
        path=tmp_path,
        base_branch="main",
        env=None,
        notebook_roots=(Path("notebooks"),),
        allowed_roots=allowed_roots,
        deny_roots=(),
        validation_commands=("pytest -q",),
        notebook_output_policy="strip",
    )


def _find_finding(audit: dict, *, title: str) -> dict:
    findings = audit.get("findings")
    if not isinstance(findings, list):
        raise AssertionError("audit.findings missing")
    for finding in findings:
        if isinstance(finding, dict) and finding.get("title") == title:
            return finding
    raise AssertionError(f"expected finding title={title!r}, got {[f.get('title') for f in findings if isinstance(f, dict)]}")


def test_planning_audit_detects_duplicate_model_shapes_across_paradigms(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text(
        "\n".join(
            [
                "from pydantic import BaseModel",
                "",
                "class UserDTO(BaseModel):",
                "    id: int",
                "    name: str",
                "    email: str",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "src" / "b.py").write_text(
        "\n".join(
            [
                "from dataclasses import dataclass",
                "",
                "@dataclass",
                "class User:",
                "    id: int",
                "    name: str",
                "    email: str",
                "",
            ]
        ),
        encoding="utf-8",
    )

    audit = build_planning_audit(run_id="run-1", repo_policy=_policy(tmp_path=tmp_path))
    finding = _find_finding(audit, title="Duplicated DTO/model shapes detected")

    assert finding["category"] == "semantic_modeling_dry"
    assert set(finding["evidence_paths"]) >= {"src/a.py", "src/b.py"}

    rationale = finding.get("confidence_rationale")
    assert isinstance(rationale, list)
    assert "signal:cross_paradigm_duplicates" in rationale

    details = finding.get("details")
    assert isinstance(details, dict)
    groups = details.get("duplicate_shape_groups")
    assert isinstance(groups, list)
    assert any(
        isinstance(g, dict)
        and g.get("fields") == ["email", "id", "name"]
        and int(g.get("count", 0) or 0) == 2
        for g in groups
    )


def test_planning_audit_detects_repeated_config_parsing_patterns(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    for name in ("a.py", "b.py", "c.py"):
        (tmp_path / "src" / name).write_text(
            "\n".join(
                [
                    "import os",
                    "VALUE = os.getenv('MY_SETTING')",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    audit = build_planning_audit(run_id="run-1", repo_policy=_policy(tmp_path=tmp_path))
    finding = _find_finding(audit, title="Repeated config parsing patterns detected")

    assert finding["category"] == "config_parsing_dry"
    assert set(finding["evidence_paths"]) >= {"src/a.py", "src/b.py", "src/c.py"}

    rationale = finding.get("confidence_rationale")
    assert isinstance(rationale, list)
    assert "signal:config_patterns_repeated_in_3+_files" in rationale

    details = finding.get("details")
    assert isinstance(details, dict)
    hot = details.get("hot_patterns")
    assert isinstance(hot, list)
    assert any(
        isinstance(item, dict)
        and item.get("pattern") == "os.getenv"
        and int(item.get("count", 0) or 0) == 3
        for item in hot
    )

