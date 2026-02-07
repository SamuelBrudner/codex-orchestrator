from __future__ import annotations

import codex_orchestrator.orchestrator_cycle as orchestrator_cycle


def test_default_max_parallel_caps_to_four(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator_cycle.os, "cpu_count", lambda: 16)
    assert orchestrator_cycle._default_max_parallel(10) == 4


def test_default_max_parallel_uses_cpu_when_low(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator_cycle.os, "cpu_count", lambda: 2)
    assert orchestrator_cycle._default_max_parallel(10) == 2


def test_default_max_parallel_returns_one_for_empty_repo_list(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator_cycle.os, "cpu_count", lambda: 8)
    assert orchestrator_cycle._default_max_parallel(0) == 1
