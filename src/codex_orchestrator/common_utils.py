from __future__ import annotations

import shlex
from collections.abc import Iterable, Sequence
from typing import TypeVar

T = TypeVar("T")


def dedupe_preserve_order(items: Iterable[T]) -> tuple[T, ...]:
    out: list[T] = []
    seen: set[T] = set()
    for item in items:
        if item in seen:
            continue
        out.append(item)
        seen.add(item)
    return tuple(out)


def dedupe_preserve_order_list(items: Iterable[T]) -> list[T]:
    return list(dedupe_preserve_order(items))


def parse_command_argv(command: str) -> list[str] | None:
    try:
        argv = shlex.split(command)
    except ValueError:
        return None
    return argv or None


def validation_status(exit_code: int) -> str:
    return "ok" if exit_code == 0 else f"exit={exit_code}"


def clean_nonempty_strings(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(item for item in (value.strip() for value in values) if item)
