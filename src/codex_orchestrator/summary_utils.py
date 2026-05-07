from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def summary_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def summary_list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def summary_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if text:
            out.append(text)
    return out


def merge_unique_strings(*lists: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for values in lists:
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            merged.append(value)
    return merged


def merge_records_by_keys(
    existing: list[dict[str, Any]],
    current: list[dict[str, Any]],
    *,
    key_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    index_by_key: dict[tuple[str, str], int] = {}

    def _record_key(item: Mapping[str, Any]) -> tuple[str, str] | None:
        for field in key_fields:
            value = item.get(field)
            if isinstance(value, str) and value.strip():
                return (field, value.strip())
        return None

    for source in (existing, current):
        for item in source:
            record = dict(item)
            record_key = _record_key(record)
            if record_key is None:
                merged.append(record)
                continue
            idx = index_by_key.get(record_key)
            if idx is None:
                index_by_key[record_key] = len(merged)
                merged.append(record)
                continue
            merged[idx].update(record)
    return merged
