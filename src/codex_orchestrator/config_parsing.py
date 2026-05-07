from __future__ import annotations

from pathlib import Path
from typing import Any, TypeVar

ErrorT = TypeVar("ErrorT", bound=Exception)
_MISSING = object()


def load_toml_table(
    path: Path,
    *,
    error_type: type[ErrorT],
    missing_message: str,
    read_message: str,
    parse_message: str,
    table_message: str,
) -> dict[str, Any]:
    try:
        import tomllib  # pyright: ignore[reportMissingImports]
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib  # type: ignore[no-redef]

    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError as e:
        raise error_type(missing_message.format(path=path)) from e
    except OSError as e:
        raise error_type(read_message.format(path=path, error=e)) from e
    except Exception as e:  # tomllib.TOMLDecodeError is not public across tomli/tomllib
        raise error_type(parse_message.format(path=path, error=e)) from e

    if not isinstance(data, dict):
        raise error_type(table_message.format(path=path))
    return data


def as_str(
    value: Any,
    *,
    field: str,
    errors: list[str],
    required: bool = False,
) -> str | None:
    if value is None:
        if required:
            errors.append(f"{field}: required field missing")
        return None
    if not isinstance(value, str):
        errors.append(f"{field}: expected string, got {type(value).__name__}")
        return None
    if not value.strip():
        errors.append(f"{field}: must be non-empty")
        return None
    return value


def as_bool(value: Any, *, field: str, errors: list[str]) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        errors.append(f"{field}: expected bool, got {type(value).__name__}")
        return None
    return value


def as_int(value: Any, *, field: str, errors: list[str]) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        errors.append(f"{field}: expected int, got {type(value).__name__}")
        return None
    return value


def as_str_list(value: Any, *, field: str, errors: list[str]) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        errors.append(f"{field}: expected list[str], got {type(value).__name__}")
        return None
    out: list[str] = []
    for idx, item in enumerate(value):
        if not isinstance(item, str):
            errors.append(f"{field}[{idx}]: expected string, got {type(item).__name__}")
            continue
        if not item.strip():
            errors.append(f"{field}[{idx}]: must be non-empty")
            continue
        out.append(item)
    return out


def as_rel_paths(
    value: Any,
    *,
    field: str,
    errors: list[str],
    default: tuple[Path, ...] | object = _MISSING,
    noun: str = "path",
) -> tuple[Path, ...] | None:
    items = as_str_list(value, field=field, errors=errors)
    if items is None:
        if default is _MISSING:
            return None
        return default  # type: ignore[return-value]

    out: list[Path] = []
    for idx, item in enumerate(items):
        p = Path(item)
        if p.is_absolute():
            errors.append(f"{field}[{idx}]: must be a relative {noun}, got {item!r}")
            continue
        if ".." in p.parts:
            errors.append(f"{field}[{idx}]: must not contain '..', got {item!r}")
            continue
        out.append(p)
    return tuple(out)


def as_rel_globs(
    value: Any,
    *,
    field: str,
    default: tuple[str, ...],
    errors: list[str],
) -> tuple[str, ...]:
    items = as_str_list(value, field=field, errors=errors)
    if items is None:
        return default

    out: list[str] = []
    for idx, item in enumerate(items):
        p = Path(item)
        if p.is_absolute():
            errors.append(f"{field}[{idx}]: must be a relative path glob, got {item!r}")
            continue
        if ".." in p.parts:
            errors.append(f"{field}[{idx}]: must not contain '..', got {item!r}")
            continue
        out.append(item)
    return tuple(out)
