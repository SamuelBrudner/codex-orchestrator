from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from codex_orchestrator.config_parsing import as_str, load_toml_table


class AiPolicyError(ValueError):
    pass


REQUIRED_CODEX_MODEL = "gpt-5.4"
REQUIRED_REASONING_EFFORT = "xhigh"


@dataclass(frozen=True, slots=True)
class AiSettings:
    model: str
    reasoning_effort: str

    def to_json_dict(self) -> dict[str, str]:
        return {"model": self.model, "reasoning_effort": self.reasoning_effort}


def load_ai_settings(config_path: Path) -> AiSettings:
    data = load_toml_table(
        config_path,
        error_type=AiPolicyError,
        missing_message="Config file not found: {path}",
        read_message="Failed to read config file: {path}",
        parse_message="Failed to parse TOML in {path}: {error}",
        table_message="Expected TOML document to be a table in {path}",
    )
    errors: list[str] = []

    allowed_top_level = {"ai"}
    unknown_top_level = set(data) - allowed_top_level
    if unknown_top_level:
        errors.append(
            f"Top-level: unknown keys {sorted(unknown_top_level)} "
            f"(allowed: {sorted(allowed_top_level)})"
        )

    ai_table = data.get("ai")
    if not isinstance(ai_table, dict):
        errors.append("ai: required table missing or malformed (expected [ai])")
        raise AiPolicyError("Invalid AI config:\n- " + "\n- ".join(errors))

    allowed_ai_keys = {"model", "reasoning_effort"}
    unknown_ai_keys = set(ai_table) - allowed_ai_keys
    if unknown_ai_keys:
        errors.append(
            f"ai: unknown keys {sorted(unknown_ai_keys)} (allowed: {sorted(allowed_ai_keys)})"
        )

    model = as_str(ai_table.get("model"), field="ai.model", errors=errors, required=True)
    reasoning_effort = as_str(
        ai_table.get("reasoning_effort"),
        field="ai.reasoning_effort",
        errors=errors,
        required=True,
    )

    if errors:
        raise AiPolicyError("Invalid AI config:\n- " + "\n- ".join(errors))
    assert model is not None
    assert reasoning_effort is not None
    return AiSettings(model=model, reasoning_effort=reasoning_effort)


def enforce_unattended_ai_policy(settings: AiSettings, *, config_path: Path) -> None:
    violations: list[str] = []
    if settings.model != REQUIRED_CODEX_MODEL:
        violations.append(
            f"ai.model must be {REQUIRED_CODEX_MODEL!r} (got {settings.model!r})"
        )
    if settings.reasoning_effort != REQUIRED_REASONING_EFFORT:
        violations.append(
            f"ai.reasoning_effort must be {REQUIRED_REASONING_EFFORT!r} "
            f"(got {settings.reasoning_effort!r})"
        )
    if not violations:
        return

    message = "\n".join(
        [
            "Unattended AI policy violation; refusing to start.",
            "",
            f"Config: {config_path.as_posix()}",
            "Violations:",
            *[f"- {v}" for v in violations],
            "",
            "Next action: set the required values in config/orchestrator.toml and re-run.",
        ]
    )
    raise AiPolicyError(message)


def codex_cli_args_for_settings(settings: AiSettings) -> tuple[str, ...]:
    # Keep this in one place so subprocess invocations and audit logs stay consistent.
    return (
        "--model",
        settings.model,
        "-c",
        f'reasoning_effort="{settings.reasoning_effort}"',
    )
