from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping

if TYPE_CHECKING:  # pragma: no cover
    from .environment import EnvironmentSettings


def normalize_env_name(value: str | None) -> str | None:
    """Normalize environment name to lower-case stripped string."""

    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


def normalize_tool(value: str | None) -> str | None:
    """Trim PubMed tool value and convert blank strings to ``None``."""

    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def resolve_vocab_store(value: Path | None) -> Path | None:
    """Expand user and resolve vocab store path if provided."""

    if value is None:
        return None
    return value.expanduser().resolve()


def coerce_bool(value: Any) -> bool:
    """Convert common textual booleans into ``bool`` values."""

    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def validate_email(value: str | None) -> str | None:
    """Validate email-like value and normalize blanks to ``None``."""

    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if "@" not in normalized:
        msg = "email address must contain '@'"
        raise ValueError(msg)
    return normalized


def parse_env_file(env_file: Path | None) -> dict[str, str]:
    """Parse ``.env``-style files into a mapping."""

    if env_file is None or not env_file.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        values[key.strip()] = value.strip()
    return values


class EnvironmentProvider:
    """Абстракция для доступа к настройкам окружения и .env файлам."""

    def load_env_file_values(self, env_file: Path | None) -> Mapping[str, str]:
        """Загрузить значения из ``env_file``."""

        raise NotImplementedError

    def load_environment_settings(self, env_file: Path | None = None) -> "EnvironmentSettings":
        """Загрузить ``EnvironmentSettings`` из ``env_file`` и/или процесса."""

        raise NotImplementedError


class DefaultEnvironmentProvider(EnvironmentProvider):
    """Стандартный провайдер окружения на базе pydantic settings."""

    def __init__(self, loader: Callable[..., "EnvironmentSettings"]) -> None:
        self._loader = loader

    def load_env_file_values(self, env_file: Path | None) -> Mapping[str, str]:
        return parse_env_file(env_file)

    def load_environment_settings(self, env_file: Path | None = None) -> "EnvironmentSettings":
        return self._loader(env_file=env_file)


__all__ = [
    "DefaultEnvironmentProvider",
    "EnvironmentProvider",
    "coerce_bool",
    "normalize_env_name",
    "normalize_tool",
    "parse_env_file",
    "resolve_vocab_store",
    "validate_email",
]
