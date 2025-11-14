"""Configuration utility functions."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def resolve_existing_directory(directory: Path, *, base: Path) -> Path:
    """Resolve ``directory`` relative to ``base``/parents/current working dir."""

    if directory.is_absolute():
        return directory.resolve()

    search_roots: list[Path] = [base, *base.parents, Path.cwd()]
    for root in search_roots:
        candidate = (root / directory).resolve()
        if candidate.exists():
            return candidate

    return (Path.cwd() / directory).resolve()


def coerce_bool(value: Any) -> bool:
    """Best-effort conversion for boolean-like configuration values.

    Parameters
    ----------
    value
        Value to coerce to boolean.

    Returns
    -------
    bool
        Coerced boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return bool(value)
