"""Shared helper functions for configuration modules."""

from __future__ import annotations

from collections.abc import Iterable, MutableMapping
from pathlib import Path
from typing import Any, Sequence


def coerce_bool(value: Any) -> bool:
    """Best-effort conversion for boolean-like configuration values."""

    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def resolve_directory(directory: Path, *, base: Path) -> Path:
    """Resolve ``directory`` relative to ``base``/parents/current working dir."""

    if directory.is_absolute():
        return directory.resolve()

    search_roots: list[Path] = [base, *base.parents, Path.cwd()]
    seen: set[Path] = set()
    for root in search_roots:
        candidate = (root / directory).resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            return candidate

    return (Path.cwd() / directory).resolve()


def build_env_overrides(
    pairs: Iterable[tuple[Sequence[str], Any]],
) -> dict[str, Any]:
    """Construct a nested mapping from ``pairs`` of path segments and values."""

    tree: dict[str, Any] = {}

    for raw_parts, value in pairs:
        parts = tuple(str(part) for part in raw_parts)
        if not parts:
            continue

        current: MutableMapping[str, Any] = tree
        for part in parts[:-1]:
            existing = current.get(part)
            if isinstance(existing, MutableMapping):
                current = existing
            else:
                next_level: dict[str, Any] = {}
                current[part] = next_level
                current = next_level

        current[parts[-1]] = value

    return tree


__all__ = ["build_env_overrides", "coerce_bool", "resolve_directory"]

