"""Shared helpers for ChEMBL enrichment configuration handling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from bioetl.core.logging import LogEvents


def _enrich_flag(config: Mapping[str, Any] | None, path: Sequence[str]) -> bool:
    """Return True when an ``enabled`` flag is set for the provided config path."""
    if not config:
        return False

    current: Any = config
    try:
        for key in path:
            current = current.get(key)  # type: ignore[union-attr]
    except (AttributeError, KeyError, TypeError):
        return False

    return bool(current) if current is not None else False


def _extract_enrich_config(
    config: Mapping[str, Any] | None,
    path: Sequence[str],
    *,
    log: Any,
) -> dict[str, Any]:
    """Safely extract enrichment configuration mapping with logging on failure."""
    if not config:
        return {}

    current: Any = config
    try:
        for key in path:
            current = current.get(key)  # type: ignore[union-attr]
    except (AttributeError, KeyError, TypeError) as exc:
        log.warning(
            LogEvents.ENRICHMENT_CONFIG_ERROR,
            error=str(exc),
            message="Using default enrichment config",
        )
        return {}

    if isinstance(current, Mapping):
        return dict(current)

    return {}
