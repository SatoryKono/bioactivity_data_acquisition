"""Shared helpers for ChEMBL enrichment configuration handling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from bioetl.core.logging import LogEvents

if TYPE_CHECKING:  # pragma: no cover - import guard for type checking only
    import pandas as pd
    from structlog.stdlib import BoundLogger

    from bioetl.clients.client_chembl import ChemblClient


EnrichmentTransform = Callable[
    [
        Any,
        "pd.DataFrame",
        "ChemblClient",
        Mapping[str, Any],
        "BoundLogger",
    ],
    "pd.DataFrame",
]


@dataclass(slots=True, frozen=True)
class ChemblEnrichmentScenario:
    """Declarative configuration for a single enrichment stage."""

    name: str
    entity_name: str
    config_path: tuple[str, ...]
    client_name: str
    transform: EnrichmentTransform
    enabled_path: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "config_path", tuple(self.config_path))
        if self.enabled_path is not None:
            object.__setattr__(self, "enabled_path", tuple(self.enabled_path))

    def flag_path(self) -> tuple[str, ...]:
        """Return the configuration path used to resolve the enabled flag."""

        if self.enabled_path is not None:
            return self.enabled_path
        return self.config_path + ("enabled",)

    def is_enabled(self, config: Mapping[str, Any] | None) -> bool:
        """Return ``True`` when the scenario is enabled in the provided config."""

        return enrich_flag(config, self.flag_path())

    def extract_config(self, config: Mapping[str, Any] | None, *, log: Any) -> dict[str, Any]:
        """Extract a mutable enrichment configuration mapping for this scenario."""

        return _extract_enrich_config(config, self.config_path, log=log)


def enrich_flag(config: Mapping[str, Any] | None, path: Sequence[str]) -> bool:
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


def _enrich_flag(config: Mapping[str, Any] | None, path: Sequence[str]) -> bool:
    """Backward-compatible alias for :func:`enrich_flag`."""

    return enrich_flag(config, path)


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
