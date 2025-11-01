"""Shared document source pipeline utilities."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

__all__ = [
    "FieldSpec",
    "AdapterDefinition",
    "ExternalEnrichmentResult",
]


@dataclass(frozen=True)
class FieldSpec:
    """Specification describing how to resolve a configuration attribute."""

    default: Any | None = None
    default_factory: Callable[[], Any] | None = None
    env: str | None = None
    coalesce_default_on_blank: bool = False

    def get_default(self) -> Any:
        """Return a copy of the default value for this field."""

        if self.default_factory is not None:
            return self.default_factory()
        return deepcopy(self.default)


@dataclass(frozen=True)
class AdapterDefinition:
    """Definition for constructing external enrichment adapters."""

    adapter_cls: type[Any]
    api_fields: dict[str, FieldSpec]
    adapter_fields: dict[str, FieldSpec]


@dataclass
class ExternalEnrichmentResult:
    """Container describing the outcome of an external enrichment request."""

    dataframe: pd.DataFrame
    status: str
    errors: dict[str, str]
    requested_count: int = 0
    matched_count: int = 0
    missing_ids: dict[str, list[str]] = field(default_factory=dict)
    coverage: dict[str, float] = field(default_factory=dict)

    def has_errors(self) -> bool:
        """Return ``True`` when at least one adapter reported an error."""

        return bool(self.errors)

    @property
    def missing_count(self) -> int:
        """Return the number of identifiers that remain unresolved."""

        remaining = self.requested_count - self.matched_count
        return remaining if remaining > 0 else 0
