"""Deprecated compatibility layer for :mod:`bioactivity.clients`."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from library._compat import reexport

reexport(
    "library.clients",
    "bioactivity.clients",
    globals(),
    aliases={
        "ChemblClient": "ChEMBLClient",
        "PubmedClient": "PubMedClient",
    },
)


@dataclass(slots=True)
class ClientConfig:
    """Legacy configuration object retained for compatibility."""

    name: str
    base_url: str
    api_key: str | None = None
    rate_limit_per_minute: int | None = None
    extra_headers: Mapping[str, str] = field(default_factory=dict)


# Preserve historical aliases that downstream projects may still import.
BasePublicationsClient = BaseApiClient  # type: ignore[misc]
__all__.extend(["BasePublicationsClient", "ClientConfig"])
