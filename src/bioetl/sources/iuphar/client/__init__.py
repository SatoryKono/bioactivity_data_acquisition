"""HTTP client helpers for the IUPHAR source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.core.api_client import UnifiedAPIClient

__all__ = ["DEFAULT_IUPHAR_BASE_URL", "IupharClient"]

DEFAULT_IUPHAR_BASE_URL = "https://www.guidetopharmacology.org/services"


@dataclass(slots=True)
class IupharClient:
    """Light-weight wrapper around :class:`UnifiedAPIClient` for IUPHAR."""

    api: UnifiedAPIClient

    def request_json(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        """Proxy JSON requests to the underlying API client."""

        return self.api.request_json(path, params=params)

    def close(self) -> None:
        """Close the underlying HTTP session."""

        self.api.close()
