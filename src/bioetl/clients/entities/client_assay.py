"""Assay-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl`."""

from __future__ import annotations

from typing import ClassVar, Mapping

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import (
    ChemblClientProtocol,
    ChemblEntityConfigMixin,
    ChemblEntityFetcherBase,
)

__all__ = ["ChemblAssayClient"]


class ChemblAssayClient(ChemblEntityConfigMixin, ChemblEntityFetcherBase):
    """High level helper focused on retrieving assay payloads."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("assay")
    MAX_BATCH_SIZE: ClassVar[int] = 25
    DEFAULT_HANDSHAKE_ENDPOINT: ClassVar[str] = "/status.json"
    REQUIRE_MAX_URL_LENGTH: ClassVar[bool] = True
    DEFAULT_BATCH_SIZE: ClassVar[int] = MAX_BATCH_SIZE

    def _normalize_batch_size(self, batch_size: int | None) -> int | None:
        if batch_size is None:
            return None
        return min(batch_size, self.MAX_BATCH_SIZE)

    def handshake(
        self,
        *,
        endpoint: str | None = None,
        enabled: bool = True,
    ) -> Mapping[str, Any]:
        """Perform handshake with the configured default endpoint."""

        effective_endpoint = endpoint or self.DEFAULT_HANDSHAKE_ENDPOINT
        return super().handshake(endpoint=effective_endpoint, enabled=enabled)

