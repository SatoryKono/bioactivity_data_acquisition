"""Activity-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl`."""

from __future__ import annotations

from typing import Any, ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import ChemblEntityFetcherBase

__all__ = ["ChemblActivityClient"]


class ChemblActivityClient(ChemblEntityFetcherBase):
    """High level helper focused on retrieving activity payloads."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("activity")

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        self._init_from_entity_config(
            self,
            chembl_client,
            entity_config=self.ENTITY_CONFIG,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

