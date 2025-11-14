"""Document-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl`."""

from __future__ import annotations

from typing import Any, ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import ChemblEntityFetcherBase

__all__ = ["ChemblDocumentClient"]


class ChemblDocumentClient(ChemblEntityFetcherBase):
    """High level helper focused on retrieving document payloads."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("document")

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Initialize the document client wrapper.

        Parameters
        ----------
        chembl_client:
            Instance of ChemblClient used to perform API calls.
        batch_size:
            Batch size for pagination (max 25 for the ChEMBL API).
        max_url_length:
            Optional URL length limit; disables the check when None.
        """
        super().__init__(
            chembl_client=chembl_client,
            config=self.ENTITY_CONFIG,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

