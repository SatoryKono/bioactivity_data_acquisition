"""Assay-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl`."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import ChemblClientProtocol, ChemblEntityFetcherBase
from bioetl.clients.entities.client_assay_entity import ChemblAssayEntityClient

if TYPE_CHECKING:
    from bioetl.clients.client_chembl import ChemblClient

__all__ = ["ChemblAssayClient"]


class ChemblAssayClient(ChemblEntityFetcherBase):
    """High level helper focused on retrieving assay payloads."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("assay")

    def __init__(
        self,
        chembl_client: ChemblClient | ChemblClientProtocol,
        *,
        batch_size: int,
        max_url_length: int,
    ) -> None:
        """Initialize Chembl assay client.

        Parameters
        ----------
        chembl_client:
            Instance of ChemblClient used for HTTP requests.
        batch_size:
            Batch size for pagination (maximum 25 for the ChEMBL API).
        max_url_length:
            Maximum URL length enforced during request preparation.
        """
        super().__init__(
            chembl_client=chembl_client,
            config=self.ENTITY_CONFIG,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

        self._entity_client = ChemblAssayEntityClient(chembl_client)

