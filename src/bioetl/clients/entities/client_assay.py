"""Assay-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl_common`."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bioetl.clients.client_chembl_base import ChemblClientProtocol, EntityConfig
from bioetl.clients.client_chembl_iterator import ChemblEntityIteratorBase
from bioetl.clients.entities.client_assay_entity import ChemblAssayEntityClient

if TYPE_CHECKING:
    from bioetl.clients.client_chembl_common import ChemblClient

__all__ = ["ChemblAssayClient"]


class ChemblAssayClient(ChemblEntityIteratorBase):
    """High level helper focused on retrieving assay payloads."""

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
        config = EntityConfig(
            endpoint="/assay.json",
            filter_param="assay_chembl_id__in",
            id_key="assay_chembl_id",
            items_key="assays",
            log_prefix="assay",
            chunk_size=100,
            supports_list_result=False,
            base_endpoint_length=len("/assay.json?"),
            enable_url_length_check=True,
        )

        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

        self._entity_client = ChemblAssayEntityClient(chembl_client)

