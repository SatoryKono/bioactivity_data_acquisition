"""Assay entity client for ChEMBL API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bioetl.clients.chembl_base import ChemblClientProtocol, ChemblEntityFetcher, EntityConfig

if TYPE_CHECKING:
    from bioetl.clients import ChemblClient

__all__ = ["ChemblAssayEntityClient"]


class ChemblAssayEntityClient(ChemblEntityFetcher):
    """Клиент для получения assay записей из ChEMBL API."""

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        """Инициализировать клиент для assay.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        """
        config = EntityConfig(
            endpoint="/assay.json",
            filter_param="assay_chembl_id__in",
            id_key="assay_chembl_id",
            items_key="assays",
            log_prefix="assay",
            chunk_size=100,
        )
        super().__init__(chembl_client, config)
