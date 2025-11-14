"""Chembl assay class map entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import ChemblClientProtocol, ChemblEntityFetcherBase

__all__ = ["ChemblAssayClassMapEntityClient"]


class ChemblAssayClassMapEntityClient(ChemblEntityFetcherBase):
    """Client for retrieving ``assay_class_map`` records from the ChEMBL API."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("assay_class_map")

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        super().__init__(chembl_client, config=self.ENTITY_CONFIG)

