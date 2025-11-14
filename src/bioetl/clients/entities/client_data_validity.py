"""Chembl data validity entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import ChemblClientProtocol, ChemblEntityFetcherBase

__all__ = ["ChemblDataValidityEntityClient"]


class ChemblDataValidityEntityClient(ChemblEntityFetcherBase):
    """Client for retrieving ``data_validity_lookup`` records from the ChEMBL API."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("data_validity_lookup")

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        super().__init__(chembl_client, config=self.ENTITY_CONFIG)

