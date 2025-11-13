"""Assay entity client for ChEMBL API."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblAssayEntityClient"]


class ChemblAssayEntityClient(ChemblEntityClientBase):
    """Client for fetching assay entries from the ChEMBL API."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("assay")

