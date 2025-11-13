"""Chembl molecule entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblMoleculeEntityClient"]


class ChemblMoleculeEntityClient(ChemblEntityClientBase):
    """Client for retrieving ``molecule`` records from the ChEMBL API."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("molecule")

