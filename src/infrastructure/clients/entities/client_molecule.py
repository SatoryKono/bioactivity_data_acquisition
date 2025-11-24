"""Chembl molecule entity client."""

from __future__ import annotations

from typing import ClassVar

from infrastructure.clients.chembl_config import EntityConfig, get_entity_config
from infrastructure.clients.client_chembl_entity_base import (
    ChemblEntityConfigMixin,
    ChemblEntityFetcherBase,
)

__all__ = ["ChemblMoleculeEntityClient"]


class ChemblMoleculeEntityClient(
    ChemblEntityConfigMixin, ChemblEntityFetcherBase
):
    """Client for retrieving ``molecule`` records from the ChEMBL API."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("molecule")
