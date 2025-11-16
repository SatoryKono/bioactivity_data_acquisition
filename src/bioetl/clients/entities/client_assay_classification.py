"""Chembl assay classification entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import (
    ChemblEntityConfigMixin,
    ChemblEntityFetcherBase,
)

__all__ = ["ChemblAssayClassificationEntityClient"]


class ChemblAssayClassificationEntityClient(
    ChemblEntityConfigMixin, ChemblEntityFetcherBase
):
    """Client for retrieving ``assay_classification`` records from the ChEMBL API."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("assay_classification")

