"""Target-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl`."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import (
    ChemblEntityConfigMixin,
    ChemblEntityFetcherBase,
)

__all__ = ["ChemblTargetClient"]


class ChemblTargetClient(ChemblEntityConfigMixin, ChemblEntityFetcherBase):
    """High level helper focused on retrieving target payloads."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("target")
    DEFAULT_BATCH_SIZE: ClassVar[int] = 25

