"""Base ChEMBL entity client built around a predefined configuration."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_config import EntityConfig
from bioetl.clients.client_chembl_base import ChemblClientProtocol, ChemblEntityFetcherBase

__all__ = ["ChemblEntityClientBase"]

class ChemblEntityClientBase(ChemblEntityFetcherBase):
    """Базовый клиент, использующий заранее объявленную ``EntityConfig``."""

    CONFIG: ClassVar[EntityConfig]

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        """Initialize the client with the class-level ``EntityConfig`` instance."""
        config = getattr(self.__class__, "CONFIG", None)
        if config is None:
            raise ValueError(f"{self.__class__.__name__}.CONFIG is not defined")
        if not isinstance(config, EntityConfig):
            raise TypeError(
                f"{self.__class__.__name__}.CONFIG must be an EntityConfig instance, got {type(config)!r}",
            )
        super().__init__(chembl_client=chembl_client, config=config)

