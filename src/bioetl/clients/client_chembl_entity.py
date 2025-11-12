"""Базовый клиент сущностей ChEMBL с предопределенной конфигурацией."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_chembl_base import (
    ChemblClientProtocol,
    ChemblEntityFetcherBase,
    EntityConfig,
)

__all__ = ["ChemblEntityClientBase"]

class ChemblEntityClientBase(ChemblEntityFetcherBase):
    """Базовый клиент, использующий EntityConfig, созданный через make_entity_config."""

    CONFIG: ClassVar[EntityConfig]

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        """Инициализировать клиент с конфигурацией, заданной на уровне класса и созданной через make_entity_config."""
        config = getattr(self.__class__, "CONFIG", None)
        if config is None:
            raise ValueError(f"{self.__class__.__name__}.CONFIG не задан")
        if not isinstance(config, EntityConfig):
            raise TypeError(
                f"{self.__class__.__name__}.CONFIG должен быть экземпляром EntityConfig, получено {type(config)!r}",
            )
        super().__init__(chembl_client=chembl_client, config=config)

