"""Base ChEMBL entity client built around a predefined configuration."""

from __future__ import annotations

from typing import ClassVar, cast

from bioetl.clients.chembl_config import EntityConfig
from bioetl.clients.client_chembl_base import ChemblClientProtocol, ChemblEntityFetcherBase

__all__ = ["ChemblEntityClientBase"]


class ChemblEntityClientBase(ChemblEntityFetcherBase):
    """Базовый клиент, использующий заранее объявленную ``EntityConfig``."""

    # CONFIG оставлен для обратной совместимости, ENTITY_CONFIG — новый контракт.
    CONFIG: ClassVar[EntityConfig] = cast(EntityConfig, None)
    ENTITY_CONFIG: ClassVar[EntityConfig] = cast(EntityConfig, None)

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        """Инициализирует клиент, используя class-level конфигурацию сущности."""
        config = self._resolve_entity_config()
        super().__init__(chembl_client=chembl_client, config=config)

    @classmethod
    def _resolve_entity_config(cls) -> EntityConfig:
        """Вернуть EntityConfig, объявленную на уровне класса.

        Предпочитает ``ENTITY_CONFIG`` и поддерживает ``CONFIG`` для совместимости.
        """
        candidate = cls.ENTITY_CONFIG if cls.ENTITY_CONFIG is not None else cls.CONFIG
        if candidate is None:
            msg = (
                f"{cls.__name__} отсутствует ENTITY_CONFIG (или legacy CONFIG); "
                "укажите EntityConfig на уровне класса."
            )
            raise ValueError(msg)
        if not isinstance(candidate, EntityConfig):
            msg = (
                f"{cls.__name__}.ENTITY_CONFIG (или CONFIG) должен быть EntityConfig, "
                f"получено {type(candidate)!r}"
            )
            raise TypeError(msg)
        return candidate

