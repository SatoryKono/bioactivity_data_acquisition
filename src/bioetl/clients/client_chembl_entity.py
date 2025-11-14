"""Base ChEMBL entity client built around a predefined configuration."""

from __future__ import annotations

from typing import ClassVar, cast

from bioetl.clients.chembl_config import EntityConfig
from bioetl.clients.client_chembl_base import ChemblClientProtocol, ChemblEntityFetcherBase

__all__ = ["ChemblEntityClientBase"]


class ChemblEntityClientBase(ChemblEntityFetcherBase):
    """Базовый клиент, использующий заранее объявленную ``EntityConfig``.

    Упрощает создание клиентов сущностей: вместо передачи EntityConfig
    в конструктор, конфигурация объявляется на уровне класса через
    атрибут ENTITY_CONFIG (или legacy CONFIG для обратной совместимости).

    Parameters
    ----------
    chembl_client : ChemblClientProtocol
        Клиент ChEMBL для выполнения HTTP-запросов.

    Attributes
    ----------
    CONFIG : ClassVar[EntityConfig]
        Устаревший атрибут для обратной совместимости. Используйте ENTITY_CONFIG.
    ENTITY_CONFIG : ClassVar[EntityConfig]
        Конфигурация сущности, объявленная на уровне класса. Должна быть задана
        в подклассах.

    Raises
    ------
    ValueError
        Если ни ENTITY_CONFIG, ни CONFIG не заданы в классе.
    TypeError
        Если ENTITY_CONFIG или CONFIG не являются экземплярами EntityConfig.

    Examples
    --------
    >>> from bioetl.clients.chembl_config import get_entity_config
    >>> class MyEntityClient(ChemblEntityClientBase):
    ...     ENTITY_CONFIG = get_entity_config("activity")
    >>> client = MyEntityClient(chembl_client)
    """

    # CONFIG оставлен для обратной совместимости, ENTITY_CONFIG — новый контракт.
    CONFIG: ClassVar[EntityConfig] = cast(EntityConfig, None)
    ENTITY_CONFIG: ClassVar[EntityConfig] = cast(EntityConfig, None)

    def __init__(self, chembl_client: ChemblClientProtocol) -> None:
        """Инициализирует клиент, используя class-level конфигурацию сущности.

        Parameters
        ----------
        chembl_client : ChemblClientProtocol
            Клиент ChEMBL для выполнения HTTP-запросов.

        Raises
        ------
        ValueError
            Если ни ENTITY_CONFIG, ни CONFIG не заданы в классе.
        TypeError
            Если ENTITY_CONFIG или CONFIG не являются экземплярами EntityConfig.
        """
        config = self._resolve_entity_config()
        super().__init__(chembl_client=chembl_client, config=config)

    @classmethod
    def _resolve_entity_config(cls) -> EntityConfig:
        """Возвращает EntityConfig, объявленную на уровне класса.

        Предпочитает ``ENTITY_CONFIG`` и поддерживает ``CONFIG`` для совместимости.

        Returns
        -------
        EntityConfig
            Конфигурация сущности из атрибута класса.

        Raises
        ------
        ValueError
            Если ни ENTITY_CONFIG, ни CONFIG не заданы в классе.
        TypeError
            Если ENTITY_CONFIG или CONFIG не являются экземплярами EntityConfig.
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

