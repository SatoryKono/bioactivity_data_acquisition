from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Mapping, Sequence


class TransformerABC(ABC):
    """Преобразует записи или DataFrame."""

    @abstractmethod
    def transform(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        """Возвращает преобразованный набор записей."""


class SideInputProviderABC(ABC):
    """Поставляет дополнительные данные для обогащения."""

    @abstractmethod
    def load(self) -> Mapping[str, object]:
        """Возвращает вспомогательные данные (например, справочники)."""


class LookupEnricherABC(ABC):
    """Обогащает записи по справочникам."""

    @abstractmethod
    def enrich(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        """Возвращает обогащенные записи."""


class BusinessKeyDeriverABC(ABC):
    """Определяет бизнес-ключи для записей."""

    @abstractmethod
    def derive(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        """Добавляет поля бизнес-ключей."""


class DeduplicatorABC(ABC):
    """Удаляет дубликаты на основе бизнес-правил."""

    @abstractmethod
    def deduplicate(self, records: Sequence[Mapping[str, object]]) -> Sequence[Mapping[str, object]]:
        """Возвращает набор без дубликатов."""


class MergeStrategyABC(ABC):
    """Стратегия объединения данных из разных источников."""

    @abstractmethod
    def merge(self, primary: Iterable[Mapping[str, object]], secondary: Iterable[Mapping[str, object]]) -> Iterable[Mapping[str, object]]:
        """Объединяет коллекции записей."""


class HasherABC(ABC):
    """Генерация хешей для идентификации записей."""

    @abstractmethod
    def hash(self, record: Mapping[str, object]) -> str:
        """Возвращает стабильный хеш для записи."""
