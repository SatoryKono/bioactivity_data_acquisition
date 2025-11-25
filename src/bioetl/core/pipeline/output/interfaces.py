from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Mapping

from ..dto import WriteResult


class PathStrategyABC(ABC):
    """Определяет расположение выходных артефактов."""

    @abstractmethod
    def resolve_path(self, dataset: str, run_id: str) -> str:
        """Возвращает путь/URI для вывода."""


class WriterABC(ABC):
    """Пишет данные в целевое хранилище."""

    @abstractmethod
    def write(self, dataset: str, records: object) -> WriteResult:
        """Сохраняет данные и возвращает результат записи."""


class MetadataWriterABC(ABC):
    """Сохраняет вспомогательные метаданные для аудита."""

    @abstractmethod
    def write_metadata(self, dataset: str, metadata: Mapping[str, object]) -> WriteResult:
        """Записывает метаданные о процессе загрузки."""
