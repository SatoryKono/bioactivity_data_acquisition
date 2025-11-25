from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Mapping, MutableMapping


class ErrorAction(Enum):
    """Доступные действия при ошибках."""

    FAIL = "fail"
    SKIP = "skip"
    RETRY = "retry"


class ConfigResolverABC(ABC):
    """Загружает и резолвит конфигурацию пайплайна."""

    @abstractmethod
    def load(self, path: str) -> Mapping[str, Any]:
        """Возвращает словарь конфигурации из файла."""


class SecretProviderABC(ABC):
    """Получает секреты (API-ключи, токены)."""

    @abstractmethod
    def get(self, name: str) -> str:
        """Возвращает секрет по имени."""


class CacheABC(ABC):
    """Кэширует промежуточные результаты."""

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """Возвращает значение по ключу или ``None``."""

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение по ключу."""


class LoggerAdapterABC(ABC):
    """Адаптер для структурированного логирования."""

    @abstractmethod
    def info(self, message: str, **kwargs: Any) -> None:
        """Логирует информационное сообщение."""

    @abstractmethod
    def warning(self, message: str, **kwargs: Any) -> None:
        """Логирует предупреждение."""

    @abstractmethod
    def error(self, message: str, **kwargs: Any) -> None:
        """Логирует ошибку."""


class TracerABC(ABC):
    """Адаптер для трассировки (open telemetry и т.п.)."""

    @abstractmethod
    def start_span(self, name: str, **kwargs: Any) -> object:
        """Создает новый span."""


class ErrorPolicyABC(ABC):
    """Описывает стратегию обработки ошибок."""

    @abstractmethod
    def decide(self, error: Exception) -> ErrorAction:
        """Возвращает действие для заданной ошибки."""


class ProgressReporterABC(ABC):
    """Сообщает о прогрессе выполнения задач."""

    @abstractmethod
    def start(self, total: int | None = None) -> None:
        """Инициализирует отчетчик прогресса."""

    @abstractmethod
    def advance(self, step: int = 1, metadata: MutableMapping[str, Any] | None = None) -> None:
        """Продвигает прогресс на указанное количество шагов."""

    @abstractmethod
    def finish(self) -> None:
        """Завершает отчет прогресса."""
