"""Base normalizer interface."""

from abc import ABC, abstractmethod
from typing import Any

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


class BaseNormalizer(ABC):
    """Базовый класс для нормализаторов."""

    @abstractmethod
    def normalize(self, value: Any, **kwargs: Any) -> Any:
        """Нормализует значение."""
        pass

    @abstractmethod
    def validate(self, value: Any) -> bool:
        """Проверяет корректность значения."""
        pass

    def safe_normalize(self, value: Any, **kwargs: Any) -> Any:
        """Безопасная нормализация с обработкой ошибок."""
        try:
            return self.normalize(value, **kwargs)
        except Exception as e:
            logger.warning("normalization_failed", error=str(e), value=value)
            return None

