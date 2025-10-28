"""Normalizer registry."""

from typing import Any

from bioetl.normalizers.base import BaseNormalizer


class NormalizerRegistry:
    """Реестр нормализаторов."""

    _registry: dict[str, BaseNormalizer] = {}

    @classmethod
    def register(cls, name: str, normalizer: BaseNormalizer) -> None:
        """Регистрирует нормализатор."""
        cls._registry[name] = normalizer

    @classmethod
    def get(cls, name: str) -> BaseNormalizer:
        """Получает нормализатор по имени."""
        if name not in cls._registry:
            raise ValueError(f"Normalizer {name} not found")
        return cls._registry[name]

    @classmethod
    def normalize(cls, name: str, value: Any) -> Any:
        """Нормализует значение через нормализатор."""
        normalizer = cls.get(name)
        return normalizer.safe_normalize(value)


# Инициализация
registry = NormalizerRegistry()

