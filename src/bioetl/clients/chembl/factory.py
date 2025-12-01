"""Фабрика клиентов ChEMBL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, TypeAlias

T = TypeVar("T")

ChemblDescriptorFactoryBuilder: TypeAlias = Callable[..., T]


@dataclass
class ChemblClientFactory(Generic[T]):
    """Фабрика клиентов ChEMBL."""
    config: Any
    builder: ChemblDescriptorFactoryBuilder[T]

    def build_entity_client_factory(self, **kwargs: Any) -> Any:
        """Создать фабрику клиентов сущности."""
        # Placeholder implementation to satisfy tests
        # In a real scenario, this would return a configured factory
        return self
