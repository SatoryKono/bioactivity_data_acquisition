"""Centralised registry for data normalisers used across the ETL runtime."""

from __future__ import annotations

from typing import Any, Iterable

from bioetl.normalizers.base import BaseNormalizer


class NormalizerRegistry:
    """Store and expose normalisers by symbolic name."""

    def __init__(self) -> None:
        # ``dict`` preserves insertion order in CPython which keeps registry dumps
        # deterministic for golden tests.
        self._registry: dict[str, BaseNormalizer] = {}

    def register(self, name: str, normalizer: BaseNormalizer) -> None:
        """Register *normalizer* under ``name`` replacing previous entries."""

        if not isinstance(name, str) or not name:
            raise ValueError("Normalizer name must be a non-empty string")
        if not isinstance(normalizer, BaseNormalizer):
            raise TypeError(
                "normalizer must inherit from BaseNormalizer"
            )
        self._registry[name] = normalizer

    def register_many(self, mapping: dict[str, BaseNormalizer]) -> None:
        """Bulk-register multiple normalisers."""

        for name, normalizer in mapping.items():
            self.register(name, normalizer)

    def get(self, name: str) -> BaseNormalizer:
        """Return the normaliser previously registered under ``name``."""

        try:
            return self._registry[name]
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Normalizer {name} not found") from exc

    def normalize(self, name: str, value: Any, **kwargs: Any) -> Any:
        """Normalise ``value`` using the registered normaliser ``name``."""

        normalizer = self.get(name)
        return normalizer.safe_normalize(value, **kwargs)

    def __contains__(self, name: str) -> bool:  # pragma: no cover - trivial proxy
        return name in self._registry

    def __iter__(self) -> Iterable[str]:  # pragma: no cover - trivial proxy
        return iter(self._registry)


# Global registry instance shared across the project.
registry = NormalizerRegistry()
