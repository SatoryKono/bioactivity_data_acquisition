"""Reusable helpers for отслеживания релиза ChEMBL на уровне пайплайнов.

Mixin предоставляет единое хранение и обновление `chembl_release` для всех
ChEMBL-пайплайнов и итераторов, устраняя дублирование кода в отдельных
классах.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ChemblReleaseTracker(Protocol):
    """Контракт для объектов, предоставляющих доступ к `chembl_release`."""

    _chembl_release: str | None

    @property
    def chembl_release(self) -> str | None:
        """Возвращает кешированный номер релиза ChEMBL."""

    def _update_release(self, value: str | None) -> None:
        """Обновляет кеш релиза ChEMBL."""


class ChemblReleaseMixin:
    """Mixin с реализацией `ChemblReleaseTracker`."""

    _chembl_release: str | None

    def __init__(self, *args: object, **kwargs: object) -> None:
        self._chembl_release = None
        super().__init__(*args, **kwargs)

    @property
    def chembl_release(self) -> str | None:
        """Возвращает актуальный релиз ChEMBL, если он был зафиксирован."""

        return self._chembl_release

    def _update_release(self, value: str | None) -> None:
        """Сохраняет номер релиза ChEMBL (может быть None при сбое handshake)."""

        self._chembl_release = value if value else None

