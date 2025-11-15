"""Mixin helpers for working with common collection patterns."""

from __future__ import annotations

from collections.abc import Collection, Sized
from typing import Any

import pandas as pd

__all__ = ["CollectionFlagMixin"]


class CollectionFlagMixin:
    """Предоставляет вспомогательный метод для проверки наличия элементов."""

    @staticmethod
    def has_items(collection: Any) -> bool:
        """Вернуть ``True``, если коллекция содержит элементы."""

        if collection is None:
            return False

        if isinstance(collection, (str, bytes, bytearray)):
            return len(collection) > 0

        if isinstance(collection, (pd.Series, pd.DataFrame, pd.Index)):
            return not collection.empty

        if isinstance(collection, Collection):
            return len(collection) > 0

        if isinstance(collection, Sized):
            return len(collection) > 0

        try:
            return bool(collection)
        except (TypeError, ValueError):
            return False
