"""Нормализаторы для testitem."""


class TestItemNormalizer:
    """Заготовка нормализации testitem."""

    def normalize(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Нормализация testitem будет реализована позже")


__all__ = ["TestItemNormalizer"]
