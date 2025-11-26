"""Нормализаторы target."""


class TargetNormalizer:
    """Заготовка нормализации target с учётом контролируемых словарей."""

    def normalize(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Нормализация target будет добавлена позже")


__all__ = ["TargetNormalizer"]
