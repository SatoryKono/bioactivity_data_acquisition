"""Нормализация данных assay."""


class AssayNormalizer:
    """Шаблон нормализации assay с поддержкой nested параметров."""

    def normalize(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Нормализация assay будет добавлена позже")


__all__ = ["AssayNormalizer"]
