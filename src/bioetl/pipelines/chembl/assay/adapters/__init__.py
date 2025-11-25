"""Адаптеры данных assay."""


class AssayAdapter:
    """Конвертация нормализованных ассев в финальный формат."""

    def adapt(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Адаптер assay будет реализован позже")


__all__ = ["AssayAdapter"]
