"""Адаптеры для document."""


class DocumentAdapter:
    """Подготовка enriched документов к записи."""

    def adapt(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Адаптер document будет реализован позже")


__all__ = ["DocumentAdapter"]
