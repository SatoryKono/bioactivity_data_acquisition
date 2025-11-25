"""Адаптеры для интеграции testitem."""


class TestItemAdapter:
    """Преобразование нормализованных данных в целевой формат."""

    def adapt(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Адаптер testitem будет добавлен позднее")


__all__ = ["TestItemAdapter"]
