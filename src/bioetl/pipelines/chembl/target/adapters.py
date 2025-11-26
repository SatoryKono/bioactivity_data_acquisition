"""Адаптеры для target."""


class TargetAdapter:
    """Формирование выходной структуры target."""

    def adapt(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Адаптер target будет реализован позже")


__all__ = ["TargetAdapter"]
