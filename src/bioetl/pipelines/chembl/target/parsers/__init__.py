"""Парсеры для target."""


class TargetPayloadParser:
    """Заглушка парсера target."""

    def parse(self, payload):  # pragma: no cover - заглушка
        raise NotImplementedError("Парсер target будет реализован позже")


__all__ = ["TargetPayloadParser"]
