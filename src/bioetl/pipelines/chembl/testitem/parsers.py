"""Парсеры для testitem."""


class TestItemPayloadParser:
    """Заготовка парсера входных данных testitem."""

    def parse(self, payload):  # pragma: no cover - заглушка
        """Разобрать сырой payload из ChEMBL/PubChem."""
        raise NotImplementedError("Реализация парсера будет добавлена позже")


__all__ = ["TestItemPayloadParser"]
