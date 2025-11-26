"""Парсеры для Document обогащения."""


class DocumentPayloadParser:
    """Заглушка парсера входных документов."""

    def parse(self, payload):  # pragma: no cover - заглушка
        raise NotImplementedError("Парсер document будет реализован позже")


__all__ = ["DocumentPayloadParser"]
