"""Парсеры payload для assay."""


class AssayPayloadParser:
    """Заготовка разбора assay_parameter.json и связанных объектов."""

    def parse(self, payload):  # pragma: no cover - заглушка
        raise NotImplementedError("Парсер assay будет реализован позже")


__all__ = ["AssayPayloadParser"]
