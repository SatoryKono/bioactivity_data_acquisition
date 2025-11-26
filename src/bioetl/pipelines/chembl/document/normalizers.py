"""Нормализаторы Document."""


class DocumentNormalizer:
    """Скелет нормализации документов (SemanticScholar/PubMed/CrossRef)."""

    def normalize(self, df):  # pragma: no cover - заглушка
        raise NotImplementedError("Нормализация document будет добавлена позже")


__all__ = ["DocumentNormalizer"]
