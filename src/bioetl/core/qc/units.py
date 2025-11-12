"""Детерминированные распределения для QC-отчетов."""

from __future__ import annotations

from typing import ClassVar, Sequence

import pandas as pd

from bioetl.qc.metrics import compute_categorical_distributions


class QCUnits:
    """Утилиты для расчета распределений ``*_units`` и ``*_relation``."""

    UNITS_SUFFIXES: ClassVar[tuple[str, ...]] = ("units",)
    RELATION_SUFFIXES: ClassVar[tuple[str, ...]] = ("relation",)
    TOP_N: ClassVar[int] = 20
    RATIO_PRECISION: ClassVar[int] = 6
    OTHER_BUCKET: ClassVar[str] = "__other__"

    @classmethod
    def for_units(cls, df: pd.DataFrame) -> dict[str, dict[str, dict[str, float | int]]]:
        """Вернуть распределения значений для всех ``*_units`` столбцов."""

        return cls._compute(df, column_suffixes=cls.UNITS_SUFFIXES)

    @classmethod
    def for_relation(cls, df: pd.DataFrame) -> dict[str, dict[str, dict[str, float | int]]]:
        """Вернуть распределения значений для всех ``*_relation`` столбцов."""

        return cls._compute(df, column_suffixes=cls.RELATION_SUFFIXES)

    @classmethod
    def _compute(
        cls,
        df: pd.DataFrame,
        *,
        column_suffixes: Sequence[str],
    ) -> dict[str, dict[str, dict[str, float | int]]]:
        return compute_categorical_distributions(
            df,
            column_suffixes=column_suffixes,
            top_n=cls.TOP_N,
            ratio_precision=cls.RATIO_PRECISION,
            other_bucket_label=cls.OTHER_BUCKET,
        )


__all__ = ["QCUnits"]

