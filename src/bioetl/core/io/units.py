"""Deterministic distributions for QC reports."""

from __future__ import annotations

from typing import ClassVar, Sequence

import pandas as pd

from bioetl.qc.metrics import (
    CategoricalDistribution,
    compute_categorical_distributions,
)


class QCUnits:
    """Utilities for computing ``*_units`` and ``*_relation`` distributions."""

    UNITS_SUFFIXES: ClassVar[tuple[str, ...]] = ("units",)
    RELATION_SUFFIXES: ClassVar[tuple[str, ...]] = ("relation",)
    TOP_N: ClassVar[int] = 20
    RATIO_PRECISION: ClassVar[int] = 6
    OTHER_BUCKET: ClassVar[str] = "__other__"

    @classmethod
    def for_units(cls, df: pd.DataFrame) -> CategoricalDistribution:
        """Return value distributions for all columns ending with ``*_units``."""

        return cls._for_suffixes(df, column_suffixes=cls.UNITS_SUFFIXES)

    @classmethod
    def for_relation(cls, df: pd.DataFrame) -> CategoricalDistribution:
        """Return value distributions for all columns ending with ``*_relation``."""

        return cls._for_suffixes(df, column_suffixes=cls.RELATION_SUFFIXES)

    @classmethod
    def for_suffixes(
        cls, df: pd.DataFrame, column_suffixes: Sequence[str]
    ) -> CategoricalDistribution:
        """Return value distributions for the provided ``column_suffixes``."""

        return cls._for_suffixes(df, column_suffixes=column_suffixes)

    @classmethod
    def _for_suffixes(
        cls,
        df: pd.DataFrame,
        *,
        column_suffixes: Sequence[str],
    ) -> CategoricalDistribution:
        return compute_categorical_distributions(
            df,
            column_suffixes=column_suffixes,
            top_n=cls.TOP_N,
            ratio_precision=cls.RATIO_PRECISION,
            other_bucket_label=cls.OTHER_BUCKET,
        )


__all__ = ["QCUnits"]

