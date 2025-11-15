"""Deterministic distributions for QC reports."""

from __future__ import annotations

from collections.abc import Callable, Sequence

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

        return cls._from_dataframe(
            df,
            match=lambda column: any(
                column.endswith(suffix) for suffix in cls.UNITS_SUFFIXES
            ),
        )

    @classmethod
    def for_relation(cls, df: pd.DataFrame) -> CategoricalDistribution:
        """Return value distributions for all columns ending with ``*_relation``."""

        return cls._from_dataframe(
            df,
            match=lambda column: any(
                column.endswith(suffix) for suffix in cls.RELATION_SUFFIXES
            ),
        )

    @classmethod
    def for_suffixes(
        cls, df: pd.DataFrame, column_suffixes: Sequence[str]
    ) -> CategoricalDistribution:
        """Return value distributions for the provided ``column_suffixes``."""

        suffixes: tuple[str, ...] = tuple(column_suffixes)
        return cls._from_dataframe(
            df,
            match=lambda column: any(column.endswith(suffix) for suffix in suffixes),
        )

    @classmethod
    def _from_dataframe(
        cls,
        df: pd.DataFrame,
        *,
        match: Callable[[str], bool],
    ) -> CategoricalDistribution:
        matched = [column for column in df.columns if match(column)]
        if not matched:
            return {}
        # Use the matched column names as suffixes to benefit from the deterministic
        # ordering and aggregation implemented in ``compute_categorical_distributions``.
        suffixes = tuple(dict.fromkeys(matched))
        return compute_categorical_distributions(
            df,
            column_suffixes=suffixes,
            top_n=cls.TOP_N,
            ratio_precision=cls.RATIO_PRECISION,
            other_bucket_label=cls.OTHER_BUCKET,
        )


__all__ = ["QCUnits"]

