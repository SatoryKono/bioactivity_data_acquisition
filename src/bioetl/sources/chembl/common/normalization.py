from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

import pandas as pd
import pandera as pa

from bioetl.core.output import hash_business_key, hash_row


@dataclass(frozen=True, slots=True)
class ColumnNormalizationSpec:
    """Specification for normalizing a single column."""

    name: str
    dtype: str | type
    default: Any = pd.NA
    transformer: Callable[[pd.Series], pd.Series] | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ColumnNormalizationSpec.name must be provided")


class BaseChemblNormalizer:
    """Shared normalization routine for ChEMBL dataframes."""

    def __init__(
        self,
        *,
        business_key_column: str,
        schema: pa.DataFrameSchema,
        columns: Sequence[str],
        column_specs: Iterable[ColumnNormalizationSpec],
    ) -> None:
        if not business_key_column:
            raise ValueError("business_key_column is required")
        self.business_key_column = business_key_column
        self.schema = schema
        self.columns = tuple(columns)
        self.column_specs = tuple(column_specs)
        if not self.column_specs:
            raise ValueError("At least one column_spec is required")

    @staticmethod
    def _resolve_dtype(dtype: str | type) -> Any:
        if dtype == "string":
            return pd.StringDtype()
        return dtype

    def normalize(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        df = df_raw.copy()
        for spec in self.column_specs:
            dtype = self._resolve_dtype(spec.dtype)
            if spec.name not in df:
                df[spec.name] = pd.Series(spec.default, index=df.index, dtype=dtype)
            series = df[spec.name]
            if spec.transformer:
                series = spec.transformer(series)
            series = series.astype(dtype)
            df[spec.name] = series

        df["business_key"] = df[self.business_key_column].astype(str)
        df["business_key_hash"] = df["business_key"].apply(hash_business_key)
        df["row_hash"] = df.apply(
            lambda row: hash_row([row[col] for col in self.columns if col != "row_hash"]),
            axis=1,
        )

        validated = self.schema.validate(df[list(self.columns)])
        return validated


__all__ = ["BaseChemblNormalizer", "ColumnNormalizationSpec"]
