"""Module for ChEMBL data normalization and standardization."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Callable, Sequence

import pandas as pd


@dataclass(frozen=True)
class ColumnMapping:
    """Mapping from source fields to a target canonical column."""
    column: str
    sources: tuple[str, ...]

    def __init__(self, column: str, sources: tuple[str, ...]):
        object.__setattr__(self, "column", column)
        object.__setattr__(self, "sources", sources)


def build_records_from_payload(
    payload: dict[str, Any], mappings: Sequence[ColumnMapping]
) -> list[dict[str, Any]]:
    """Extract canonical records from a raw API payload."""
    results = payload.get("results", [])
    if not isinstance(results, list):
        return []

    records = []
    for item in results:
        record = {}
        for mapping in mappings:
            value = None
            for source in mapping.sources:
                if source in item:
                    value = item[source]
                    break
            # Only include field if value was found, omit to allow
            # normalizer defaults
            if value is not None:
                record[mapping.column] = value
        records.append(record)
    return records


@dataclass(frozen=True)
class ColumnNormalizationSpec:
    """Specification for normalizing a single dataframe column."""
    name: str
    dtype: str = "string"
    default: Any = None
    transformer: Callable[[pd.Series], pd.Series] | None = None


class BaseChemblNormalizer:
    """Base normalizer for ChEMBL dataframes."""

    def __init__(
        self,
        business_key_column: str,
        schema: Any,  # Pandera schema class or object
        columns: Any,  # Enum or list of columns
        column_specs: Sequence[ColumnNormalizationSpec],
    ):
        self.business_key_column = business_key_column
        self.schema = schema
        self.columns = columns
        self.column_specs = column_specs

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply normalization rules to the dataframe."""
        df = df.copy()

        # 1. Apply column specs (defaults, types, transformers)
        for spec in self.column_specs:
            if spec.name not in df.columns:
                df[spec.name] = spec.default

            if spec.transformer:
                df[spec.name] = spec.transformer(df[spec.name])

            # Simple dtype casting fallback
            if spec.dtype == "string":
                df[spec.name] = df[spec.name].astype("string")
            elif spec.dtype == "float":
                df[spec.name] = pd.to_numeric(df[spec.name], errors="coerce")
            elif spec.dtype == "Int64":
                df[spec.name] = df[spec.name].astype("Int64")
            # Add more types as needed

        # 2. Business Key
        if self.business_key_column in df.columns:
            df["business_key"] = df[self.business_key_column].astype(str)
        else:
            df["business_key"] = pd.NA

        # 2.1 Business Key Hash
        def _hash_bk(val: Any) -> str | Any:
            if pd.isna(val):
                return pd.NA
            return hashlib.blake2b(
                str(val).encode(), digest_size=16
            ).hexdigest()

        if not df.empty:
            df["business_key_hash"] = df["business_key"].apply(_hash_bk)
        else:
            df["business_key_hash"] = pd.Series(dtype="string")

        # 3. Row Hash (Simple implementation for now)
        def _hash_row(row: pd.Series) -> str:
            # Canonical JSON serialization of row dict
            row_dict = row.to_dict()
            # Simple stringify for stability
            serialized = json.dumps(
                row_dict, sort_keys=True, default=str
            )
            return hashlib.blake2b(
                serialized.encode(), digest_size=16
            ).hexdigest()

        # Optimize: vectorize if possible, but apply per row is safer
        if not df.empty:
            df["row_hash"] = df.apply(_hash_row, axis=1)
        else:
            df["row_hash"] = pd.Series(dtype="string")

        return df
