"""Упрощённый Target-пайплайн."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.schema.normalizers import IdentifierRule, StringRule
from bioetl.pipelines.chembl.common import BaseChemblPipeline
from bioetl.pipelines.chembl.target.transform import serialize_target_arrays
from bioetl.schemas.chembl_target_schema import COLUMN_ORDER, TargetSchema


class ChemblTargetPipeline(BaseChemblPipeline):
    entity_name = "target"
    id_column = "target_chembl_id"
    actor = "target_chembl"

    def __init__(
        self,
        config: Any,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        writer: Any = None,
    ) -> None:
        super().__init__(config, run_id, source, writer=writer)

    def get_normalization_rules(self) -> Mapping[str, Any]:
        # Return empty field_mappings to preserve all fields from extract
        return {"field_mappings": {}}

    def get_schema(self) -> Mapping[str, Any]:
        return {"target_chembl_id": lambda series: series.notna()}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw DataFrame by applying normalization and enrichment stages."""
        if df.empty:
            return df

        log = UnifiedLogger.get(__name__).bind(
            component="target_chembl.transform"
        )

        # Start with a copy to preserve all fields
        working_df = df.copy()

        # Harmonize identifier column aliases
        working_df = self.preprocess_identifier_columns(working_df, log)

        # Normalize identifiers
        working_df = self._normalize_identifiers(working_df, log)

        # Normalize string fields
        working_df = self._normalize_string_fields(working_df, log)

        # Serialize array fields (cross_references, target_components, etc.)
        working_df = serialize_target_arrays(working_df, self.config)

        # Ensure all schema columns are present prior to type normalization
        working_df = self._ensure_schema_columns(working_df, COLUMN_ORDER, log)

        # Normalize data types according to schema
        working_df = self._normalize_data_types(working_df, TargetSchema, log)

        # Final column harmonization
        return self._order_schema_columns(working_df, COLUMN_ORDER)

    def save_results(
        self,
        df: pd.DataFrame,
        output_dir: Any,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> Any:
        # Convert None to False for IOArtifactsMixin compatibility
        include_correlation_val = (
            include_correlation if include_correlation is not None else False
        )
        include_qc_metrics_val = (
            include_qc_metrics if include_qc_metrics is not None else False
        )
        return super().save_results(
            df,
            output_dir,
            extended=extended,
            include_correlation=include_correlation_val,
            include_qc_metrics=include_qc_metrics_val,
        )

    def preprocess_identifier_columns(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:
        """Harmonize identifier column names (e.g., target_id -> target_chembl_id)."""
        working_df = df.copy()
        # Rename target_id -> target_chembl_id
        if "target_id" in working_df.columns:
            if "target_chembl_id" in working_df.columns:
                # Both exist: combine values, preferring target_chembl_id
                working_df["target_chembl_id"] = working_df[
                    "target_chembl_id"
                ].combine_first(working_df["target_id"])
                # Drop old column
                working_df = working_df.drop(columns=["target_id"])
            else:
                # Only target_id exists: rename it
                working_df = working_df.rename(
                    columns={"target_id": "target_chembl_id"}
                )
        return working_df

    def _harmonize_identifier_columns(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:
        """Alias for preprocess_identifier_columns for backward compatibility."""
        return self.preprocess_identifier_columns(df, log)

    def identifier_rules(self) -> Sequence[IdentifierRule]:
        """Return identifier normalization rules for ChEMBL identifiers."""
        return (
            IdentifierRule(
                columns=("target_chembl_id",),
                pattern=r"^CHEMBL\d+$",
                uppercase=True,
                strip=True,
                empty_to_null=True,
                name="chembl_id",
            ),
        )

    def string_rules(self) -> Mapping[str, StringRule]:
        """Return string normalization rules for target pipeline fields."""
        return {
            "pref_name": StringRule(trim=True, empty_to_null=True),
            "organism": StringRule(
                trim=True, empty_to_null=True, title_case=True
            ),
            "target_type": StringRule(
                trim=True, empty_to_null=True, uppercase=True
            ),
        }

    def _normalize_data_types(
        self, df: pd.DataFrame, schema: Any, log: BoundLogger
    ) -> pd.DataFrame:
        """Convert data types according to the schema for target pipeline."""
        import pandera as pa

        result = df.copy()

        if schema is None:
            return result

        def _to_numeric_series(series: pd.Series[Any]) -> pd.Series[Any]:
            return pd.to_numeric(series, errors="coerce")

        # Get column definitions from schema
        for column_name, column_def in schema.columns.items():
            if column_name not in result.columns:
                continue

            try:
                column_series = result[column_name]

                # Handle Int64 (nullable and non-nullable)
                dtype_name = (
                    getattr(column_def.dtype, "name", None)
                    if hasattr(column_def.dtype, "name")
                    else None
                )
                dtype_str = str(column_def.dtype).lower()
                dtype_type_name = type(column_def.dtype).__name__
                if (
                    column_def.dtype == pd.Int64Dtype()
                    or isinstance(column_def.dtype, type(pa.Int64))
                    or dtype_name == "Int64"
                    or dtype_str == "int64"
                    or dtype_type_name == "Int64"
                ):
                    # Convert to numeric first, then to Int64
                    numeric_series = pd.to_numeric(
                        column_series, errors="coerce"
                    )
                    result[column_name] = numeric_series.astype("Int64")
                # Handle Float64/float64
                elif (
                    isinstance(column_def.dtype, type(pa.Float64))
                    or dtype_name in ("Float64", "float64")
                    or dtype_str == "float64"
                    or dtype_type_name == "Float64"
                ):
                    numeric_series = _to_numeric_series(column_series)
                    result[column_name] = numeric_series.astype("float64")
                # Handle boolean flags
                elif (
                    column_def.dtype == pd.BooleanDtype()
                    or isinstance(column_def.dtype, type(pa.Bool))
                    or dtype_str == "boolean"
                    or dtype_type_name == "BOOL"
                ):
                    # Convert to boolean, handling int (0/1) and string ("0"/"1") inputs
                    mask = column_series.notna()
                    if mask.any():
                        # Convert numeric/string to boolean: 0/False -> False, 1/True -> True
                        numeric_values = pd.to_numeric(
                            column_series, errors="coerce"
                        )
                        result[column_name] = numeric_values.astype("boolean")
                    else:
                        result[column_name] = column_series.astype("boolean")
                # Handle strings
                elif (
                    (
                        hasattr(column_def.dtype, "name")
                        and column_def.dtype.name == "string"
                    )
                    or column_def.dtype == pa.String
                    or "string" in dtype_str
                    or "str" in dtype_str
                ):
                    mask = column_series.notna()
                    if mask.any():
                        result.loc[mask, column_name] = result.loc[
                            mask, column_name
                        ].astype(str)
            except (ValueError, TypeError) as exc:
                log.warning(
                    LogEvents.TYPE_CONVERSION_FAILED,
                    field=column_name,
                    error=str(exc),
                )

        return result


__all__ = ["ChemblTargetPipeline"]
