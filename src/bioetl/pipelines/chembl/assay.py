"""Assay pipeline implementation for ChEMBL."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from bioetl.clients.chembl import ChemblClient
from bioetl.config import AssaySourceConfig, PipelineConfig
from bioetl.config.models import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.schemas.assay import COLUMN_ORDER

from ...sources.chembl.assay import ChemblAssayClient
from ..base import PipelineBase


class ChemblAssayPipeline(PipelineBase):
    """ETL pipeline extracting assay records from the ChEMBL API."""

    actor = "assay_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)
        self._chembl_release: str | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release captured during extraction."""

        return self._chembl_release

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch assay payloads from ChEMBL using the configured HTTP client."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = AssaySourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(source_config)

        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_assay_http", http_client)

        chembl_client = ChemblClient(http_client)
        assay_client = ChemblAssayClient(
            chembl_client,
            batch_size=source_config.batch_size,
            max_url_length=source_config.max_url_length,
        )

        assay_client.handshake(
            endpoint=source_config.parameters.handshake_endpoint,
            enabled=source_config.parameters.handshake_enabled,
        )
        self._chembl_release = assay_client.chembl_release

        log.info(
            "chembl_assay.handshake",
            chembl_release=self._chembl_release,
            handshake_endpoint=source_config.parameters.handshake_endpoint,
            handshake_enabled=source_config.parameters.handshake_enabled,
        )

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(
                "chembl_assay.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=self._chembl_release,
            )
            return pd.DataFrame()

        records: list[Mapping[str, Any]] = []
        limit = self.config.cli.limit
        page_size = source_config.batch_size

        for item in assay_client.iterate_all(limit=limit, page_size=page_size):
            records.append(item)

        dataframe = pd.DataFrame.from_records(records)
        if not dataframe.empty and "assay_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("assay_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_assay.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            handshake_endpoint=source_config.parameters.handshake_endpoint,
            limit=limit,
        )
        return dataframe

    def transform(self, payload: object) -> pd.DataFrame:
        """Transform raw assay data by normalizing identifiers, types, and nested structures."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("transform"))

        if not isinstance(payload, pd.DataFrame):
            if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
                df = pd.DataFrame(payload)
            elif isinstance(payload, Mapping):
                df = pd.DataFrame([payload])
            else:
                log.warning("transform_invalid_payload", payload_type=type(payload).__name__)
                return pd.DataFrame()
        else:
            df = payload.copy()

        df = self._harmonize_identifier_columns(df, log)
        df = self._ensure_schema_columns(df, log)

        if df.empty:
            log.debug("transform_empty_dataframe")
            return df

        log.info("transform_started", rows=len(df))

        df = self._normalize_identifiers(df, log)
        df = self._normalize_string_fields(df, log)
        df = self._normalize_nested_structures(df, log)
        df = self._add_row_metadata(df, log)
        df = self._normalize_data_types(df, log)
        df = self._ensure_schema_columns(df, log)
        df = self._order_schema_columns(df)

        log.info("transform_completed", rows=len(df))
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str) -> SourceConfig:
        try:
            return self.config.sources[name]
        except KeyError as exc:  # pragma: no cover - configuration error path
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(source_config: AssaySourceConfig) -> str:
        base_url = source_config.parameters.base_url or "https://www.ebi.ac.uk/chembl/api/data"
        if not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url.rstrip("/")

    def _harmonize_identifier_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Harmonize identifier column names (e.g., assay_id -> assay_chembl_id)."""

        df = df.copy()
        actions: list[str] = []

        if "assay_id" in df.columns and "assay_chembl_id" not in df.columns:
            df["assay_chembl_id"] = df["assay_id"]
            actions.append("assay_id->assay_chembl_id")

        if "target_id" in df.columns and "target_chembl_id" not in df.columns:
            df["target_chembl_id"] = df["target_id"]
            actions.append("target_id->target_chembl_id")

        alias_columns = [column for column in ("assay_id", "target_id") if column in df.columns]
        if alias_columns:
            df = df.drop(columns=alias_columns)
            actions.append(f"dropped_aliases:{','.join(alias_columns)}")

        if actions:
            log.debug("identifier_harmonization", actions=actions)

        return df

    def _ensure_schema_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Add missing schema columns so downstream normalization can operate safely."""

        df = df.copy()
        if df.empty:
            return df

        expected = list(COLUMN_ORDER)
        missing = [column for column in expected if column not in df.columns]
        if missing:
            for column in missing:
                df[column] = pd.Series([pd.NA] * len(df), dtype="object")
            log.debug("schema_columns_added", columns=missing)

        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize ChEMBL identifiers with regex validation."""

        df = df.copy()
        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")

        chembl_fields = [
            "assay_chembl_id",
            "target_chembl_id",
        ]

        normalized_count = 0
        invalid_count = 0

        for field in chembl_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                df.loc[mask, field] = df.loc[mask, field].astype(str).str.upper().str.strip()
                valid_mask = df[field].str.match(chembl_id_pattern.pattern, na=False)
                invalid_mask = mask & ~valid_mask
                if invalid_mask.any():
                    invalid_count += int(invalid_mask.sum())
                    df.loc[invalid_mask, field] = None
                normalized_count += int((mask & valid_mask).astype(int).sum())

        if normalized_count > 0 or invalid_count > 0:
            log.debug(
                "identifiers_normalized",
                normalized_count=normalized_count,
                invalid_count=invalid_count,
            )

        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields (assay_type, assay_category, assay_organism, curation_level)."""

        df = df.copy()

        string_fields = [
            "assay_type",
            "assay_category",
            "assay_organism",
            "curation_level",
        ]

        for field in string_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                df.loc[mask, field] = df.loc[mask, field].astype(str).str.strip()
                # Replace empty strings with None
                empty_mask = mask & (df[field] == "")
                if empty_mask.any():
                    df.loc[empty_mask, field] = None

        return df

    def _normalize_nested_structures(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Process nested structures (assay_parameters, assay_classifications) and extract BAO format."""

        df = df.copy()

        # Extract assay_class_id from bao_format if available
        if "bao_format" in df.columns and "assay_class_id" in df.columns:
            mask = df["bao_format"].notna() & df["assay_class_id"].isna()
            if mask.any():
                df.loc[mask, "assay_class_id"] = df.loc[mask, "bao_format"]
                log.debug("assay_class_id_extracted_from_bao_format", count=int(mask.sum()))

        # Extract assay_class_id from assay_classifications if available
        if "assay_classifications" in df.columns and "assay_class_id" in df.columns:
            mask = df["assay_classifications"].notna() & df["assay_class_id"].isna()
            if mask.any():
                for idx in df[mask].index:
                    classifications = df.loc[idx, "assay_classifications"]
                    if isinstance(classifications, (list, Sequence)) and len(classifications) > 0:
                        # Extract first classification if it's a dict with bao_format
                        first_class = classifications[0]
                        if isinstance(first_class, Mapping):
                            bao_format: Any = first_class.get("bao_format")
                            if bao_format and isinstance(bao_format, str):
                                df.loc[idx, "assay_class_id"] = bao_format
                log.debug("assay_class_id_extracted_from_classifications", count=int(mask.sum()))

        # Drop nested structures that are not needed in the final schema
        nested_columns = ["assay_parameters", "assay_classifications"]
        columns_to_drop = [col for col in nested_columns if col in df.columns]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            log.debug("nested_columns_dropped", columns=columns_to_drop)

        return df

    def _add_row_metadata(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Add required row metadata fields (row_subtype, row_index)."""

        df = df.copy()
        if df.empty:
            return df

        # Add row_subtype: "assay" for all rows
        if "row_subtype" not in df.columns:
            df["row_subtype"] = "assay"
            log.debug("row_subtype_added", value="assay")
        elif df["row_subtype"].isna().all():
            df["row_subtype"] = "assay"
            log.debug("row_subtype_filled", value="assay")

        # Add row_index: sequential index starting from 0
        if "row_index" not in df.columns:
            df["row_index"] = range(len(df))
            log.debug("row_index_added", count=len(df))
        elif df["row_index"].isna().all():
            df["row_index"] = range(len(df))
            log.debug("row_index_filled", count=len(df))

        return df

    def _normalize_data_types(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Convert data types according to the AssaySchema."""

        df = df.copy()

        # Nullable integer fields
        nullable_int_fields = {
            "row_index": "Int64",
            "confidence_score": "Int64",
        }

        # Handle nullable integers - preserve NA values
        for field, _ in nullable_int_fields.items():
            if field not in df.columns:
                continue
            try:
                if field == "row_index":
                    # row_index should be non-nullable, but use Int64 for consistency
                    numeric_series_row: pd.Series[Any] = pd.to_numeric(df[field], errors="coerce")
                    df[field] = numeric_series_row.astype("Int64")
                    # Fill any NA values with sequential index
                    if df[field].isna().any():
                        df[field] = range(len(df))
                elif field == "confidence_score":
                    # confidence_score is nullable
                    numeric_series_conf: pd.Series[Any] = pd.to_numeric(df[field], errors="coerce")
                    df[field] = numeric_series_conf.astype("Int64")
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=field, error=str(exc))

        # Ensure string fields are object type (nullable strings)
        string_fields = [
            "assay_chembl_id",
            "row_subtype",
            "assay_type",
            "assay_category",
            "assay_class_id",
            "assay_organism",
            "target_chembl_id",
            "curation_level",
        ]

        for field in string_fields:
            if field not in df.columns:
                continue
            # Convert to string, preserving None values
            mask = df[field].notna()
            if mask.any():
                df.loc[mask, field] = df.loc[mask, field].astype(str)

        return df

    def _order_schema_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return DataFrame with schema columns ordered ahead of extras."""

        extras = [column for column in df.columns if column not in COLUMN_ORDER]
        if self.config.validation.strict:
            # Only return schema columns
            return df[list(COLUMN_ORDER)]
        # Return schema columns first, then extras
        return df[[*COLUMN_ORDER, *extras]]
