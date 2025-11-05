"""Target pipeline implementation for ChEMBL."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

from bioetl.clients.chembl import ChemblClient
from bioetl.config import PipelineConfig
from bioetl.config.models import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.schemas.chembl.target import COLUMN_ORDER

from ..base import PipelineBase
from .target_transform import serialize_target_arrays


class ChemblTargetPipeline(PipelineBase):
    """ETL pipeline extracting target records from the ChEMBL API."""

    actor = "target_chembl"

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
        """Fetch target payloads from ChEMBL using the configured HTTP client.

        Checks for input_file in config.cli.input_file and calls extract_by_ids()
        if present, otherwise calls extract_all().
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))

        # Check for input file and extract IDs if present
        if self.config.cli.input_file:
            id_column_name = self._get_id_column_name()
            ids = self._read_input_ids(
                id_column_name=id_column_name,
                limit=self.config.cli.limit,
                sample=self.config.cli.sample,
            )
            if ids:
                log.info("chembl_target.extract_mode", mode="batch", ids_count=len(ids))
                return self.extract_by_ids(ids)

        log.info("chembl_target.extract_mode", mode="full")
        return self.extract_all()

    def extract_all(self) -> pd.DataFrame:
        """Extract all target records from ChEMBL using pagination."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_target_http", http_client)

        chembl_client = ChemblClient(http_client)
        self._chembl_release = self._fetch_chembl_release(chembl_client, log)

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(
                "chembl_target.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=self._chembl_release,
            )
            return pd.DataFrame()

        batch_size = self._resolve_batch_size(source_config)
        limit = self.config.cli.limit
        page_size = min(batch_size, 25)
        if limit is not None:
            page_size = min(page_size, limit)
        page_size = max(page_size, 1)

        select_fields = self._resolve_select_fields(source_config)
        records: list[Mapping[str, Any]] = []

        params: Mapping[str, Any] | None = None
        if select_fields:
            params = {
                "limit": page_size,
                "only": ",".join(select_fields),
            }

        for item in chembl_client.paginate(
            "/target.json",
            params=params,
            page_size=page_size,
            items_key="targets",
        ):
            records.append(item)
            if limit is not None and len(records) >= limit:
                break

        dataframe = pd.DataFrame.from_records(records)  # pyright: ignore[reportUnknownMemberType]
        if not dataframe.empty and "target_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("target_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_target.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            limit=limit,
        )
        return dataframe

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract target records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of target_chembl_id values to extract.

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted target records.
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_target_http", http_client)

        chembl_client = ChemblClient(http_client)
        self._chembl_release = self._fetch_chembl_release(chembl_client, log)

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(
                "chembl_target.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=self._chembl_release,
            )
            return pd.DataFrame()

        batch_size = self._resolve_batch_size(source_config)
        limit = self.config.cli.limit
        select_fields = self._resolve_select_fields(source_config)

        # Process IDs in chunks to avoid URL length limits
        chunk_size = min(batch_size, 100)  # Conservative limit for target_chembl_id__in
        all_records: list[Mapping[str, Any]] = []
        ids_list = list(ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "target_chembl_id__in": ",".join(chunk),
                "limit": min(batch_size, 25),
            }
            if select_fields:
                params["only"] = ",".join(select_fields)

            try:
                for item in chembl_client.paginate(
                    "/target.json",
                    params=params,
                    page_size=min(batch_size, 25),
                    items_key="targets",
                ):
                    all_records.append(item)
                    if limit is not None and len(all_records) >= limit:
                        break
            except Exception as exc:
                log.warning(
                    "chembl_target.fetch_error",
                    target_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

            if limit is not None and len(all_records) >= limit:
                break

        dataframe = pd.DataFrame.from_records(all_records)  # pyright: ignore[reportUnknownMemberType]
        if not dataframe.empty and "target_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("target_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_target.extract_by_ids_summary",
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            limit=limit,
        )
        return dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw target data by normalizing fields and enriching with component/classification data."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("transform"))

        df = df.copy()

        df = self._harmonize_identifier_columns(df, log)
        df = self._ensure_schema_columns(df, log)

        if df.empty:
            log.debug("transform_empty_dataframe")
            return df

        log.info("transform_started", rows=len(df))

        # Normalize identifiers
        df = self._normalize_identifiers(df, log)

        # Serialize array fields (cross_references, target_components, target_component_synonyms)
        df = serialize_target_arrays(df, self.config)

        # Enrich with target_component data
        if not self.config.cli.dry_run:
            df = self._enrich_target_components(df, log)

        # Enrich with protein_classification data
        if not self.config.cli.dry_run:
            df = self._enrich_protein_classifications(df, log)

        # Normalize string fields
        df = self._normalize_string_fields(df, log)

        # Ensure all schema columns exist after enrichment
        df = self._ensure_schema_columns(df, log)

        # Normalize data types after ensuring columns (to fix types created as object)
        df = self._normalize_data_types(df, log)

        df = self._order_schema_columns(df)

        log.info("transform_completed", rows=len(df))
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str) -> SourceConfig:
        try:
            return self.config.sources[name]
        except KeyError as exc:
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(parameters: Mapping[str, Any]) -> str:
        base_url = parameters.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url.rstrip("/")

    @staticmethod
    def _resolve_batch_size(source_config: SourceConfig) -> int:
        batch_size: int | None = getattr(source_config, "batch_size", None)
        if batch_size is None:
            parameters = getattr(source_config, "parameters", {})
            if isinstance(parameters, Mapping):
                candidate: Any = parameters.get("batch_size")  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
                if isinstance(candidate, int) and candidate > 0:
                    batch_size = candidate
        if batch_size is None or batch_size <= 0:
            batch_size = 25
        return batch_size

    def _resolve_select_fields(self, source_config: SourceConfig) -> list[str] | None:
        """Resolve select_fields from config or return None."""
        parameters_raw = getattr(source_config, "parameters", {})
        if isinstance(parameters_raw, Mapping):
            parameters = cast(Mapping[str, Any], parameters_raw)
            select_fields_raw = parameters.get("select_fields")
            if (
                select_fields_raw is not None
                and isinstance(select_fields_raw, Sequence)
                and not isinstance(select_fields_raw, (str, bytes))
            ):
                select_fields = cast(Sequence[Any], select_fields_raw)
                return [str(field) for field in select_fields]
        return None

    def _fetch_chembl_release(self, client: ChemblClient, log: Any) -> str | None:
        """Fetch ChEMBL release version from /status endpoint."""
        try:
            status = client.handshake("/status")
            release_value = status.get("chembl_db_version")
            if isinstance(release_value, str):
                log.info("chembl_target.status", chembl_release=release_value)
                return release_value
        except Exception as exc:
            log.warning("chembl_target.status_failed", error=str(exc))
        return None

    def _harmonize_identifier_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Harmonize identifier column names."""
        df = df.copy()
        actions: list[str] = []

        if "target_id" in df.columns and "target_chembl_id" not in df.columns:
            df["target_chembl_id"] = df["target_id"]
            actions.append("target_id->target_chembl_id")

        alias_columns = [column for column in ("target_id",) if column in df.columns]
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

        if "target_chembl_id" in df.columns:
            mask = df["target_chembl_id"].notna()
            if mask.any():
                series = df.loc[mask, "target_chembl_id"].astype(str).str.strip()
                invalid_mask = mask & ~series.str.match(chembl_id_pattern, na=False)
                if invalid_mask.any():
                    log.warning(
                        "invalid_target_chembl_id",
                        count=int(invalid_mask.sum()),
                    )
                    df.loc[invalid_mask, "target_chembl_id"] = pd.NA

        return df

    def _enrich_target_components(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Enrich targets with component data from /target_component endpoint."""
        df = df.copy()

        if df.empty or "target_chembl_id" not in df.columns:
            return df

        # Reuse existing client if available, otherwise create new one
        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        chembl_client = ChemblClient(http_client)

        target_ids = df["target_chembl_id"].dropna().unique().tolist()
        component_map: dict[str, list[str]] = {}

        log.info("enrich_target_components_start", target_count=len(target_ids))

        for target_id in target_ids:
            try:
                components: list[str] = []
                for item in chembl_client.paginate(
                    "/target_component.json",
                    params={"target_chembl_id": target_id},
                    page_size=25,
                    items_key="target_components",
                ):
                    accession = item.get("accession")
                    if isinstance(accession, str) and accession.strip():
                        components.append(accession.strip())

                if components:
                    component_map[target_id] = components
            except Exception as exc:
                log.warning(
                    "target_component_fetch_error",
                    target_chembl_id=target_id,
                    error=str(exc),
                )

        # Add uniprot_accessions column as JSON array
        if "uniprot_accessions" in df.columns:
            df["uniprot_accessions"] = df["target_chembl_id"].map(
                lambda x: json.dumps(component_map.get(str(x), [])) if pd.notna(x) else pd.NA
            )

        # Add component_count column
        if "component_count" in df.columns:
            df["component_count"] = df["target_chembl_id"].map(
                lambda x: len(component_map.get(str(x), [])) if pd.notna(x) else pd.NA
            )

        log.info("enrich_target_components_complete", enriched_count=len(component_map))
        return df

    def _enrich_protein_classifications(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Enrich targets with protein classification data."""
        df = df.copy()

        if df.empty or "target_chembl_id" not in df.columns:
            return df

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        chembl_client = ChemblClient(http_client)

        target_ids = df["target_chembl_id"].dropna().unique().tolist()
        classification_map: dict[str, str] = {}

        log.info("enrich_protein_classifications_start", target_count=len(target_ids))

        for target_id in target_ids:
            try:
                classifications: list[str] = []
                for item in chembl_client.paginate(
                    "/protein_classification.json",
                    params={"target_chembl_id": target_id},
                    page_size=25,
                    items_key="protein_classifications",
                ):
                    level = item.get("protein_classification_level")
                    class_id = item.get("protein_classification_id")
                    if isinstance(level, str) and isinstance(class_id, str):
                        classifications.append(f"{level}:{class_id}")

                if classifications:
                    classification_map[target_id] = "; ".join(classifications)
            except Exception as exc:
                log.warning(
                    "protein_classification_fetch_error",
                    target_chembl_id=target_id,
                    error=str(exc),
                )

        # Add protein_class_desc column
        if "protein_class_desc" in df.columns:
            df["protein_class_desc"] = df["target_chembl_id"].map(
                lambda x: classification_map.get(str(x), pd.NA) if pd.notna(x) else pd.NA
            )

        log.info("enrich_protein_classifications_complete", enriched_count=len(classification_map))
        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields by trimming whitespace."""
        df = df.copy()

        string_columns = [
            "pref_name",
            "target_type",
            "organism",
            "tax_id",
        ]

        for col in string_columns:
            if col in df.columns:
                mask = df[col].notna()
                if mask.any():
                    # Convert to string first to handle any numeric types
                    df[col] = df[col].astype(str)
                    df.loc[mask, col] = df.loc[mask, col].str.strip()

        return df

    def _normalize_data_types(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize data types to match schema expectations."""
        df = df.copy()

        # Ensure component_count is Int64 (nullable integer)
        if "component_count" in df.columns:
            df["component_count"] = pd.to_numeric(df["component_count"], errors="coerce").astype("Int64")  # pyright: ignore[reportUnknownMemberType]

        # Normalize species_group_flag: convert bool/str/int to Int64 (0 or 1)
        if "species_group_flag" in df.columns:
            try:
                # Handle bool values explicitly first
                if df["species_group_flag"].dtype == "bool":
                    df["species_group_flag"] = df["species_group_flag"].astype(int).astype("Int64")
                else:
                    numeric_series_flag: pd.Series[Any] = pd.to_numeric(df["species_group_flag"], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                    df["species_group_flag"] = numeric_series_flag.astype("Int64")
            except (ValueError, TypeError) as exc:
                log.warning("species_group_flag_conversion_failed", error=str(exc))

        return df

    def _order_schema_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns to match schema order."""
        if df.empty:
            return df

        expected_order = list(COLUMN_ORDER)
        existing_columns = [col for col in expected_order if col in df.columns]
        extra_columns = [col for col in df.columns if col not in expected_order]

        ordered_df = df[existing_columns + extra_columns]
        return ordered_df

