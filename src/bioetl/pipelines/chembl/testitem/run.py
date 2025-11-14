"""TestItem pipeline implementation for ChEMBL."""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, TypeVar, cast

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.clients.entities.client_testitem import ChemblTestitemClient
from bioetl.config import PipelineConfig, TestItemSourceConfig
from bioetl.core import UnifiedLogger
from bioetl.core.http import UnifiedAPIClient
from bioetl.core.logging import LogEvents
from bioetl.core.schema import StringRule, StringStats, normalize_string_columns
from bioetl.schemas.chembl_testitem_schema import COLUMN_ORDER

from ..common.descriptor import (
    BatchExtractionContext,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from .transform import transform as transform_testitem

SelfTestitemChemblPipeline = TypeVar(
    "SelfTestitemChemblPipeline", bound="TestItemChemblPipeline"
)

# Required fields that must always be requested from the API.
MUST_HAVE_FIELDS: tuple[str, ...] = (
    # Scalar fields.
    "molecule_chembl_id",
    "pref_name",
    "molecule_type",
    "availability_type",
    "chirality",
    "first_approval",
    "first_in_class",
    "indication_class",
    "helm_notation",
    # Nested roots to ensure the server returns objects.
    "molecule_properties",
    "molecule_structures",
    "molecule_hierarchy",
)


class TestItemChemblPipeline(ChemblPipelineBase):
    """ETL pipeline extracting molecule records from the ChEMBL API."""

    actor = "testitem_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._chembl_db_version: str | None = None
        self._api_version: str | None = None

    @property
    def chembl_db_version(self) -> str | None:
        """Return the cached ChEMBL DB version captured during extraction."""
        return self._chembl_db_version

    @property
    def api_version(self) -> str | None:
        """Return the cached ChEMBL API version captured during extraction."""
        return self._api_version

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient | "ChemblClient" | Any,  # noqa: ANN401
        log: BoundLogger | None = None,
    ) -> str | None:
        """Capture ChEMBL release and API version from status endpoint."""

        if log is None:
            bound_log: BoundLogger = UnifiedLogger.get(__name__).bind(
                component=f"{self.pipeline_code}.extract"
            )
        else:
            bound_log = log

        request_timestamp = datetime.now(timezone.utc)
        release_value: str | None = None
        api_version: str | None = None

        try:
            status_payload: dict[str, Any] = {}
            handshake_candidate = getattr(client, "handshake", None)
            if callable(handshake_candidate):
                status_payload = self._coerce_mapping(handshake_candidate("/status"))
            else:
                get_candidate = getattr(client, "get", None)
                if callable(get_candidate):
                    response = get_candidate("/status.json")
                    json_candidate = getattr(response, "json", None)
                    if callable(json_candidate):
                        status_payload = self._coerce_mapping(json_candidate())

            if status_payload:
                release_value = self._extract_chembl_release(status_payload)
                api_candidate = status_payload.get("api_version")
                if isinstance(api_candidate, str) and api_candidate.strip():
                    api_version = api_candidate
                bound_log.info(LogEvents.CHEMBL_TESTITEM_STATUS,
                    chembl_db_version=release_value,
                    api_version=api_version,
                )
        except Exception as exc:  # noqa: BLE001
            bound_log.warning(LogEvents.CHEMBL_TESTITEM_STATUS_FAILED, error=str(exc))
        finally:
            self._chembl_db_version = release_value
            self._api_version = api_version
            self.record_extract_metadata(
                chembl_release=release_value,
                requested_at_utc=request_timestamp,
            )

        return release_value

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch molecule payloads from ChEMBL using the unified HTTP client.

        Checks for input_file in config.cli.input_file and calls extract_by_ids()
        if present, otherwise calls extract_all().
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))

        return self._dispatch_extract_mode(
            log,
            event_name="chembl_testitem.extract_mode",
            batch_callback=self.extract_by_ids,
            full_callback=self.extract_all,
            id_column_name="molecule_chembl_id",
        )

    def extract_all(self) -> pd.DataFrame:
        """Extract all molecule records from ChEMBL using pagination."""

        descriptor = self._build_testitem_descriptor()
        return self.run_extract_all(descriptor)

    def _build_testitem_descriptor(
        self: SelfTestitemChemblPipeline,
    ) -> ChemblExtractionDescriptor[SelfTestitemChemblPipeline]:
        """Return the descriptor powering testitem extraction."""

        def build_context(
            pipeline: SelfTestitemChemblPipeline,
            source_config: TestItemSourceConfig,
            log: BoundLogger,
        ) -> ChemblExtractionContext:
            bundle = pipeline.build_chembl_entity_bundle(
                "testitem",
                source_name="chembl",
                source_config=source_config,
            )
            if "chembl_testitem_http" not in pipeline._registered_clients:
                pipeline.register_client("chembl_testitem_http", bundle.api_client)
            chembl_client = bundle.chembl_client
            pipeline._fetch_chembl_release(chembl_client, log)
            select_fields = source_config.parameters.select_fields
            log.debug(LogEvents.CHEMBL_TESTITEM_SELECT_FIELDS, fields=select_fields)
            testitem_client = cast(ChemblTestitemClient, bundle.entity_client)
            if testitem_client is None:
                msg = "Фабрика вернула пустой клиент для 'testitem'"
                raise RuntimeError(msg)
            return ChemblExtractionContext(
                source_config=source_config,
                iterator=testitem_client,
                chembl_client=chembl_client,
                select_fields=list(select_fields) if select_fields else None,
                page_size=source_config.page_size,
                chembl_release=pipeline._chembl_db_version,
                metadata={"api_version": pipeline._api_version},
            )

        def empty_frame(_: SelfTestitemChemblPipeline, __: ChemblExtractionContext) -> pd.DataFrame:
            return pd.DataFrame({"molecule_chembl_id": pd.Series(dtype="string")})

        def dry_run_handler(
            pipeline: SelfTestitemChemblPipeline,
            _: ChemblExtractionContext,
            log: BoundLogger,
            stage_start: float,
        ) -> pd.DataFrame:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(LogEvents.CHEMBL_TESTITEM_EXTRACT_SKIPPED,
                dry_run=True,
                duration_ms=duration_ms,
                chembl_db_version=pipeline._chembl_db_version,
                api_version=pipeline._api_version,
            )
            return pd.DataFrame()

        def summary_extra(
            pipeline: SelfTestitemChemblPipeline,
            _: pd.DataFrame,
            __: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            return {
                "chembl_db_version": pipeline._chembl_db_version,
                "api_version": pipeline._api_version,
                "limit": pipeline.config.cli.limit,
            }

        return ChemblExtractionDescriptor[SelfTestitemChemblPipeline](
            name="chembl_testitem",
            source_name="chembl",
            source_config_factory=TestItemSourceConfig.from_source_config,
            build_context=build_context,
            id_column="molecule_chembl_id",
            summary_event="chembl_testitem.extract_summary",
            must_have_fields=tuple(MUST_HAVE_FIELDS),
            default_select_fields=MUST_HAVE_FIELDS,
            sort_by=("molecule_chembl_id",),
            empty_frame_factory=empty_frame,
            dry_run_handler=dry_run_handler,
            summary_extra=summary_extra,
            hard_page_size_cap=None,
        )

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract molecule records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of molecule_chembl_id values to extract.

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted molecule records.
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = TestItemSourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "testitem",
            source_name="chembl",
            source_config=source_config,
        )
        if "chembl_testitem_http" not in self._registered_clients:
            self.register_client("chembl_testitem_http", bundle.api_client)
        chembl_client = bundle.chembl_client
        self._fetch_chembl_release(chembl_client, log)

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(LogEvents.CHEMBL_TESTITEM_EXTRACT_SKIPPED,
                dry_run=True,
                duration_ms=duration_ms,
                chembl_db_version=self._chembl_db_version,
                api_version=self._api_version,
            )
            return pd.DataFrame()

        page_size = min(source_config.page_size, 25)
        limit = self.config.cli.limit
        resolved_select_fields = source_config.parameters.select_fields
        merged_select_fields = self._merge_select_fields(resolved_select_fields, MUST_HAVE_FIELDS)
        log.debug(LogEvents.CHEMBL_TESTITEM_SELECT_FIELDS, fields=merged_select_fields)

        testitem_client = cast(ChemblTestitemClient, bundle.entity_client)
        if testitem_client is None:
            msg = "Фабрика вернула пустой клиент для 'testitem'"
            raise RuntimeError(msg)

        def fetch_testitems(
            batch_ids: Sequence[str],
            context: BatchExtractionContext,
        ) -> Iterable[Mapping[str, Any]]:
            iterator = testitem_client.iterate_by_ids(
                batch_ids,
                select_fields=context.select_fields or None,
            )
            for item in iterator:
                yield dict(item)

        dataframe, stats = self.run_batched_extraction(
            ids,
            id_column="molecule_chembl_id",
            fetcher=fetch_testitems,
            select_fields=merged_select_fields or None,
            batch_size=page_size,
            chunk_size=min(page_size, 25),
            max_batch_size=25,
            limit=limit,
            metadata_filters={
                "select_fields": list(merged_select_fields) if merged_select_fields else None,
            },
            chembl_release=self._chembl_release,
        )

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(LogEvents.CHEMBL_TESTITEM_EXTRACT_BY_IDS_SUMMARY,
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_db_version=self._chembl_db_version,
            api_version=self._api_version,
            limit=limit,
            batches=stats.batches,
            api_calls=stats.api_calls,
        )
        return dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw molecule data by normalizing fields and extracting nested properties."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("transform"))

        # According to documentation, transform should accept df: pd.DataFrame
        # df is already a pd.DataFrame, so we can use it directly
        df = df.copy()

        if df.empty:
            log.debug(LogEvents.TRANSFORM_EMPTY_DATAFRAME)
            return df

        log.info(LogEvents.STAGE_TRANSFORM_START, rows=len(df))

        # Apply transform module: flatten nested objects and serialize arrays
        df = transform_testitem(df, self.config)

        # Normalize identifiers
        df = self._normalize_identifiers(df, log)

        # Normalize string fields
        df = self._normalize_string_fields(df, log)

        # Ensure all schema columns exist with proper types
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        # Normalize numeric fields (type coercion, negative values -> None)
        df = self._normalize_numeric_fields(df, log)

        # Check for empty columns and warn if too many are empty
        self._check_empty_columns(df, log)

        # Remove columns not in schema
        df = self._remove_extra_columns(df, log)

        # Add version fields
        df["_chembl_db_version"] = self._chembl_db_version or ""
        df["_api_version"] = self._api_version or ""

        # Deduplication
        df = self._deduplicate_molecules(df, log)

        # Sort by molecule_chembl_id for determinism
        if "molecule_chembl_id" in df.columns:
            df = df.sort_values("molecule_chembl_id").reset_index(drop=True)

        # Reorder columns according to schema COLUMN_ORDER
        df = self._order_schema_columns(df, COLUMN_ORDER)

        log.info(LogEvents.STAGE_TRANSFORM_FINISH, rows=len(df))
        return df

    def augment_metadata(
        self,
        metadata: Mapping[str, object],
        df: pd.DataFrame,
    ) -> Mapping[str, object]:
        """Enrich metadata with ChEMBL versions."""

        enriched = dict(super().augment_metadata(metadata, df))
        if self._chembl_db_version:
            enriched["chembl_db_version"] = self._chembl_db_version
        if self._api_version:
            enriched["api_version"] = self._api_version
        return enriched

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flatten_nested_structures(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Flatten nested molecule_structures and molecule_properties into flat columns."""

        if df.empty:
            return df

        # Flatten molecule_structures
        if "molecule_structures" in df.columns:
            structures_df = pd.json_normalize(  # pyright: ignore[reportUnknownMemberType]
                df["molecule_structures"].tolist(),
            )
            if "canonical_smiles" in structures_df.columns:
                df["canonical_smiles"] = structures_df["canonical_smiles"]
            if "standard_inchi_key" in structures_df.columns:
                df["standard_inchi_key"] = structures_df["standard_inchi_key"]

        # Flatten molecule_properties
        if "molecule_properties" in df.columns:
            properties_df = pd.json_normalize(  # pyright: ignore[reportUnknownMemberType]
                df["molecule_properties"].tolist(),
            )
            property_columns = [
                "full_mwt",
                "mw_freebase",
                "alogp",
                "hbd",
                "hba",
                "psa",
                "aromatic_rings",
                "rtb",
                "num_ro5_violations",
            ]
            for col in property_columns:
                if col in properties_df.columns:
                    df[col] = properties_df[col]

        log.debug(LogEvents.FLATTEN_NESTED_STRUCTURES_COMPLETED, columns=list(df.columns))
        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize ChEMBL identifiers and InChI keys."""

        if df.empty:
            return df

        working_df = df.copy()

        inchi_key_col = (
            "molecule_structures__standard_inchi_key"
            if "molecule_structures__standard_inchi_key" in working_df.columns
            else "standard_inchi_key"
        )

        rules: dict[str, StringRule] = {}
        if "molecule_chembl_id" in working_df.columns:
            rules["molecule_chembl_id"] = StringRule()
        if inchi_key_col in working_df.columns:
            rules[inchi_key_col] = StringRule(uppercase=True)

        if rules:
            normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)
        else:
            normalized_df = working_df
            stats = StringStats()

        if stats.has_changes:
            log.debug(LogEvents.NORMALIZE_IDENTIFIERS_COMPLETED,
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )
        else:
            log.debug(LogEvents.NORMALIZE_IDENTIFIERS_COMPLETED)

        return normalized_df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields: trim, replace empty strings with NaN."""

        if df.empty:
            return df

        working_df = df.copy()

        rules = {
            "pref_name": StringRule(),
            "molecule_type": StringRule(),
            "molecule_structures__canonical_smiles": StringRule(),
            "canonical_smiles": StringRule(),
        }

        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)

        if stats.has_changes:
            log.debug(LogEvents.NORMALIZE_STRING_FIELDS_COMPLETED,
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )
        else:
            log.debug(LogEvents.NORMALIZE_STRING_FIELDS_COMPLETED)

        return normalized_df

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        specs = dict(super()._schema_column_specs())

        int_columns = [
            "max_phase",
            "first_approval",
            "first_in_class",
            "availability_type",
            "black_box_warning",
            "chirality",
            "dosed_ingredient",
            "inorganic_flag",
            "natural_product",
            "prodrug",
            "therapeutic_flag",
            "molecule_properties__aromatic_rings",
            "molecule_properties__hba",
            "molecule_properties__hba_lipinski",
            "molecule_properties__hbd",
            "molecule_properties__hbd_lipinski",
            "molecule_properties__heavy_atoms",
            "molecule_properties__num_lipinski_ro5_violations",
            "molecule_properties__num_ro5_violations",
            "molecule_properties__ro3_pass",
            "molecule_properties__rtb",
        ]

        float_columns = [
            "molecule_properties__alogp",
            "molecule_properties__cx_logd",
            "molecule_properties__cx_logp",
            "molecule_properties__cx_most_apka",
            "molecule_properties__cx_most_bpka",
            "molecule_properties__full_mwt",
            "molecule_properties__mw_freebase",
            "molecule_properties__mw_monoisotopic",
            "molecule_properties__psa",
            "molecule_properties__qed_weighted",
        ]

        for column in int_columns:
            specs[column] = {"dtype": "Int64", "default": pd.NA}
        for column in float_columns:
            specs[column] = {"dtype": "Float64", "default": pd.NA}

        for column in COLUMN_ORDER:
            if column not in specs:
                specs[column] = {"dtype": "string", "default": pd.NA}

        return specs

    def _normalize_numeric_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize numeric fields: convert types, replace negative values with None."""

        if df.empty:
            return df

        # Integer columns that should be >= 0
        int_ge_zero_columns = [
            "chirality",
            "availability_type",
            "hbd",
            "hba",
            "aromatic_rings",
            "rtb",
            "num_ro5_violations",
        ]

        # Integer columns that can have specific ranges
        int_columns = [
            "max_phase",
            "first_approval",
            "black_box_warning",
        ]

        # Boolean-like columns (0/1/-1) - convert -1 to None
        boolean_columns = [
            "first_in_class",
            "inorganic_flag",
            "natural_product",
            "prodrug",
            "therapeutic_flag",
            "dosed_ingredient",
        ]

        # Convert all integer columns, handling None/NaN properly
        for col in int_ge_zero_columns + int_columns + boolean_columns:
            if col in df.columns:
                # Convert to numeric, coercing errors and None to NaN
                numeric_series = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[col],
                    errors="coerce",
                )
                # For columns that should be >= 0, replace negative values with NaN
                if col in int_ge_zero_columns:
                    # Replace negative values with NaN
                    numeric_series = numeric_series.where(numeric_series >= 0)
                # For boolean-like columns, replace -1 with NaN (unknown)
                elif col in boolean_columns:
                    numeric_series = numeric_series.where(numeric_series >= 0)
                    # Also ensure values are only 0 or 1
                    numeric_series = numeric_series.where(
                        (numeric_series == 0) | (numeric_series == 1) | numeric_series.isna()
                    )
                # Convert to nullable Int64 type
                # This will automatically convert NaN to pd.NA for nullable integers
                df[col] = numeric_series.astype("Int64")

        # Handle molecule_properties__ prefixed columns
        for col in df.columns:
            if col.startswith("molecule_properties__"):
                prop_name = col.replace("molecule_properties__", "")
                # Special handling for ro3_pass (Y/N to 1/0)
                if prop_name == "ro3_pass":
                    # Convert Y/N to 1/0
                    mask = df[col].notna()
                    if mask.any():
                        # Convert to string and uppercase for comparison
                        col_str = df.loc[mask, col].astype(str).str.upper().str.strip()
                        # Create new series with converted values
                        converted = df[col].copy()
                        # Map Y to 1, N to 0
                        converted.loc[mask & (col_str == "Y")] = 1
                        converted.loc[mask & (col_str == "N")] = 0
                        # For values that are not Y/N, try numeric conversion
                        other_mask = mask & ~col_str.isin(["Y", "N"])  # pyright: ignore[reportUnknownMemberType]
                        if other_mask.any():
                            # Try to convert existing numeric values
                            numeric_vals = pd.to_numeric(df.loc[other_mask, col], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                            # Keep only valid numeric values (0 or 1)
                            valid_numeric = numeric_vals.notna() & numeric_vals.isin([0, 1])  # pyright: ignore[reportUnknownMemberType]
                            # Set all other_mask values to NA first
                            converted.loc[other_mask] = pd.NA
                            # Then restore valid numeric values
                            if valid_numeric.any():
                                valid_indices = numeric_vals.index[valid_numeric]
                                converted.loc[valid_indices] = numeric_vals.loc[valid_indices]
                        df[col] = converted
                    # Convert to Int64, ensuring only 0 or 1
                    numeric_series = pd.to_numeric(df[col], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                    numeric_series = numeric_series.where(
                        (numeric_series == 0) | (numeric_series == 1) | numeric_series.isna()
                    )
                    df[col] = numeric_series.astype("Int64")
                # Handle other numeric properties
                elif prop_name in [
                    "aromatic_rings",
                    "hba",
                    "hba_lipinski",
                    "hbd",
                    "hbd_lipinski",
                    "heavy_atoms",
                    "num_lipinski_ro5_violations",
                    "num_ro5_violations",
                    "rtb",
                ]:
                    numeric_series = pd.to_numeric(df[col], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                    numeric_series = numeric_series.where(numeric_series >= 0)
                    df[col] = numeric_series.astype("Int64")

        log.debug(LogEvents.NORMALIZE_NUMERIC_FIELDS_COMPLETED)
        return df

    def _check_empty_columns(self, df: pd.DataFrame, log: Any) -> None:
        """Check for mostly empty columns after transformation.

        Logs a warning when more than 95% of values in key fields are empty.
        This often indicates missing select_fields or improper flatten_objects configuration.

        Parameters
        ----------
        df:
            DataFrame produced by the transform stage.
        log:
            Logger used for emitting warnings.
        """
        if df.empty:
            return

        # Key fields to inspect (scalar and flattened molecule_properties).
        key_fields = [
            "molecule_chembl_id",
            "pref_name",
            "molecule_type",
            "availability_type",
            "chirality",
            "first_approval",
            "first_in_class",
            "indication_class",
            "helm_notation",
            "molecule_properties__cx_logp",
            "molecule_properties__cx_logd",
            "molecule_properties__mw_monoisotopic",
            "molecule_properties__hba_lipinski",
            "molecule_properties__hbd_lipinski",
            "molecule_properties__molecular_species",
            "molecule_properties__num_lipinski_ro5_violations",
        ]

        # Evaluate only fields present in the DataFrame.
        available_key_fields = [col for col in key_fields if col in df.columns]
        if not available_key_fields:
            return

        # Calculate the percentage of empty values for each key field.
        empty_percentages: dict[str, float] = {}
        for col in available_key_fields:
            if len(df) > 0:
                empty_count = df[col].isna().sum()
                empty_percentage = (empty_count / len(df)) * 100.0
                empty_percentages[col] = empty_percentage

        # Collect fields with more than 95% empty values.
        highly_empty_fields = {col: pct for col, pct in empty_percentages.items() if pct > 95.0}

        if highly_empty_fields:
            log.warning(LogEvents.HIGHLY_EMPTY_COLUMNS_DETECTED,
                empty_fields=highly_empty_fields,
                message=(
                    f"Fields with >95% empty values detected: {highly_empty_fields}. "
                    "This may indicate missing fields in select_fields or flatten_objects configuration."
                ),
            )

    def _remove_extra_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Remove columns that are not in the schema."""

        if df.empty:
            return df

        from bioetl.schemas.chembl_testitem_schema import COLUMN_ORDER

        schema_columns = set(COLUMN_ORDER)
        existing_columns = set(df.columns)
        extra_columns = existing_columns - schema_columns

        if extra_columns:
            df = df.drop(columns=list(extra_columns))
            log.debug(LogEvents.REMOVE_EXTRA_COLUMNS_COMPLETED,
                removed_columns=list(extra_columns),
                remaining_columns=list(df.columns),
            )

        return df

    def _deduplicate_molecules(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Deduplicate molecules by standard_inchi_key (fallback to molecule_chembl_id)."""

        if df.empty:
            return df

        rows_before = len(df)

        # Check for standard_inchi_key in both old and new column names
        inchi_key_col = (
            "molecule_structures__standard_inchi_key"
            if "molecule_structures__standard_inchi_key" in df.columns
            else "standard_inchi_key"
        )
        canonical_smiles_col = (
            "molecule_structures__canonical_smiles"
            if "molecule_structures__canonical_smiles" in df.columns
            else "canonical_smiles"
        )
        full_mwt_col = (
            "molecule_properties__full_mwt"
            if "molecule_properties__full_mwt" in df.columns
            else "full_mwt"
        )
        alogp_col = (
            "molecule_properties__alogp" if "molecule_properties__alogp" in df.columns else "alogp"
        )

        # Prioritize records with non-empty canonical_smiles and more complete properties
        if inchi_key_col in df.columns:
            # Sort by completeness (more complete first)
            completeness_cols = [canonical_smiles_col, full_mwt_col, alogp_col]
            available_completeness = [col for col in completeness_cols if col in df.columns]
            if available_completeness:
                # Count non-null values in completeness columns
                df["_completeness"] = df[available_completeness].notna().sum(axis=1)
                df = df.sort_values(
                    ["_completeness", canonical_smiles_col],
                    ascending=[False, False],
                    na_position="last",
                )
                df = df.drop(columns=["_completeness"])

            # Deduplicate by standard_inchi_key, keeping first (most complete)
            df = df.drop_duplicates(subset=[inchi_key_col], keep="first")
        else:
            # Fallback: deduplicate by molecule_chembl_id
            df = df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

        rows_after = len(df)
        dropped = rows_before - rows_after
        if dropped > 0:
            log.info(LogEvents.DEDUPLICATE_MOLECULES_COMPLETED,
                rows_before=rows_before,
                rows_after=rows_after,
                dropped=dropped,
            )

        return df
