"""Assay pipeline implementation for ChEMBL."""

from __future__ import annotations

import re
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, TypeVar, cast

import pandas as pd
from pandas import Series
from structlog.stdlib import BoundLogger

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.clients.entities.client_assay import ChemblAssayClient
from bioetl.config import AssaySourceConfig, PipelineConfig
from bioetl.core import UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.core.schema import (
    IdentifierRule,
    StringRule,
    normalize_identifier_columns,
    normalize_string_columns,
)
from bioetl.schemas.chembl_assay_schema import COLUMN_ORDER, AssaySchema

from ..common.descriptor import (
    BatchExtractionContext,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from .normalize import (
    enrich_with_assay_classifications,
    enrich_with_assay_parameters,
)
from .transform import (
    serialize_array_fields,
    validate_assay_parameters_truv,
)

SelfChemblAssayPipeline = TypeVar("SelfChemblAssayPipeline", bound="ChemblAssayPipeline")

_CLASSIFICATION_ID_KEYS: tuple[str, ...] = (
    "assay_class_id",
    "class_id",
    "id",
    "bao_format",
)

_BAO_CANONICAL_PATTERN = re.compile(r"^BAO_\d{7}$")
_BAO_FLEXIBLE_PATTERN = re.compile(r"^BAO[:_](\d{7})$", re.IGNORECASE)
_BAO_DIGIT_PATTERN = re.compile(r"^(\d{7})$")


def _normalize_bao_identifier(raw_value: Any) -> str | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, float) and pd.isna(raw_value):
        return None

    text = str(raw_value).strip()
    if not text:
        return None

    upper = text.upper()
    if _BAO_CANONICAL_PATTERN.match(upper):
        return upper

    flexible_match = _BAO_FLEXIBLE_PATTERN.match(upper)
    if flexible_match:
        return f"BAO_{flexible_match.group(1)}"

    digit_match = _BAO_DIGIT_PATTERN.match(upper)
    if digit_match:
        return f"BAO_{digit_match.group(1)}"

    return None


def _iter_classification_mappings(node: Any) -> Iterator[Mapping[str, Any]]:
    if node is None:
        return

    if isinstance(node, Mapping):
        mapping = cast(Mapping[str, Any], node)
        yield mapping
        for nested_value in mapping.values():
            yield from _iter_classification_mappings(nested_value)
        return

    if isinstance(node, (list, tuple, set)):
        iterable = cast(Iterable[Any], node)
        for item in iterable:
            yield from _iter_classification_mappings(item)


def _extract_bao_ids_from_classifications(node: Any) -> list[str]:
    identifiers: list[str] = []
    seen: set[str] = set()

    for mapping in _iter_classification_mappings(node):
        for key in _CLASSIFICATION_ID_KEYS:
            if key not in mapping:
                continue
            normalized = _normalize_bao_identifier(mapping.get(key))
            if normalized is None:
                continue
            if normalized in seen:
                break
            seen.add(normalized)
            identifiers.append(normalized)
            break

    return identifiers

# Required fields that must always be requested from the API.
MUST_HAVE_FIELDS: tuple[str, ...] = (
    "assay_chembl_id",
    #   "assay_category",
    #   "assay_group",
    #   "src_assay_id",
    #   "curation_level",
)


class ChemblAssayPipeline(ChemblPipelineBase):
    """ETL pipeline extracting assay records from the ChEMBL API."""

    actor = "assay_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._chembl_release: str | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release captured during extraction."""

        return self._chembl_release

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch assay payloads from ChEMBL using the configured HTTP client.

        Checks for input_file in config.cli.input_file and calls extract_by_ids()
        if present, otherwise calls extract_all().
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))

        return self._dispatch_extract_mode(
            log,
            event_name="chembl_assay.extract_mode",
            batch_callback=self.extract_by_ids,
            full_callback=self.extract_all,
            id_column_name="assay_chembl_id",
        )

    def extract_all(self) -> pd.DataFrame:
        """Extract all assay records from ChEMBL using pagination."""

        descriptor = self._build_assay_descriptor()
        return self.run_extract_all(descriptor)

    def _build_assay_descriptor(
        self: SelfChemblAssayPipeline,
    ) -> ChemblExtractionDescriptor[SelfChemblAssayPipeline]:
        """Return the descriptor powering the shared extraction template."""

        def _require_assay_pipeline(pipeline: ChemblPipelineBase) -> ChemblAssayPipeline:
            if isinstance(pipeline, ChemblAssayPipeline):
                return pipeline
            msg = "ChemblAssayPipeline instance required"
            raise TypeError(msg)

        def build_context(
            pipeline: SelfChemblAssayPipeline,
            source_config: AssaySourceConfig,
            log: BoundLogger,
        ) -> ChemblExtractionContext:
            assay_pipeline = _require_assay_pipeline(pipeline)
            bundle = assay_pipeline.build_chembl_entity_bundle(
                "assay",
                source_name="chembl",
                source_config=source_config,
            )
            if "chembl_assay_http" not in assay_pipeline._registered_clients:
                assay_pipeline.register_client("chembl_assay_http", bundle.api_client)
            chembl_client = bundle.chembl_client
            assay_client = cast(ChemblAssayClient, bundle.entity_client)
            if assay_client is None:
                msg = "Фабрика вернула пустой клиент для 'assay'"
                raise RuntimeError(msg)

            assay_client.handshake(
                endpoint=source_config.parameters.handshake_endpoint,
                enabled=source_config.parameters.handshake_enabled,
            )
            assay_pipeline._chembl_release = assay_client.chembl_release
            log.info(LogEvents.CHEMBL_ASSAY_HANDSHAKE,
                chembl_release=assay_pipeline._chembl_release,
                handshake_endpoint=source_config.parameters.handshake_endpoint,
                handshake_enabled=source_config.parameters.handshake_enabled,
            )

            raw_source = assay_pipeline._resolve_source_config("chembl")
            select_fields = assay_pipeline._resolve_select_fields(raw_source)
            log.debug(LogEvents.CHEMBL_ASSAY_SELECT_FIELDS,
                fields=select_fields,
                fields_count=len(select_fields) if select_fields else 0,
            )

            context = ChemblExtractionContext(source_config, assay_client)
            context.chembl_client = chembl_client
            context.select_fields = (
                tuple(select_fields) if select_fields else None
            )
            context.chembl_release = assay_pipeline._chembl_release
            context.extra_filters = {"max_url_length": source_config.max_url_length}
            return context

        def empty_frame(
            _: SelfChemblAssayPipeline,
            __: ChemblExtractionContext,
        ) -> pd.DataFrame:
            return pd.DataFrame({"assay_chembl_id": pd.Series(dtype="string")})

        def post_process(
            pipeline: SelfChemblAssayPipeline,
            df: pd.DataFrame,
            context: ChemblExtractionContext,
            log: BoundLogger,
        ) -> pd.DataFrame:
            assay_pipeline = _require_assay_pipeline(pipeline)

            if df.empty:
                return df

            for must_field in ("assay_category", "assay_group", "src_assay_id"):
                if must_field not in df.columns or df[must_field].isna().all():
                    log.warning(LogEvents.CHEMBL_ASSAY_MISSING_REQUIRED_FIELD,
                        field=must_field,
                        note="Field not returned by API; check select_fields/only",
                    )

            select_fields = context.select_fields
            if select_fields:
                expected_fields = set(select_fields)
                actual_fields = set(df.columns)
                missing_in_response = sorted(expected_fields - actual_fields)
                if missing_in_response:
                    log.warning(LogEvents.ASSAY_MISSING_FIELDS_IN_API_RESPONSE,
                        missing_fields=missing_in_response,
                        requested_fields_count=len(select_fields),
                        received_fields_count=len(actual_fields),
                        chembl_release=assay_pipeline._chembl_release,
                        message=(
                            "Fields requested in select_fields but missing in API response: "
                            f"{missing_in_response}"
                        ),
                    )

            df = assay_pipeline._check_missing_columns(
                df,
                log,
                select_fields=list(select_fields) if select_fields else None,
            )
            return df

        def dry_run_handler(
            pipeline: SelfChemblAssayPipeline,
            _: ChemblExtractionContext,
            log: BoundLogger,
            stage_start: float,
        ) -> pd.DataFrame:
            assay_pipeline = _require_assay_pipeline(pipeline)

            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(LogEvents.CHEMBL_ASSAY_EXTRACT_SKIPPED,
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=assay_pipeline._chembl_release,
            )
            return pd.DataFrame()

        def summary_extra(
            pipeline: SelfChemblAssayPipeline,
            _: pd.DataFrame,
            context: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            assay_pipeline = _require_assay_pipeline(pipeline)

            return {
                "handshake_endpoint": context.source_config.parameters.handshake_endpoint,
                "limit": assay_pipeline.config.cli.limit,
            }

        return ChemblExtractionDescriptor[SelfChemblAssayPipeline](
            name="chembl_assay",
            source_name="chembl",
            source_config_factory=AssaySourceConfig.from_source_config,
            build_context=build_context,
            id_column="assay_chembl_id",
            summary_event="chembl_assay.extract_summary",
            must_have_fields=MUST_HAVE_FIELDS,
            default_select_fields=MUST_HAVE_FIELDS,
            post_processors=(post_process,),
            sort_by=("assay_chembl_id",),
            empty_frame_factory=empty_frame,
            dry_run_handler=dry_run_handler,
            summary_extra=summary_extra,
        )

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract assay records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of assay_chembl_id values to extract.

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted assay records.
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = AssaySourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "assay",
            source_name="chembl",
            source_config=source_config,
        )
        if "chembl_assay_http" not in self._registered_clients:
            self.register_client("chembl_assay_http", bundle.api_client)

        chembl_client = bundle.chembl_client
        assay_client = cast(ChemblAssayClient, bundle.entity_client)
        if assay_client is None:
            msg = "Фабрика вернула пустой клиент для 'assay'"
            raise RuntimeError(msg)

        assay_client.handshake(
            endpoint=source_config.parameters.handshake_endpoint,
            enabled=source_config.parameters.handshake_enabled,
        )
        self._chembl_release = assay_client.chembl_release

        log.info(LogEvents.CHEMBL_ASSAY_HANDSHAKE,
            chembl_release=self._chembl_release,
            handshake_endpoint=source_config.parameters.handshake_endpoint,
            handshake_enabled=source_config.parameters.handshake_enabled,
        )

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(LogEvents.CHEMBL_ASSAY_EXTRACT_SKIPPED,
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=self._chembl_release,
            )
            return pd.DataFrame()

        limit = self.config.cli.limit
        resolved_select_fields = self._resolve_select_fields(source_raw)
        merged_select_fields = self._merge_select_fields(resolved_select_fields, MUST_HAVE_FIELDS)

        log.debug(LogEvents.CHEMBL_ASSAY_SELECT_FIELDS,
            fields=merged_select_fields,
            fields_count=len(merged_select_fields) if merged_select_fields else 0,
        )

        def fetch_assays(
            batch_ids: Sequence[str],
            context: BatchExtractionContext,
        ) -> Iterable[Mapping[str, Any]]:
            iterator = assay_client.iterate_by_ids(
                batch_ids,
                select_fields=context.select_fields or None,
            )
            for item in iterator:
                yield dict(item)

        def finalize_dataframe(
            dataframe: pd.DataFrame, context: BatchExtractionContext
        ) -> pd.DataFrame:
            if dataframe.empty:
                return dataframe

            for must_field in ("assay_category", "assay_group", "src_assay_id"):
                if must_field not in dataframe.columns or dataframe[must_field].isna().all():
                    log.warning(LogEvents.CHEMBL_ASSAY_MISSING_REQUIRED_FIELD,
                        field=must_field,
                        note="Field not returned by API; check select_fields/only",
                    )

            if context.select_fields:
                expected_fields = set(context.select_fields)
                actual_fields = set(dataframe.columns)
                missing_in_response = sorted(expected_fields - actual_fields)
                if missing_in_response:
                    log.warning(LogEvents.ASSAY_MISSING_FIELDS_IN_API_RESPONSE,
                        missing_fields=missing_in_response,
                        requested_fields_count=len(context.select_fields),
                        received_fields_count=len(actual_fields),
                        chembl_release=self._chembl_release,
                        message=(
                            "Fields requested in select_fields but missing in API response: "
                            f"{missing_in_response}"
                        ),
                    )

            dataframe = self._check_missing_columns(
                dataframe,
                log,
                select_fields=list(context.select_fields) if context.select_fields else None,
            )
            return dataframe

        dataframe, stats = self.run_batched_extraction(
            ids,
            id_column="assay_chembl_id",
            fetcher=fetch_assays,
            select_fields=merged_select_fields or None,
            batch_size=assay_client.batch_size,
            max_batch_size=25,
            limit=limit,
            metadata_filters={
                "select_fields": list(merged_select_fields) if merged_select_fields else None,
                "max_url_length": source_config.max_url_length,
            },
            chembl_release=self._chembl_release,
            finalize=finalize_dataframe,
        )

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(LogEvents.CHEMBL_ASSAY_EXTRACT_BY_IDS_SUMMARY,
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            limit=limit,
            batches=stats.batches,
            api_calls=stats.api_calls,
        )
        return dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw assay data by normalizing identifiers, types, and nested structures."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("transform"))

        df = df.copy()

        df = self._harmonize_identifier_columns(df, log)
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        if df.empty:
            log.debug(LogEvents.TRANSFORM_EMPTY_DATAFRAME)
            return df

        log.info(LogEvents.STAGE_TRANSFORM_START, rows=len(df))

        df = self._normalize_identifiers(df, log)
        df = self._normalize_string_fields(df, log)
        df = self._enrich_with_related_data(df, log)
        df = self._normalize_nested_structures(df, log)
        df = self._serialize_array_fields(df, log)
        df = self._add_row_metadata(df, log)
        df = self._normalize_data_types(df, AssaySchema, log)
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)
        df = self._order_schema_columns(df, COLUMN_ORDER)

        log.info(LogEvents.STAGE_TRANSFORM_FINISH, rows=len(df))
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _serialize_array_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Serialize array-of-object fields to header+rows format."""
        df = df.copy()

        # Get arrays_to_header_rows from config
        arrays_to_serialize: list[str] = list(self.config.transform.arrays_to_header_rows)

        if arrays_to_serialize:
            df_result: pd.DataFrame = serialize_array_fields(df, arrays_to_serialize)
            for column in arrays_to_serialize:
                if column in df_result.columns:
                    column_as_string: Series = df_result[column].astype("string")
                    filled_column: Series = column_as_string.copy()
                    filled_column[column_as_string.isna()] = ""
                    empty_mask: Series = filled_column.eq("")
                    if bool(empty_mask.any()):
                        df_result.loc[empty_mask, column] = pd.NA
            df = df_result
            log.debug(LogEvents.ARRAY_FIELDS_SERIALIZED, columns=arrays_to_serialize)

        return df

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
            log.debug(LogEvents.IDENTIFIER_HARMONIZATION, actions=actions)

        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize ChEMBL identifiers with regex validation."""

        rules = [
            IdentifierRule(
                name="chembl",
                columns=[
                    "assay_chembl_id",
                    "target_chembl_id",
                    "document_chembl_id",
                    "cell_chembl_id",
                    "tissue_chembl_id",
                ],
                pattern=r"^CHEMBL\d+$",
            ),
        ]

        normalized_df, stats = normalize_identifier_columns(df, rules)

        if stats.has_changes:
            log.debug(LogEvents.IDENTIFIERS_NORMALIZED,
                normalized_count=stats.normalized,
                invalid_count=stats.invalid,
                columns=list(stats.per_column.keys()),
            )

        return normalized_df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields (assay_type, assay_category, assay_organism, curation_level).

        Important: all fields are sourced directly from the API payload—no surrogate computations.
        - assay_category: pulled from ASSAYS.ASSAY_CATEGORY (not assay_type or BAO).
        - assay_strain: sourced from ASSAYS.ASSAY_STRAIN (not target/organism).
        - src_assay_id: sourced from ASSAYS.SRC_ASSAY_ID (not alternative sources).
        - assay_group: sourced from ASSAYS.ASSAY_GROUP.
        - curation_level: taken from an explicit column when available, otherwise NULL.
        """

        working_df = df.copy()

        string_fields = [
            "assay_type",
            "assay_category",
            "assay_organism",
            "assay_strain",
            "src_assay_id",
            "assay_group",
            "curation_level",
        ]

        rules = {column: StringRule() for column in string_fields}

        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)

        # Ensure curation_level defaults to NULL when absent in the payload.
        if "curation_level" not in normalized_df.columns:
            normalized_df["curation_level"] = pd.NA
            log.warning(LogEvents.CURATION_LEVEL_MISSING,
                message="curation_level not found in API response, setting to NULL",
            )

        if stats.has_changes:
            log.debug(LogEvents.STRING_FIELDS_NORMALIZED,
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )

        return normalized_df

    def _normalize_nested_structures(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Process nested structures (assay_parameters, assay_classifications).

        Important: do not derive assay_class_id from surrogates. It must come from ASSAY_CLASS_MAP
        via enrichment, otherwise remain NULL.
        """

        df = df.copy()

        # Important: never derive assay_class_id from bao_format or other surrogates.
        # assay_class_id must be filled via enrichment from ASSAY_CLASS_MAP.
        # When enrichment was not executed and the field is empty, keep it NULL.

        # Validate TRUV invariants for assay_parameters (fail-fast).
        if "assay_parameters" in df.columns:
            log.debug(LogEvents.VALIDATING_ASSAY_PARAMETERS_TRUV)
            df = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        if "assay_classifications" in df.columns:
            if "assay_class_id" not in df.columns:
                df["assay_class_id"] = pd.NA

            updated_rows = 0
            classifications_series = df["assay_classifications"]

            for row_index, value in classifications_series.items():
                if value is None or value is pd.NA:
                    continue
                if isinstance(value, float) and pd.isna(value):
                    continue
                if isinstance(value, str):
                    # Already serialized; enrichment happens later.
                    continue

                extracted_ids = _extract_bao_ids_from_classifications(value)
                if not extracted_ids:
                    continue

                joined_ids = ";".join(extracted_ids)
                row_label = cast(Any, row_index)
                current_value = df.at[row_label, "assay_class_id"]

                if pd.isna(current_value) or current_value != joined_ids:
                    df.at[row_label, "assay_class_id"] = joined_ids
                    updated_rows += 1

            if updated_rows > 0:
                log.debug(LogEvents.ASSAY_CLASS_ID_EXTRACTED_FROM_CLASSIFICATIONS,
                    rows_updated=updated_rows,
                )

        # Note: assay_parameters and assay_classifications are serialized in
        # _serialize_array_fields() and preserved in the final schema.

        return df

    def _add_row_metadata(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Add required row metadata fields (row_subtype, row_index)."""

        df = df.copy()
        if df.empty:
            return df

        # Add row_subtype: "assay" for all rows
        if "row_subtype" not in df.columns:
            df["row_subtype"] = "assay"
            log.debug(LogEvents.ROW_SUBTYPE_ADDED, value="assay")
        elif df["row_subtype"].isna().all():
            df["row_subtype"] = "assay"
            log.debug(LogEvents.ROW_SUBTYPE_FILLED, value="assay")

        # Add row_index: sequential index starting from 0
        if "row_index" not in df.columns:
            df["row_index"] = range(len(df))
            log.debug(LogEvents.ROW_INDEX_ADDED, count=len(df))
        elif df["row_index"].isna().all():
            df["row_index"] = range(len(df))
            log.debug(LogEvents.ROW_INDEX_FILLED, count=len(df))

        return df

    def _normalize_data_types(self, df: pd.DataFrame, schema: Any, log: Any) -> pd.DataFrame:
        """Convert data types according to the AssaySchema.

        Overrides base implementation to handle row_index and confidence_score specially.
        """
        df = super()._normalize_data_types(df, schema, log)

        # Handle row_index specially - fill NA values with sequential index
        if "row_index" in df.columns and df["row_index"].isna().any():
            df["row_index"] = range(len(df))
            log.debug(LogEvents.ROW_INDEX_FILLED, count=len(df))

        return df

    def _check_missing_columns(
        self, df: pd.DataFrame, log: Any, select_fields: list[str] | None = None
    ) -> pd.DataFrame:
        """Check for missing columns across ChEMBL releases (v34/v35).

        Adds missing optional columns with NULL values and logs warnings.
        Warns when select_fields did not request a missing column and verifies all schema-required
        columns that should originate from the API.

        Parameters
        ----------
        df:
            Assay DataFrame to inspect.
        log:
            Logger used to emit diagnostics.
        select_fields:
            Fields requested from the API via the ``only=`` parameter.

        Returns
        -------
        pd.DataFrame:
            DataFrame with missing columns added as needed.
        """
        df = df.copy()

        # Optional columns that may be absent in different ChEMBL releases.
        optional_columns = {
            "assay_strain": "v34",  # Introduced in v34.
            "assay_group": "v35",  # Introduced in v35.
            "curation_level": "unknown",  # May be missing in some releases.
        }

        # Schema fields that must be requested from the API (excluding metadata and enrichment outputs).
        expected_api_fields = {
            "assay_category",
            "assay_cell_type",
            "assay_group",
            "assay_strain",
            "assay_subcellular_fraction",
            "assay_test_type",
            "assay_tissue",
            "cell_chembl_id",
            "curation_level",
            "src_assay_id",
            "tissue_chembl_id",
            "variant_sequence",
        }
        # Note: assay_category and src_assay_id are already present in expected_api_fields.

        # Convert select_fields to a set for quick lookups.
        select_fields_set: set[str] = set()
        if select_fields is not None:
            select_fields_set = set(select_fields)

        # Inspect required fields from expected_api_fields.
        missing_in_response: list[str] = []
        missing_in_select_fields: list[str] = []
        for column in expected_api_fields:
            if column not in df.columns:
                missing_in_response.append(column)
                # Track whether the column was requested via select_fields.
                if select_fields is not None and column not in select_fields_set:
                    missing_in_select_fields.append(column)
                    log.warning(LogEvents.MISSING_FIELD_NOT_REQUESTED,
                        column=column,
                        chembl_release=self._chembl_release,
                        message=f"Field {column} not found in API response and was not requested in select_fields",
                    )
                else:
                    log.warning(LogEvents.MISSING_FIELD_IN_RESPONSE,
                        column=column,
                        chembl_release=self._chembl_release,
                        message=f"Field {column} was requested but not found in API response",
                    )

        # Inspect optional columns.
        missing_columns: list[str] = []
        for column, version in optional_columns.items():
            if column not in df.columns:
                # Add column initialized with NULL values.
                df[column] = pd.NA
                missing_columns.append(column)

                # Track whether the column was requested via select_fields.
                if select_fields is not None and column not in select_fields_set:
                    missing_in_select_fields.append(column)
                    log.warning(LogEvents.MISSING_COLUMN_NOT_REQUESTED,
                        column=column,
                        version_introduced=version,
                        chembl_release=self._chembl_release,
                        message=f"Column {column} not found in API response and was not requested in select_fields, setting to NULL",
                    )
                else:
                    log.warning(LogEvents.MISSING_OPTIONAL_COLUMN,
                        column=column,
                        version_introduced=version,
                        chembl_release=self._chembl_release,
                        message=f"Column {column} not found in API response, setting to NULL",
                    )

        if missing_in_response or missing_columns:
            log.debug(LogEvents.MISSING_COLUMNS_HANDLED,
                missing_in_response=missing_in_response if missing_in_response else None,
                missing_columns=missing_columns if missing_columns else None,
                missing_in_select_fields=sorted(missing_in_select_fields)
                if missing_in_select_fields
                else None,
                chembl_release=self._chembl_release,
            )

        return df

    def _enrich_with_related_data(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Enrich the DataFrame with related tables (ASSAY_CLASS_MAP, ASSAY_PARAMETERS)."""
        if df.empty:
            return df

        # Create an HTTP client for API requests using the same source as extract.
        try:
            source_raw = self._resolve_source_config("chembl")
        except KeyError as exc:
            log.debug(LogEvents.ENRICHMENT_SKIPPED_MISSING_SOURCE,
                source="chembl",
                message="Skipping enrichment: source configuration not available",
                error=str(exc),
            )
            return df
        source_config = AssaySourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "assay",
            source_name="chembl",
            source_config=source_config,
        )
        chembl_client = bundle.chembl_client

        # Retrieve enrichment configuration from config.chembl.assay.enrich.
        chembl_config = getattr(self.config, "chembl", None)
        if chembl_config is None:
            log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_CHEMBL_CONFIG, message="ChEMBL config not found")
            return df

        if not isinstance(chembl_config, Mapping):
            log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_CHEMBL_CONFIG, message="ChEMBL config is not a Mapping"
            )
            return df

        assay_config = cast(Mapping[str, Any], chembl_config).get("assay")
        if not isinstance(assay_config, Mapping):
            log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_ASSAY_CONFIG, message="Assay config not found")
            return df

        enrich_config = cast(Mapping[str, Any], assay_config).get("enrich")
        if not isinstance(enrich_config, Mapping):
            log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_ENRICH_CONFIG, message="Enrich config not found")
            return df

        # Enrich with classification data.
        classifications_cfg = cast(Mapping[str, Any], enrich_config).get("classifications")
        if classifications_cfg is not None:
            log.info(LogEvents.ENRICHMENT_CLASSIFICATIONS_STARTED)
            df_with_classifications: pd.DataFrame = enrich_with_assay_classifications(
                df, chembl_client, cast(Mapping[str, Any], classifications_cfg)
            )
            df = df_with_classifications
            log.info(LogEvents.ENRICHMENT_CLASSIFICATIONS_COMPLETED)

            # Diagnostic check to ensure assay_class_id is filled after enrichment.
            if "assay_class_id" in df.columns:
                filled_count = int(df["assay_class_id"].notna().sum())
                total_count = len(df)
                if filled_count == 0:
                    log.warning(LogEvents.ASSAY_CLASS_ID_EMPTY_AFTER_ENRICHMENT,
                        total_assays=total_count,
                        filled_count=0,
                        message="assay_class_id is empty after enrichment. Check if ASSAY_CLASS_MAP contains data for these assays.",
                    )
                else:
                    log.debug(LogEvents.ASSAY_CLASS_ID_ENRICHMENT_STATS,
                        total_assays=total_count,
                        filled_count=filled_count,
                        empty_count=total_count - filled_count,
                    )
            else:
                log.warning(
                    "assay_class_id_column_missing_after_enrichment",
                    message="assay_class_id column is missing after enrichment",
                )
        else:
            log.warning(LogEvents.ENRICHMENT_CLASSIFICATIONS_DISABLED,
                message="Enrichment for classifications is not configured. assay_class_id will remain NULL.",
            )

        # Enrich with assay parameter data.
        parameters_cfg = cast(Mapping[str, Any], enrich_config).get("parameters")
        if parameters_cfg is not None:
            log.info(LogEvents.ENRICHMENT_PARAMETERS_STARTED)
            df_with_parameters: pd.DataFrame = enrich_with_assay_parameters(
                df, chembl_client, cast(Mapping[str, Any], parameters_cfg)
            )
            df = df_with_parameters
            log.info(LogEvents.ENRICHMENT_PARAMETERS_COMPLETED)

        return df
