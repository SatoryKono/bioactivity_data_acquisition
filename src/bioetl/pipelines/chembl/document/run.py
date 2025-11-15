"""Document pipeline implementation for ChEMBL."""

from __future__ import annotations

import re
import time
from collections.abc import Iterable, Mapping, Sequence
from numbers import Integral, Real
from typing import Any, TypeVar, cast

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.clients.client_chembl import ChemblClient
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.config import DocumentSourceConfig
from bioetl.config.models.models import PipelineConfig
from bioetl.config.models.source import SourceConfig
from bioetl.core import UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.core.schema import StringRule, normalize_string_columns
from bioetl.schemas import SchemaRegistryEntry
from bioetl.schemas.pipeline_contracts import get_out_schema

from .._constants import API_DOCUMENT_FIELDS, DOCUMENT_MUST_HAVE_FIELDS
from ..common.descriptor import (
    BatchExtractionContext,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from ..common.enrich import _enrich_flag, _extract_enrich_config
from .normalize import enrich_with_document_terms

SelfChemblDocumentPipeline = TypeVar(
    "SelfChemblDocumentPipeline", bound="ChemblDocumentPipeline"
)

class ChemblDocumentPipeline(ChemblPipelineBase):
    """ETL pipeline extracting document records from the ChEMBL API."""

    actor = "document_chembl"
    id_column = "document_chembl_id"
    extract_event_name = "chembl_document.extract_mode"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._last_batch_extract_stats: dict[str, Any] | None = None
        self._output_schema_entry: SchemaRegistryEntry = get_out_schema(self.pipeline_code)
        self._output_schema = self._output_schema_entry.schema
        self._output_column_order = self._output_schema_entry.column_order

    def build_descriptor(
        self: SelfChemblDocumentPipeline,
    ) -> ChemblExtractionDescriptor[SelfChemblDocumentPipeline]:
        """Return the descriptor powering the shared extraction routine."""

        def _require_document_pipeline(
            pipeline: ChemblPipelineBase,
        ) -> ChemblDocumentPipeline:
            if isinstance(pipeline, ChemblDocumentPipeline):
                return pipeline
            msg = "ChemblDocumentPipeline instance required"
            raise TypeError(msg)

        def build_context(
            pipeline: SelfChemblDocumentPipeline,
            source_config: Any,
            log: BoundLogger,
        ) -> ChemblExtractionContext:
            document_pipeline = _require_document_pipeline(pipeline)
            typed_source_config = (
                source_config
                if isinstance(source_config, DocumentSourceConfig)
                else DocumentSourceConfig.from_source_config(cast(Any, source_config))
            )

            bundle = document_pipeline.build_chembl_entity_bundle(
                "document",
                source_name="chembl",
                source_config=typed_source_config,
            )
            if "chembl_document_client" not in document_pipeline._registered_clients:
                document_pipeline.register_client("chembl_document_client", bundle.api_client)
            chembl_client = bundle.chembl_client
            document_client = document_pipeline._build_document_client(
                chembl_client=bundle.chembl_client,
                source_config=typed_source_config,
            )
            document_pipeline._set_chembl_release(
                document_pipeline.fetch_chembl_release(chembl_client, log)
            )
            select_fields = document_pipeline._resolve_select_fields(
                cast(SourceConfig[Any], cast(Any, typed_source_config)),
                default_fields=API_DOCUMENT_FIELDS,
            )

            context = ChemblExtractionContext(typed_source_config, document_client)
            context.chembl_client = chembl_client
            context.select_fields = tuple(select_fields) if select_fields else None
            context.chembl_release = document_pipeline.chembl_release
            return context

        def empty_frame(
            _: SelfChemblDocumentPipeline,
            __: ChemblExtractionContext,
        ) -> pd.DataFrame:
            return pd.DataFrame({"document_chembl_id": pd.Series(dtype="string")})

        def record_transform(
            pipeline: SelfChemblDocumentPipeline,
            payload: Mapping[str, Any],
            _: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            document_pipeline = _require_document_pipeline(pipeline)
            return document_pipeline._extract_nested_fields(dict(payload))

        def summary_extra(
            pipeline: SelfChemblDocumentPipeline,
            df: pd.DataFrame,
            context: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            _require_document_pipeline(pipeline)
            page_size = context.page_size or 0
            pages = 0
            if page_size > 0:
                total_rows = int(df.shape[0])
                pages = (total_rows + page_size - 1) // page_size
            return {"pages": pages}

        return ChemblExtractionDescriptor[SelfChemblDocumentPipeline](
            name="chembl_document",
            source_name="chembl",
            source_config_factory=DocumentSourceConfig.from_source_config,
            build_context=build_context,
            id_column="document_chembl_id",
            summary_event="chembl_document.extract_summary",
            must_have_fields=DOCUMENT_MUST_HAVE_FIELDS,
            default_select_fields=API_DOCUMENT_FIELDS,
            record_transform=record_transform,
            sort_by=("document_chembl_id",),
            empty_frame_factory=empty_frame,
            summary_extra=summary_extra,
        )

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract document records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of document_chembl_id values to extract (as strings).

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted document records.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = DocumentSourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "document",
            source_name="chembl",
            source_config=source_config,
        )
        if "chembl_document_client" not in self._registered_clients:
            self.register_client("chembl_document_client", bundle.api_client)
        chembl_client = bundle.chembl_client
        document_client = self._build_document_client(
            chembl_client=bundle.chembl_client,
            source_config=source_config,
        )

        self._set_chembl_release(self.fetch_chembl_release(chembl_client, log))

        resolved_select_fields = self._resolve_select_fields(
            source_raw, default_fields=list(API_DOCUMENT_FIELDS)
        )
        merged_select_fields = self._merge_select_fields(
            resolved_select_fields,
            DOCUMENT_MUST_HAVE_FIELDS,
        )

        limit = self.config.cli.limit

        def fetch_documents(
            batch_ids: Sequence[str],
            context: BatchExtractionContext,
        ) -> Iterable[Mapping[str, Any]]:
            if "original_paginate" not in context.extra:
                original_paginate = chembl_client.paginate

                def counted_paginate(*args: Any, **kwargs: Any) -> Any:
                    context.increment_api_calls()
                    return original_paginate(*args, **kwargs)

                chembl_client.paginate = counted_paginate  # type: ignore[method-assign]
                context.extra["original_paginate"] = original_paginate

            iterator = document_client.iterate_by_ids(
                batch_ids,
                select_fields=context.select_fields or None,
            )
            for item in iterator:
                yield self._extract_nested_fields(dict(item))

        def finalize_context(context: BatchExtractionContext) -> None:
            original = context.extra.pop("original_paginate", None)
            if original is not None:
                chembl_client.paginate = original  # type: ignore[method-assign]

            api_calls_value = context.stats.api_calls if context.stats.api_calls is not None else 0
            override = {
                "batches": context.stats.batches,
                "api_calls": api_calls_value,
                "cache_hits": context.stats.cache_hits,
            }
            context.extra["stats_attribute_override"] = override

        dataframe, stats = self.run_batched_extraction(
            ids,
            id_column="document_chembl_id",
            fetcher=fetch_documents,
            select_fields=merged_select_fields or None,
            batch_size=document_client.batch_size,
            max_batch_size=25,
            limit=limit,
            metadata_filters={"select_fields": merged_select_fields} if merged_select_fields else None,
            chembl_release=self.chembl_release,
            finalize_context=finalize_context,
            stats_attribute="_last_batch_extract_stats",
        )

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(LogEvents.CHEMBL_DOCUMENT_EXTRACT_BY_IDS_SUMMARY,
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_release=self.chembl_release,
            batches=stats.batches,
            api_calls=stats.api_calls,
            cache_hits=stats.cache_hits,
        )
        return dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw document data by normalizing fields and identifiers."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.transform")

        df = df.copy()

        if df.empty:
            log.debug(LogEvents.TRANSFORM_EMPTY_DATAFRAME)
            return df

        log.info(LogEvents.STAGE_TRANSFORM_START, rows=len(df))

        df = self._normalize_and_enforce_schema(
            df,
            self._output_column_order,
            log,
            order_columns=False,
        )

        # Normalize numeric fields
        df = self._normalize_numeric_fields(df, log)

        # Enrichment: document_term fields
        if self._should_enrich_document_terms():
            df = self._enrich_document_terms(df)

        # Add system fields
        df = self._add_system_fields(df, log)

        # Deduplicate by document_chembl_id before validation
        # Schema requires unique document_chembl_id, so we need to remove duplicates
        # Use deterministic deduplication: sort by all columns, then keep first occurrence
        if "document_chembl_id" in df.columns and df["document_chembl_id"].duplicated().any():
            initial_count = len(df)
            # Sort by all columns for deterministic deduplication
            df = df.sort_values(by=list(df.columns)).drop_duplicates(
                subset=["document_chembl_id"], keep="first"
            )
            deduped_count = len(df)
            if deduped_count < initial_count:
                log.warning(LogEvents.DOCUMENT_DEDUPLICATION_APPLIED,
                    initial_count=initial_count,
                    deduped_count=deduped_count,
                    removed_count=initial_count - deduped_count,
                )

        df = self._normalize_and_enforce_schema(
            df,
            self._output_column_order,
            log,
            normalize_identifiers=False,
            normalize_strings=False,
            order_columns=True,
        )

        log.info(LogEvents.STAGE_TRANSFORM_FINISH, rows=len(df))
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate payload against the registered output schema with detailed errors."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.validate")

        if df.empty:
            log.debug(LogEvents.VALIDATE_EMPTY_DATAFRAME)
            return df

        if self.config.validation.strict:
            allowed_columns = set(self._output_column_order)
            extra_columns = [column for column in df.columns if column not in allowed_columns]
            if extra_columns:
                log.debug(LogEvents.DROP_EXTRA_COLUMNS_BEFORE_VALIDATION,
                    extras=extra_columns,
                )
                df = df.drop(columns=extra_columns)

        log.info(LogEvents.STAGE_VALIDATE_START, rows=len(df))

        # Pre-validation checks
        self._check_document_id_uniqueness(df, log)

        # Call base validation
        validated = super().validate(df)
        log.info(LogEvents.STAGE_VALIDATE_FINISH,
            rows=len(validated),
            schema=self.config.validation.schema_out,
            strict=self.config.validation.strict,
            coerce=self.config.validation.coerce,
        )
        return validated

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize identifier fields (DOI, PMID)."""
        df = df.copy()

        # Normalize DOI
        if "doi" in df.columns:
            df["doi_clean"] = df["doi"].apply(self._normalize_doi)  # pyright: ignore[reportUnknownMemberType]

        # Normalize PMID
        if "pubmed_id" in df.columns:
            df["pubmed_id"] = pd.to_numeric(df["pubmed_id"], errors="coerce").astype("Int64")  # pyright: ignore[reportUnknownMemberType]

        return df

    @staticmethod
    def _normalize_doi(doi: str | None) -> str:
        """Normalize DOI by removing prefixes and validating format."""
        if not doi:
            return ""
        if not isinstance(doi, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            return ""
        doi = doi.strip().lower()
        # Remove prefixes
        for prefix in ["doi:", "https://doi.org/", "http://dx.doi.org/", "http://doi.org/"]:
            if doi.startswith(prefix):
                doi = doi[len(prefix) :]
        doi = doi.strip()
        # Validate regex
        doi_pattern = re.compile(r"^10\.\d{4,9}/\S+$")
        if doi_pattern.match(doi):
            return doi
        return ""

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields (title, abstract, journal, authors)."""
        working_df = df.copy()

        rules = {
            "title": StringRule(max_length=1000),
            "abstract": StringRule(max_length=5000),
        }

        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)

        if stats.has_changes:
            log.debug(LogEvents.STRING_FIELDS_NORMALIZED,
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )

        # Normalize journal
        if "journal" in normalized_df.columns:
            journal_series: pd.Series[Any] = normalized_df["journal"]
            normalized_df["journal"] = journal_series.map(
                lambda value: self._normalize_journal(value)
            )

        # Normalize authors
        if "authors" in normalized_df.columns:
            def _to_author_tuple(item: object) -> tuple[str, int] | None:
                if not isinstance(item, tuple):
                    return None
                tuple_item = cast(tuple[object, ...], item)
                if len(tuple_item) != 2:
                    return None
                name_raw, count_raw = tuple_item
                if not isinstance(name_raw, str):
                    return None
                name_value: str = name_raw
                if isinstance(count_raw, Integral):
                    count_value = int(count_raw)
                elif isinstance(count_raw, Real):
                    float_value = float(count_raw)
                    if not float_value.is_integer():
                        return None
                    count_value = int(float_value)
                else:
                    return None
                if count_value < 0:
                    return None
                return (name_value, count_value)

            def _author_name_from_tuple(data: tuple[str, int] | None) -> str:
                return data[0] if data is not None else ""

            def _author_count_from_tuple(data: tuple[str, int] | None) -> int:
                return data[1] if data is not None else 0

            authors_series: pd.Series[Any] = normalized_df["authors"]
            normalized_result = authors_series.apply(self._normalize_authors)
            normalized_tuples = normalized_result.apply(_to_author_tuple)
            normalized_df["authors"] = normalized_tuples.apply(_author_name_from_tuple)
            normalized_df["authors_count"] = normalized_tuples.apply(_author_count_from_tuple)

        return normalized_df

    @staticmethod
    def _normalize_journal(value: Any, max_len: int = 255) -> str:
        """Trim and collapse whitespace for journal name."""
        if pd.isna(value):
            return ""
        text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_len] if len(text) > max_len else text

    @staticmethod
    def _normalize_authors(authors: Any, separator: str = ", ") -> tuple[str, int]:
        """Normalize author separators and count."""
        if pd.isna(authors):
            return ("", 0)
        text = str(authors).strip()
        text = re.sub(r";", ",", text)  # ; → ,
        text = re.sub(r"\s+", " ", text)  # collapse whitespace
        if not text:
            return ("", 0)
        parts = text.split(",")
        parts = [p.strip() for p in parts if p.strip()]
        return (separator.join(parts), len(parts))

    def _normalize_numeric_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize numeric fields (year)."""
        df = df.copy()

        # Normalize year
        if "year" in df.columns:
            def _coerce_year(value: object) -> int | None:
                if value is None or value is pd.NA:
                    return None
                if isinstance(value, Integral):
                    year_int = int(value)
                elif isinstance(value, Real):
                    float_value = float(value)
                    if not float_value.is_integer():
                        return None
                    year_int = int(float_value)
                elif isinstance(value, str):
                    stripped = value.strip()
                    if not stripped:
                        return None
                    if not stripped.isdigit():
                        return None
                    year_int = int(stripped)
                else:
                    return None

                if 1500 <= year_int <= 2100:
                    return year_int
                return None

            normalized_year = df["year"].apply(_coerce_year)
            df["year"] = normalized_year.astype("Int64")

        return df

    def _add_system_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Add document-specific system fields (source)."""
        df = df.copy()

        # Add source field
        df["source"] = "ChEMBL"

        return df

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        specs = dict(super()._schema_column_specs())
        specs["source"] = {"default": "ChEMBL"}
        specs["authors_count"] = {"default": 0, "dtype": "Int64"}

        hashing_config = self.config.determinism.hashing
        business_key_column = hashing_config.business_key_column
        row_hash_column = hashing_config.row_hash_column

        if business_key_column:
            specs[business_key_column] = {"default": ""}
        if row_hash_column:
            specs[row_hash_column] = {"default": ""}

        return specs

    def _check_document_id_uniqueness(self, df: pd.DataFrame, log: Any) -> None:
        """Check that document_chembl_id is unique."""
        if df.empty:
            return
        if "document_chembl_id" not in df.columns:
            return
        duplicates = df["document_chembl_id"].duplicated()
        if duplicates.any():
            duplicate_ids = (
                df[df["document_chembl_id"].duplicated()]["document_chembl_id"].unique().tolist()
            )
            log.warning(LogEvents.DOCUMENT_ID_DUPLICATES,
                duplicate_count=duplicates.sum(),
                duplicate_ids=duplicate_ids[:10],  # Limit to first 10
            )

    def _should_enrich_document_terms(self) -> bool:
        """Return True when document_term enrichment is enabled in the config."""
        chembl_config = cast(Mapping[str, Any] | None, self.config.chembl)
        return _enrich_flag(
            chembl_config,
            ("document", "enrich", "document_term", "enabled"),
        )

    def _enrich_document_terms(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply document_term enrichment to the DataFrame."""
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.enrich")

        chembl_config = cast(Mapping[str, Any] | None, self.config.chembl)
        enrich_cfg = _extract_enrich_config(
            chembl_config,
            ("document", "enrich", "document_term"),
            log=log,
        )

        # Create or reuse the ChEMBL client.
        source_raw = self._resolve_source_config("chembl")
        source_config = DocumentSourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "document_term",
            source_name="chembl",
            source_config=source_config,
        )
        if "chembl_enrichment_client" not in self._registered_clients:
            self.register_client("chembl_enrichment_client", bundle.api_client)
        chembl_client = bundle.chembl_client

        # Invoke the enrichment routine.
        return enrich_with_document_terms(df, chembl_client, enrich_cfg)

    def _extract_nested_fields(self, record: dict[str, Any]) -> dict[str, Any]:
        """Extract fields from nested objects in document records."""
        # For documents, there are typically no nested objects to extract
        # But we keep this method for consistency with other pipelines
        return record

    def _build_document_client(
        self,
        *,
        chembl_client: ChemblClient,
        source_config: DocumentSourceConfig,
    ) -> ChemblDocumentClient:
        """Instantiate a document client honoring runtime monkeypatching."""

        batch_size = self._resolve_batch_size(source_config)
        parameters = source_config.parameters_mapping()
        max_url_candidate = parameters.get("max_url_length")
        max_url_length: int | None = None
        if isinstance(max_url_candidate, Integral):
            candidate = int(max_url_candidate)
            if candidate > 0:
                max_url_length = candidate
        client_kwargs: dict[str, Any] = {"batch_size": batch_size}
        if max_url_length is not None:
            client_kwargs["max_url_length"] = max_url_length
        return ChemblDocumentClient(chembl_client, **client_kwargs)
