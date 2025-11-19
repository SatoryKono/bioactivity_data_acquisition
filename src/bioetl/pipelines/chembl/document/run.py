"""Document pipeline implementation for ChEMBL."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from numbers import Integral, Real
from typing import Any, TypeVar, cast

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.chembl.common.descriptor import (
    BatchExtractionContext,
    ChemblContextSpec,
    ChemblDescriptorSpec,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
    FetcherCallable,
    FetcherFactory,
    FinalizeContextCallable,
    FinalizeContextFactory,
    build_standard_chembl_context,
)
from bioetl.chembl.common.handlers import make_empty_frame_factory
from bioetl.chembl.common.enrich import _extract_enrich_config, enrich_flag
from bioetl.clients.client_chembl import ChemblClient
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.config import DocumentSourceConfig
from bioetl.config.models.models import PipelineConfig
from bioetl.config.models.source import SourceConfig
from bioetl.core.logging import LogEvents
from bioetl.core.schema import StringRule
from bioetl.core.schema.normalizers import StringStats
from bioetl.pipelines.unified_base import UnifiedPipelineBase
from bioetl.schemas.pipeline_contracts import get_out_schema

from .._constants import API_DOCUMENT_FIELDS, DOCUMENT_MUST_HAVE_FIELDS
from .normalize import enrich_with_document_terms

SelfChemblDocumentPipeline = TypeVar("SelfChemblDocumentPipeline", bound="ChemblDocumentPipeline")


class ChemblDocumentPipeline(UnifiedPipelineBase):
    """ETL pipeline extracting document records from the ChEMBL API."""

    actor = "document_chembl"
    id_column = "document_chembl_id"
    extract_event_name = "chembl_document.extract_mode"
    id_extraction_summary_event = LogEvents.CHEMBL_DOCUMENT_EXTRACT_BY_IDS_SUMMARY

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._last_batch_extract_stats: dict[str, Any] | None = None
        self.configure_output_schema(get_out_schema(self.pipeline_code))

    def descriptor_spec(
        self: SelfChemblDocumentPipeline,
    ) -> ChemblDescriptorSpec[SelfChemblDocumentPipeline]:
        """Return the declarative descriptor specification for documents."""

        def _require_document_pipeline(
            pipeline: ChemblPipelineBase,
        ) -> ChemblDocumentPipeline:
            if isinstance(pipeline, ChemblDocumentPipeline):
                return pipeline
            msg = "ChemblDocumentPipeline instance required"
            raise TypeError(msg)

        empty_frame = make_empty_frame_factory("document_chembl_id")

        def record_transform(
            pipeline: SelfChemblDocumentPipeline,
            payload: Mapping[str, Any],
            _: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            document_pipeline = _require_document_pipeline(pipeline)
            return document_pipeline._extract_nested_fields(dict(payload))

        def summary_extra(
            _: SelfChemblDocumentPipeline,
            df: pd.DataFrame,
            context: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            page_size = context.page_size or 0
            pages = 0
            if page_size > 0:
                total_rows = int(df.shape[0])
                pages = (total_rows + page_size - 1) // page_size
            return {"pages": pages}

        def select_fields_resolver(
            pipeline: SelfChemblDocumentPipeline,
            source_config: DocumentSourceConfig,
        ) -> Sequence[str] | None:
            return pipeline._resolve_select_fields(
                cast(SourceConfig[Any], source_config),
                default_fields=API_DOCUMENT_FIELDS,
            )

        def after_build(
            pipeline: SelfChemblDocumentPipeline,
            context: ChemblExtractionContext,
            source_config: DocumentSourceConfig,
            _: BoundLogger,
        ) -> ChemblExtractionContext:
            chembl_client = cast(ChemblClient, context.chembl_client)
            context.iterator = pipeline._build_document_client(
                chembl_client=chembl_client,
                source_config=source_config,
            )
            return context

        context_spec = ChemblContextSpec(
            entity_name="document",
            entity_client_type=ChemblDocumentClient,
            select_fields_resolver=select_fields_resolver,
            client_registry_name="chembl_document_client",
            page_size_resolver=lambda cfg: cfg.batch_size,
            after_build=after_build,
        )

        return ChemblDescriptorSpec(
            name="chembl_document",
            source_name="chembl",
            source_config_factory=DocumentSourceConfig.from_source_config,
            context=context_spec,
            id_column="document_chembl_id",
            summary_event="chembl_document.extract_summary",
            must_have_fields=DOCUMENT_MUST_HAVE_FIELDS,
            default_select_fields=API_DOCUMENT_FIELDS,
            record_transform=record_transform,
            sort_by=("document_chembl_id",),
            empty_frame_factory=empty_frame,
            summary_extra=summary_extra,
        )

    def _build_document_context(
        self,
        *,
        source_config: DocumentSourceConfig,
        log: BoundLogger,
    ) -> ChemblExtractionContext:
        def select_resolver(
            pipeline_obj: ChemblPipelineBase,
            cfg: SourceConfig[Any],
        ) -> Sequence[str] | None:
            return pipeline_obj._resolve_select_fields(
                cast(SourceConfig[Any], cast(Any, cfg)),
                default_fields=API_DOCUMENT_FIELDS,
            )

        context = build_standard_chembl_context(
            self,
            "document",
            source_config,
            log,
            select_fields_resolver=select_resolver,
            client_registry_name="chembl_document_client",
            page_size_resolver=lambda cfg: cfg.batch_size,
        )
        if context.chembl_client is None:
            raise RuntimeError("chembl_client is None in _build_document_context")
        context.iterator = self._build_document_client(
            chembl_client=context.chembl_client,
            source_config=source_config,
        )
        return context

    def id_extraction_stats_attribute(self) -> str | None:
        return "_last_batch_extract_stats"

    def id_extraction_fetcher_factory(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: DocumentSourceConfig,
    ) -> FetcherFactory:
        def fetcher_factory(
            extraction_context: ChemblExtractionContext,
            log: BoundLogger,  # noqa: ARG001
        ) -> FetcherCallable:
            chembl_client = cast(ChemblClient, extraction_context.chembl_client)
            document_client = cast(ChemblDocumentClient, extraction_context.iterator)

            def fetch_documents(
                batch_ids: Sequence[str],
                fetch_context: BatchExtractionContext,
            ) -> Iterable[Mapping[str, Any]]:
                if "original_paginate" not in fetch_context.extra:
                    original_paginate = chembl_client.paginate

                    def counted_paginate(*args: Any, **kwargs: Any) -> Any:
                        fetch_context.increment_api_calls()
                        return original_paginate(*args, **kwargs)

                    chembl_client.paginate = counted_paginate  # type: ignore[method-assign]
                    fetch_context.extra["original_paginate"] = original_paginate

                iterator = document_client.iterate_by_ids(
                    batch_ids,
                    select_fields=fetch_context.select_fields or None,
                )
                for item in iterator:
                    yield self._extract_nested_fields(dict(item))

            return fetch_documents

        return fetcher_factory

    def id_extraction_finalize_context_factory(
        self,
        descriptor: ChemblExtractionDescriptor[Any],
        typed_source_config: DocumentSourceConfig,
    ) -> FinalizeContextFactory:
        def finalize_context_factory(
            extraction_context: ChemblExtractionContext,
            log: BoundLogger,  # noqa: ARG001
        ) -> FinalizeContextCallable:
            chembl_client = cast(ChemblClient, extraction_context.chembl_client)

            def finalize_context(fetch_context: BatchExtractionContext) -> None:
                original = fetch_context.extra.pop("original_paginate", None)
                if original is not None:
                    chembl_client.paginate = original  # type: ignore[method-assign]

                api_calls_value = (
                    fetch_context.stats.api_calls
                    if fetch_context.stats.api_calls is not None
                    else 0
                )
                override = {
                    "batches": fetch_context.stats.batches,
                    "api_calls": api_calls_value,
                    "cache_hits": fetch_context.stats.cache_hits,
                }
                fetch_context.extra["stats_attribute_override"] = override

            return finalize_context

        return finalize_context_factory

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            self.logger_for(stage="transform").debug(LogEvents.TRANSFORM_EMPTY_DATAFRAME)
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        log = self.logger_for(stage="transform", component="document_enrich")
        working_df = self._normalize_numeric_fields(df, log)

        if self._should_enrich_document_terms():
            working_df = self._enrich_document_terms(working_df)

        working_df = self._add_system_fields(working_df, log)
        working_df = self._deduplicate_documents(working_df, log)
        return working_df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        log = self.logger_for(stage="transform", component="post_transform")
        return self._normalize_and_enforce_schema(
            df,
            self._output_column_order,
            log,
            normalize_identifiers=False,
            normalize_strings=False,
            order_columns=True,
            copy=False,
        )

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            self.logger_for(stage="validate").debug(LogEvents.VALIDATE_EMPTY_DATAFRAME)
            return df

        if self.config.validation.strict:
            allowed_columns = set(self._output_column_order)
            extra_columns = [column for column in df.columns if column not in allowed_columns]
            if extra_columns:
                self.logger_for(stage="validate", component="schema_prune").debug(
                    LogEvents.DROP_EXTRA_COLUMNS_BEFORE_VALIDATION,
                    extras=extra_columns,
                )
                df = df.drop(columns=extra_columns)

        self._check_document_id_uniqueness(
            df,
            self.logger_for(stage="validate", component="document_checks"),
        )
        return super().validate(df)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def preprocess_identifier_columns(self, df: pd.DataFrame, log: BoundLogger) -> pd.DataFrame:
        if "doi" in df.columns:
            df["doi_clean"] = df["doi"].apply(self._normalize_doi)  # pyright: ignore[reportUnknownMemberType]
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

    def string_rules(self) -> Mapping[str, StringRule]:
        return {
            "title": StringRule(max_length=1000),
            "abstract": StringRule(max_length=5000),
        }

    def postprocess_string_columns(
        self,
        df: pd.DataFrame,
        stats: StringStats,
        log: BoundLogger,
    ) -> pd.DataFrame:
        result = super().postprocess_string_columns(df, stats, log)

        if "journal" in result.columns:
            journal_series: pd.Series[Any] = result["journal"]
            result["journal"] = journal_series.map(
                lambda value: self._normalize_journal(value)
            )

        if "authors" in result.columns:

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

            authors_series: pd.Series[Any] = result["authors"]
            normalized_result = authors_series.apply(self._normalize_authors)
            normalized_tuples = normalized_result.apply(_to_author_tuple)
            result["authors"] = normalized_tuples.apply(_author_name_from_tuple)
            result["authors_count"] = normalized_tuples.apply(_author_count_from_tuple)

        return result

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

    def _deduplicate_documents(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        if df.empty or "document_chembl_id" not in df.columns:
            return df
        if not df["document_chembl_id"].duplicated().any():
            return df

        initial_count = len(df)
        deduped_df = df.sort_values(by=list(df.columns)).drop_duplicates(
            subset=["document_chembl_id"],
            keep="first",
        )
        deduped_count = len(deduped_df)
        if deduped_count < initial_count:
            log.warning(
                LogEvents.DOCUMENT_DEDUPLICATION_APPLIED,
                initial_count=initial_count,
                deduped_count=deduped_count,
                removed_count=initial_count - deduped_count,
            )
        return deduped_df

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
            log.warning(
                LogEvents.DOCUMENT_ID_DUPLICATES,
                duplicate_count=duplicates.sum(),
                duplicate_ids=duplicate_ids[:10],  # Limit to first 10
            )

    def _should_enrich_document_terms(self) -> bool:
        """Return True when document_term enrichment is enabled in the config."""
        chembl_config = self.config.chembl
        return enrich_flag(
            chembl_config,
            ("document", "enrich", "document_term", "enabled"),
        )

    def _enrich_document_terms(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply document_term enrichment to the DataFrame."""
        log = self.logger_for(stage="transform", component=f"{self.pipeline_code}.enrich")

        chembl_config = self.config.chembl
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
