"""Document Pipeline - ChEMBL document extraction with external enrichment."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from pandera.errors import SchemaErrors

from bioetl.config import PipelineConfig
from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import (
    EnrichmentStage,
    PipelineBase,
    enrichment_stage_registry,
)
from bioetl.sources.crossref.pipeline import CROSSREF_ADAPTER_DEFINITION
from bioetl.sources.document.merge.policy import merge_with_precedence
from bioetl.sources.document.pipeline import (
    AdapterDefinition,
    ExternalEnrichmentResult,
)
from bioetl.sources.openalex.pipeline import OPENALEX_ADAPTER_DEFINITION
from bioetl.sources.pubmed.pipeline import PUBMED_ADAPTER_DEFINITION
from bioetl.sources.semantic_scholar.pipeline import (
    SEMANTIC_SCHOLAR_ADAPTER_DEFINITION,
)
from bioetl.schemas.document import (
    DocumentNormalizedSchema,
    DocumentRawSchema,
    DocumentSchema,
)
from bioetl.schemas.registry import schema_registry
from bioetl.utils.dtypes import coerce_retry_after
from bioetl.utils.qc import compute_field_coverage, duplicate_summary

from .client import DocumentChEMBLClient, DocumentFetchCallbacks
from .merge import merge_enrichment_results
from .normalizer import normalize_document_frame
from .output import append_qc_sections, persist_rejected_inputs
from .parser import prepare_document_input_ids
from .request import (
    build_adapter_configs as request_build_adapter_configs,
    collect_enrichment_metrics,
    init_external_adapters,
    run_enrichment_requests,
)
from .schema import build_document_fallback_row

__all__ = ["DocumentPipeline"]

NAType = type(pd.NA)

logger = UnifiedLogger.get(__name__)


_ADAPTER_DEFINITIONS: dict[str, AdapterDefinition] = {
    "pubmed": PUBMED_ADAPTER_DEFINITION,
    "crossref": CROSSREF_ADAPTER_DEFINITION,
    "openalex": OPENALEX_ADAPTER_DEFINITION,
    "semantic_scholar": SEMANTIC_SCHOLAR_ADAPTER_DEFINITION,
}

# Register schema
schema_registry.register("document", "1.0.0", DocumentNormalizedSchema)  # type: ignore[arg-type]


class DocumentPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL document data.

    Modes:
    - chembl: ChEMBL only
    - all: ChEMBL + PubMed/Crossref/OpenAlex/Semantic Scholar
    """

    _INTEGER_COLUMNS: tuple[str, ...] = (
        "document_pubmed_id",
        "pmid",
        "year",
        "citation_count",
        "influential_citations",
        "chembl_pmid",
        "pubmed_pmid",
        "openalex_pmid",
        "semantic_scholar_pmid",
        "chembl_year",
        "openalex_year",
        "pubmed_year_completed",
        "pubmed_month_completed",
        "pubmed_day_completed",
        "pubmed_year_revised",
        "pubmed_month_revised",
        "pubmed_day_revised",
        "fallback_http_status",
        "fallback_attempt",
    )

    _BOOLEAN_COLUMNS: tuple[str, ...] = (
        "referenses_on_previous_experiments",
        "original_experimental_document",
        "is_oa",
        "conflict_doi",
        "conflict_pmid",
    )

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = DocumentNormalizedSchema

        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        default_batch_size = 10
        default_max_url_length = 1800
        self.max_batch_size = 25

        self.document_client = DocumentChEMBLClient(
            config,
            defaults={
                "enabled": True,
                "base_url": default_base_url,
                "batch_size": default_batch_size,
                "max_url_length": default_max_url_length,
            },
            batch_size_cap=self.max_batch_size,
        )

        self.api_client = self.document_client.api_client
        self.register_client(self.api_client)
        self.batch_size = self.document_client.batch_size
        self.max_url_length = max(1, int(self.document_client.max_url_length))

        # Initialize external adapters if enabled
        self.external_adapters: dict[str, Any] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()
        self.document_client.release = self._chembl_release

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract document data from input file with optional enrichment."""
        df, resolved_path = self.read_input_table(
            default_filename=Path("document.csv"),
            expected_columns=["document_chembl_id"],
            dtype="string",
            input_file=input_file,
            apply_limit=False,
        )

        if not resolved_path.exists():
            return df

        valid_ids, rejected_rows = self._prepare_input_ids(df)
        if rejected_rows:
            self._persist_rejected_inputs(rejected_rows)

        if not valid_ids:
            logger.warning("no_valid_ids", total=len(df))
            return pd.DataFrame(columns=["document_chembl_id"])

        logger.info("validated_ids", total=len(valid_ids))

        limit_value = self.get_runtime_limit()
        if limit_value is not None and len(valid_ids) > limit_value:
            logger.info(
                "applying_input_limit",
                limit=limit_value,
                original_ids=len(valid_ids),
            )
            valid_ids = valid_ids[:limit_value]

        records = self._fetch_documents(valid_ids)
        if not records:
            logger.warning("no_records_fetched", ids=len(valid_ids))
            return pd.DataFrame(columns=["document_chembl_id"])

        result_df = pd.DataFrame(records)
        result_df = result_df.convert_dtypes()

        validated = self._validate_raw_dataframe(result_df)
        logger.info("extraction_completed", rows=len(validated))
        return validated

    def _init_external_adapters(self) -> None:
        """Initialize external API adapters using structured definitions."""

        self.external_adapters = init_external_adapters(self.config, _ADAPTER_DEFINITIONS)

    def close_resources(self) -> None:
        """Close external adapters and the primary API client."""

        try:
            for name, adapter in getattr(self, "external_adapters", {}).items():
                self._close_resource(adapter, resource_name=f"external_adapter.{name}")
        finally:
            super().close_resources()

    def _build_adapter_configs(
        self,
        source_name: str,
        source_cfg: Any,
        definition: AdapterDefinition,
    ) -> tuple[Any, Any]:
        """Backward-compatible wrapper around the request helper."""

        return request_build_adapter_configs(self.config, source_name, source_cfg, definition)

    def _enrich_with_external_sources(
        self, chembl_df: pd.DataFrame
    ) -> ExternalEnrichmentResult:
        """Enrich ChEMBL data with external sources."""
        if not self.external_adapters:
            logger.info(
                "external_enrichment_skipped",
                reason="no_external_adapters",
                chembl_rows=len(chembl_df),
            )
            self.qc_enrichment_metrics = pd.DataFrame()
            return chembl_df

        pmids: list[str] = []
        dois: list[str] = []

        if "chembl_pmid" in chembl_df.columns:
            pmids = chembl_df["chembl_pmid"].dropna().astype(str).tolist()
        elif "pmid" in chembl_df.columns:
            pmids = chembl_df["pmid"].dropna().astype(str).tolist()
        elif "pubmed_id" in chembl_df.columns:
            pmids = chembl_df["pubmed_id"].dropna().astype(str).tolist()

        doi_columns = [col for col in ("doi", "chembl_doi") if col in chembl_df.columns]
        if doi_columns:
            seen_dois: set[str] = set()
            ordered_dois: list[str] = []
            for column in doi_columns:
                for value in chembl_df[column].dropna().tolist():
                    doi_str = str(value)
                    if doi_str and doi_str not in seen_dois:
                        seen_dois.add(doi_str)
                        ordered_dois.append(doi_str)
            dois = ordered_dois

        titles: list[str] = []
        if "_original_title" in chembl_df.columns:
            titles = chembl_df["_original_title"].dropna().astype(str).tolist()

        logger.info(
            "enrichment_data",
            pmids_count=len(pmids),
            dois_count=len(dois),
            doi_columns=doi_columns,
            sample_pmids=pmids[:3] if pmids else [],
        )

        pubmed_df, crossref_df, openalex_df, semantic_scholar_df, adapter_errors = (
            run_enrichment_requests(
                self.external_adapters,
                pmids=pmids,
                dois=dois,
                titles=titles,
            )
        )

        frames = {
            "pubmed": pubmed_df,
            "crossref": crossref_df,
            "openalex": openalex_df,
            "semantic_scholar": semantic_scholar_df,
        }
        self.qc_enrichment_metrics = collect_enrichment_metrics(frames, adapter_errors)

        enriched_df = merge_enrichment_results(
            chembl_df,
            pubmed_df=pubmed_df,
            crossref_df=crossref_df,
            openalex_df=openalex_df,
            semantic_scholar_df=semantic_scholar_df,
        )

        logger.info("after_merge", enriched_cols=len(enriched_df.columns), enriched_rows=len(enriched_df))

        if adapter_errors:
            logger.error("external_enrichment_failed", errors=adapter_errors)
            status = "failed"
        else:
            status = "completed"

        return ExternalEnrichmentResult(enriched_df, status, adapter_errors)

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _prepare_input_ids(self, df: pd.DataFrame) -> tuple[list[str], list[dict[str, str]]]:
        """Normalize and validate input identifiers using the parser helper."""

        return prepare_document_input_ids(df)

    def _persist_rejected_inputs(self, rows: list[dict[str, str]]) -> None:
        """Persist rejected inputs for auditability."""

        persist_rejected_inputs(
            rows,
            add_table=self.add_additional_table,
        )

    def _validate_raw_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate raw payloads against Pandera schema before transformation."""

        if df.empty:
            logger.info("raw_validation_skipped_empty", rows=0)
            return df

        working_df = df.copy()
        schema = DocumentRawSchema.to_schema()

        expected_order = list(schema.columns.keys())
        missing_columns = [column for column in expected_order if column not in working_df.columns]
        extra_columns = [column for column in working_df.columns if column not in expected_order]

        if missing_columns:
            for column in missing_columns:
                if column == "fallback_retry_after_sec":
                    working_df[column] = pd.Series(
                        pd.array([pd.NA] * len(working_df), dtype="Float64"), index=working_df.index
                    )
                else:
                    working_df[column] = pd.NA

        # Reindex the known columns explicitly to avoid Pandera's COLUMN_NOT_ORDERED
        # failures when upstream payloads shuffle column order. Using ``reindex`` with
        # the schema-driven order guarantees determinism while keeping optional fields
        # (like ``journal_abbrev``) present even when the input payload omits them.
        ordered_core = working_df.reindex(columns=expected_order)
        extras_df = working_df.loc[:, extra_columns] if extra_columns else None

        # Log the ordering context before recombining to simplify troubleshooting of
        # Pandera validation errors reported by end users.
        logger.debug(
            "raw_schema_ordering",
            expected=expected_order,
            current=list(df.columns),
            missing=missing_columns,
            extra=extra_columns,
        )

        try:
            core_df = ordered_core.copy()

            if "source" not in core_df.columns:
                core_df["source"] = "ChEMBL"
            else:
                core_df["source"] = core_df["source"].fillna("ChEMBL")

            core_df = core_df.convert_dtypes()
            validated_core = DocumentRawSchema.validate(core_df, lazy=True)

            if extras_df is not None and not extras_df.empty:
                extras_df = extras_df.convert_dtypes()
                validated = pd.concat([validated_core, extras_df], axis=1)
            else:
                validated = validated_core

            logger.info("raw_schema_validation_passed", rows=len(validated))
            return validated
        except SchemaErrors as exc:
            failure_cases = getattr(exc, "failure_cases", None)
            details = None
            if isinstance(failure_cases, pd.DataFrame):
                details = failure_cases.to_dict(orient="records")

            self.record_validation_issue(
                {
                    "metric": "raw_schema_validation",
                    "severity": "error",
                    "details": details,
                }
            )
            logger.error("raw_schema_validation_failed", error=str(exc))
            raise

    def _fetch_documents(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        """Fetch document payloads from ChEMBL with resilient batching."""

        callbacks = DocumentFetchCallbacks(
            classify_error=self._classify_error,
            create_fallback=lambda document_id, error_type, message, error: self._create_fallback_row(
                document_id,
                error_type,
                message,
                error,
            ),
        )
        return self.document_client.fetch_documents(ids, callbacks)

    @staticmethod
    def _normalise_chembl_release_value(value: Any) -> str | None:
        """Return a canonical ChEMBL release string or ``None`` for missing values."""

        if value is None:
            return None

        if value is pd.NA:  # type: ignore[comparison-overlap]
            return None

        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass

        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None

        if isinstance(value, Mapping):
            candidate = value.get("chembl_release") or value.get("version") or value.get("name")
            if isinstance(candidate, str):
                stripped_candidate = candidate.strip()
                return stripped_candidate or None
            return None

        normalised = str(value).strip()
        return normalised or None

    def _classify_error(self, exc: Exception) -> str:
        if isinstance(exc, requests.exceptions.ReadTimeout):
            return "E_TIMEOUT"
        if isinstance(exc, requests.exceptions.HTTPError):
            status = exc.response.status_code if exc.response is not None else None
            if status == 429:
                return "E_HTTP_429"
            if status and 500 <= status < 600:
                return "E_HTTP_5XX"
            if status and 400 <= status < 500:
                return "E_HTTP_4XX"
        if isinstance(exc, CircuitBreakerOpenError):
            return "E_CIRCUIT_BREAKER_OPEN"
        return "E_UNKNOWN"

    def _create_fallback_row(
        self,
        document_id: str,
        error_type: str,
        error_message: str,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        return build_document_fallback_row(
            document_id,
            error_type=error_type,
            error_message=error_message,
            chembl_release=self._chembl_release,
            error=error,
        )

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from status endpoint.

        Returns:
            Version string (e.g., 'ChEMBL_36') or None
        """
        release = self._fetch_chembl_release_info(self.api_client)
        return release.version

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform document data with multi-source merge."""
        if df.empty:
            expected_columns = DocumentSchema.get_column_order()

            if expected_columns:
                empty_df = pd.DataFrame(columns=expected_columns)
                logger.info(
                    "transform_returning_empty_schema",
                    columns=len(expected_columns),
                )
                return empty_df

            return df

        df = normalize_document_frame(df)

        # Keep pubmed_id and doi with original names for enrichment (chembl_pmid already created above)

        with_pubmed = bool(self.runtime_options.get("with_pubmed", True))
        self.runtime_options["with_pubmed"] = with_pubmed

        logger.info(
            "external_enrichment_configured",
            with_pubmed=with_pubmed,
            adapters=len(self.external_adapters),
        )

        self.reset_stage_context()
        df = self.execute_enrichment_stages(df)

        if "pubmed" not in self.stage_context:
            reason = "disabled" if not with_pubmed else "no_external_adapters_enabled"
            if self.external_adapters:
                reason = "stage_not_executed" if with_pubmed else reason
            logger.info("enrichment_skipped", reason=reason)
            df = merge_with_precedence(df)

        # Drop temporary join keys if they exist
        if "pubmed_id" in df.columns:
            df = df.drop(columns=["pubmed_id"])
        if "_original_title" in df.columns:
            df = df.drop(columns=["_original_title"])

        pipeline_version = self.config.pipeline.version
        default_source = "chembl"

        if "chembl_release" in df.columns:
            df["chembl_release"] = df["chembl_release"].apply(self._normalise_chembl_release_value)

        release_value = self._normalise_chembl_release_value(self._chembl_release)

        if "fallback_reason" in df.columns:
            fallback_mask = df["fallback_reason"].notna()
            if fallback_mask.any():
                df.loc[fallback_mask, "source_system"] = "DOCUMENT_FALLBACK"

        df = self.finalize_with_standard_metadata(
            df,
            business_key="document_chembl_id",
            sort_by=["document_chembl_id"],
            schema=DocumentSchema,
            default_source=default_source,
            chembl_release=release_value,
        )

        self.set_export_metadata_from_dataframe(
            df,
            pipeline_version=pipeline_version,
            source_system=default_source,
            chembl_release=release_value,
        )

        df = df.convert_dtypes()
        df = self._enforce_schema_dtypes(df)

        return df

    def _enforce_schema_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns to schema-compatible nullable dtypes."""

        for column in self._INTEGER_COLUMNS:
            if column in df.columns:
                numeric_series = pd.to_numeric(df[column], errors="coerce")
                df[column] = numeric_series.astype("Int64")

        for column in self._BOOLEAN_COLUMNS:
            if column in df.columns:
                df[column] = df[column].astype("boolean")

        coerce_retry_after(df)
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate document data against schema with strict validation."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            self.set_qc_metrics({})
            append_qc_sections(
                self.add_qc_summary_section,
                dataset_name="documents",
                row_count=0,
                duplicates=duplicate_summary(0, 0, field="document_chembl_id"),
            )
            self.refresh_validation_issue_summary()
            return df

        working_df = df.copy().convert_dtypes()
        initial_rows = int(len(working_df))

        canonical_order = DocumentSchema.get_column_order()
        if canonical_order:
            missing_columns = [column for column in canonical_order if column not in working_df.columns]
            for column in missing_columns:
                if column == "fallback_retry_after_sec":
                    working_df[column] = pd.Series(
                        pd.NA,
                        index=working_df.index,
                        dtype=pd.Float64Dtype(),
                    )
                else:
                    working_df[column] = pd.NA

            if "fallback_retry_after_sec" in working_df.columns:
                working_df = working_df.astype({"fallback_retry_after_sec": "Float64"})

            extra_columns = [column for column in working_df.columns if column not in canonical_order]
            if extra_columns:
                working_df = working_df.drop(columns=extra_columns)

            working_df = working_df.loc[:, canonical_order]

        # Ensure schema-driven dtypes are respected even for newly added columns.
        working_df = self._enforce_schema_dtypes(working_df)

        # Defensive: гарантируем float64 для retry-after перед Pandera-валидацией
        if "fallback_retry_after_sec" in working_df.columns:
            numeric_retry = pd.Series(
                pd.to_numeric(working_df["fallback_retry_after_sec"], errors="coerce"),
                index=working_df.index,
                dtype="Float64",
            )
            working_df.loc[:, "fallback_retry_after_sec"] = numeric_retry

        if "run_id" in working_df.columns:
            working_df.loc[:, "run_id"] = working_df["run_id"].fillna(self.run_id)
        else:
            working_df.loc[:, "run_id"] = self.run_id

        duplicate_count = (
            working_df["document_chembl_id"].duplicated().sum()
            if "document_chembl_id" in working_df.columns
            else 0
        )
        if duplicate_count > 0:
            logger.warning("duplicates_found", count=duplicate_count)
            working_df = working_df.drop_duplicates(subset=["document_chembl_id"], keep="first")
            self.record_validation_issue(
                {
                    "metric": "duplicates_removed",
                    "value": int(duplicate_count),
                    "severity": "warning",
                }
            )

        sources_to_validate = [
            ("pubmed", ["pubmed_doc_type"], ["pubmed_article_title", "pubmed_abstract"]),
            ("crossref", ["crossref_doc_type"], ["crossref_title", "crossref_authors"]),
            ("openalex", ["openalex_doc_type"], ["openalex_title", "openalex_authors"]),
            (
                "semantic_scholar",
                ["semantic_scholar_doc_type"],
                ["semantic_scholar_title", "semantic_scholar_abstract"],
            ),
        ]

        validation_errors: list[str] = []
        for source_name, required_fields, actual_field_names in sources_to_validate:
            has_source_data = any(
                column in working_df.columns and working_df[column].notna().any()
                for column in actual_field_names
            )

            if has_source_data:
                for required_field in required_fields:
                    if required_field not in working_df.columns or working_df[required_field].isna().all():
                        validation_errors.append(
                            f"{source_name}: {required_field} обязателен при наличии данных"
                        )

        if validation_errors:
            error_msg = "; ".join(validation_errors)
            self.record_validation_issue(
                {
                    "metric": "source_doc_type_missing",
                    "severity": "error",
                    "details": validation_errors,
                }
            )
            logger.error("validation_failed", errors=error_msg)
            raise ValueError(f"Валидация не прошла: {error_msg}")

        metrics: dict[str, float] = {}

        duplicates_payload: dict[str, Any] | None = None

        def _update_qc_summary(validated_df: pd.DataFrame) -> None:
            nonlocal metrics, duplicates_payload

            metrics = self._compute_qc_metrics(validated_df)
            self.set_qc_metrics(metrics)
            row_count = int(len(validated_df))
            duplicates_payload = duplicate_summary(
                initial_rows,
                int(duplicate_count),
                field="document_chembl_id",
            )
            append_qc_sections(
                self.add_qc_summary_section,
                dataset_name="documents",
                row_count=row_count,
                duplicates=duplicates_payload,
            )

        validated_df = self.run_schema_validation(
            working_df,
            DocumentNormalizedSchema,
            dataset_name="documents",
            severity="error",
            metric_name="normalized_schema_validation",
            success_callbacks=(_update_qc_summary,),
        )

        coverage_columns = {
            "doi": "doi_clean",
            "pmid": "pmid" if "pmid" in validated_df.columns else "pubmed_id",
            "title": "title",
            "journal": "journal",
            "authors": "authors",
        }
        coverage_stats = compute_field_coverage(validated_df, coverage_columns.values())
        coverage_payload = {
            key: coverage_stats.get(column, 0.0)
            for key, column in coverage_columns.items()
        }
        append_qc_sections(
            self.add_qc_summary_section,
            dataset_name="documents",
            row_count=int(len(validated_df)),
            duplicates=None,
            coverage=coverage_payload,
        )

        self._enforce_qc_thresholds(metrics)

        self.refresh_validation_issue_summary()

        logger.info("validation_completed", rows=len(validated_df), duplicates_removed=duplicate_count)
        return validated_df

    def _compute_qc_metrics(self, df: pd.DataFrame) -> dict[str, float]:
        """Compute coverage, conflict, and fallback quality metrics."""

        total = len(df)
        if total == 0:
            return {
                "doi_coverage": 0.0,
                "pmid_coverage": 0.0,
                "title_coverage": 0.0,
                "journal_coverage": 0.0,
                "authors_coverage": 0.0,
                "conflicts_doi": 0.0,
                "conflicts_pmid": 0.0,
                "title_fallback_rate": 0.0,
            }

        def _coverage(column: str) -> float:
            if column not in df.columns:
                return 0.0
            return float(df[column].notna().sum() / total)

        metrics: dict[str, float] = {
            "doi_coverage": _coverage("doi_clean"),
            "pmid_coverage": _coverage("pmid") if "pmid" in df.columns else _coverage("pubmed_id"),
            "title_coverage": _coverage("title"),
            "journal_coverage": _coverage("journal"),
            "authors_coverage": _coverage("authors"),
        }

        if "conflict_doi" in df.columns:
            conflict_doi = df["conflict_doi"].fillna(False).astype("boolean")
            metrics["conflicts_doi"] = float(conflict_doi.sum() / total)
        else:
            metrics["conflicts_doi"] = 0.0

        if "conflict_pmid" in df.columns:
            conflict_pmid = df["conflict_pmid"].fillna(False).astype("boolean")
            metrics["conflicts_pmid"] = float(conflict_pmid.sum() / total)
        else:
            metrics["conflicts_pmid"] = 0.0

        if "title_source" in df.columns:
            fallback_mask = (
                df["title_source"]
                .fillna("")
                .astype(str)
                .str.contains("fallback", case=False)
            )
            metrics["title_fallback_rate"] = float(fallback_mask.sum() / total)
        else:
            metrics["title_fallback_rate"] = 0.0

        logger.debug("qc_metrics_computed", metrics=metrics)
        return metrics

    def _enforce_qc_thresholds(self, metrics: dict[str, float]) -> None:
        """Validate QC metrics against configured thresholds."""

        thresholds = self.config.qc.thresholds or {}
        if not thresholds:
            return

        failing: list[str] = []
        for metric_name, config in thresholds.items():
            if not isinstance(config, dict):
                continue

            value = metrics.get(metric_name)
            if value is None:
                continue

            min_threshold = config.get("min")
            max_threshold = config.get("max")
            severity = str(config.get("severity", "warning"))

            passed = True
            if min_threshold is not None and value < float(min_threshold):
                passed = False
            if max_threshold is not None and value > float(max_threshold):
                passed = False

            issue_payload = {
                "metric": metric_name,
                "value": value,
                "threshold_min": float(min_threshold) if min_threshold is not None else None,
                "threshold_max": float(max_threshold) if max_threshold is not None else None,
                "severity": severity if not passed else "info",
                "passed": passed,
            }
            self.record_validation_issue(issue_payload)

            if not passed and self._should_fail(severity):
                failing.append(metric_name)

        if failing:
            logger.error("qc_threshold_exceeded", failing=failing)
            raise ValueError("QC thresholds exceeded for metrics: " + ", ".join(sorted(failing)))


def _document_should_run_pubmed(
    pipeline: PipelineBase, df: pd.DataFrame
) -> tuple[bool, str | None]:
    """Decide whether the document pipeline should execute the PubMed stage."""

    if not isinstance(pipeline, DocumentPipeline):
        return False, "unsupported_pipeline"
    if df.empty:
        return False, "empty_frame"

    with_pubmed = bool(pipeline.runtime_options.get("with_pubmed", True))
    pipeline.runtime_options["with_pubmed"] = with_pubmed
    if not with_pubmed:
        return False, "disabled"

    if not pipeline.external_adapters:
        return False, "no_external_adapters"

    return True, None


def _document_run_pubmed_stage(
    pipeline: PipelineBase, df: pd.DataFrame
) -> pd.DataFrame:
    """Run external enrichment for the document pipeline."""

    if not isinstance(pipeline, DocumentPipeline):  # pragma: no cover - defensive
        return df

    try:
        result = pipeline._enrich_with_external_sources(df)
    except Exception as exc:
        pipeline.record_validation_issue(
            {
                "metric": "enrichment.pubmed",
                "issue_type": "enrichment",
                "severity": "warning",
                "status": "failed",
                "error": str(exc),
            }
        )
        raise

    enriched_df = result.dataframe
    pipeline.stage_context["pubmed"] = {
        "executed": True,
        "errors": result.errors,
        "status": result.status,
    }

    if result.has_errors():
        pipeline.set_stage_summary(
            "pubmed",
            result.status,
            rows=int(len(enriched_df)),
            errors=result.errors,
            error_count=len(result.errors),
        )
        pipeline.record_validation_issue(
            {
                "metric": "enrichment.pubmed",
                "issue_type": "enrichment",
                "severity": "error",
                "status": "failed",
                "errors": result.errors,
                "error_count": len(result.errors),
            }
        )
    else:
        pipeline.set_stage_summary("pubmed", result.status, rows=int(len(enriched_df)))

    return enriched_df


def _register_document_enrichment_stages() -> None:
    """Register enrichment stages for the document pipeline."""

    enrichment_stage_registry.register(
        DocumentPipeline,
        EnrichmentStage(
            name="pubmed",
            include_if=_document_should_run_pubmed,
            handler=_document_run_pubmed_stage,
        ),
    )


_register_document_enrichment_stages()

