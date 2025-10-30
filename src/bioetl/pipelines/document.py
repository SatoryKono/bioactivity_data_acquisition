"""Document Pipeline - ChEMBL document extraction with external enrichment."""

import os
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
import requests
from pandera.errors import SchemaErrors

from bioetl.adapters import (
    CrossrefAdapter,
    OpenAlexAdapter,
    PubMedAdapter,
    SemanticScholarAdapter,
)
from bioetl.adapters.base import AdapterConfig
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, CircuitBreakerOpenError
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import (
    EnrichmentStage,
    PipelineBase,
    enrichment_stage_registry,
)
from bioetl.pipelines.document_enrichment import merge_with_precedence
from bioetl.schemas.document import (
    DocumentNormalizedSchema,
    DocumentRawSchema,
    DocumentSchema,
)
from bioetl.schemas.document_input import DocumentInputSchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.config import coerce_float_config, coerce_int_config
from bioetl.utils.dtypes import coerce_optional_bool, coerce_retry_after
from bioetl.utils.fallback import build_fallback_payload
from bioetl.utils.qc import compute_field_coverage, duplicate_summary

NAType = type(pd.NA)

logger = UnifiedLogger.get(__name__)


@dataclass
class ExternalEnrichmentResult:
    """Container describing the outcome of an external enrichment request."""

    dataframe: pd.DataFrame
    status: str
    errors: dict[str, str]

    def has_errors(self) -> bool:
        """Return True when at least one adapter reported an error."""

        return bool(self.errors)

# ---------------------------------------------------------------------------
# External adapter configuration profiles
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldSpec:
    """Specification describing how to resolve a configuration attribute."""

    default: Any | None = None
    default_factory: Callable[[], Any] | None = None
    env: str | None = None
    coalesce_default_on_blank: bool = False

    def get_default(self) -> Any:
        """Return a copy of the default value for this field."""

        if self.default_factory is not None:
            return self.default_factory()
        return deepcopy(self.default)


@dataclass(frozen=True)
class AdapterDefinition:
    """Definition for constructing external enrichment adapters."""

    adapter_cls: type[Any]
    api_fields: dict[str, FieldSpec]
    adapter_fields: dict[str, FieldSpec]


_ADAPTER_DEFINITIONS: dict[str, AdapterDefinition] = {
    "pubmed": AdapterDefinition(
        adapter_cls=PubMedAdapter,
        api_fields={
            "base_url": FieldSpec(default="https://eutils.ncbi.nlm.nih.gov/entrez/eutils"),
            "rate_limit_max_calls": FieldSpec(default=3),
            "rate_limit_period": FieldSpec(default=1.0),
            "rate_limit_jitter": FieldSpec(default=True),
            "headers": FieldSpec(default_factory=dict),
        },
        adapter_fields={
            "batch_size": FieldSpec(default=200),
            "workers": FieldSpec(default=1),
            "tool": FieldSpec(
                default="bioactivity_etl",
                env="PUBMED_TOOL",
                coalesce_default_on_blank=True,
            ),
            "email": FieldSpec(default="", env="PUBMED_EMAIL"),
            "api_key": FieldSpec(default="", env="PUBMED_API_KEY"),
        },
    ),
    "crossref": AdapterDefinition(
        adapter_cls=CrossrefAdapter,
        api_fields={
            "base_url": FieldSpec(default="https://api.crossref.org"),
            "rate_limit_max_calls": FieldSpec(default=2),
            "rate_limit_period": FieldSpec(default=1.0),
            "rate_limit_jitter": FieldSpec(default=True),
            "headers": FieldSpec(default_factory=dict),
        },
        adapter_fields={
            "batch_size": FieldSpec(default=100),
            "workers": FieldSpec(default=2),
            "mailto": FieldSpec(default="", env="CROSSREF_MAILTO"),
        },
    ),
    "openalex": AdapterDefinition(
        adapter_cls=OpenAlexAdapter,
        api_fields={
            "base_url": FieldSpec(default="https://api.openalex.org"),
            "rate_limit_max_calls": FieldSpec(default=10),
            "rate_limit_period": FieldSpec(default=1.0),
            "rate_limit_jitter": FieldSpec(default=True),
            "headers": FieldSpec(default_factory=dict),
        },
        adapter_fields={
            "batch_size": FieldSpec(default=100),
            "workers": FieldSpec(default=4),
        },
    ),
    "semantic_scholar": AdapterDefinition(
        adapter_cls=SemanticScholarAdapter,
        api_fields={
            "base_url": FieldSpec(default="https://api.semanticscholar.org/graph/v1"),
            "rate_limit_max_calls": FieldSpec(default=1),
            "rate_limit_period": FieldSpec(default=1.25),
            "rate_limit_jitter": FieldSpec(default=True),
            "headers": FieldSpec(default_factory=dict),
        },
        adapter_fields={
            "batch_size": FieldSpec(default=50),
            "workers": FieldSpec(default=1),
            "api_key": FieldSpec(default="", env="SEMANTIC_SCHOLAR_API_KEY"),
        },
    ),
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

        # Initialize ChEMBL API client
        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        default_batch_size = 10
        default_max_url_length = 1800
        self.max_batch_size = 25

        chembl_context = self._init_chembl_client(
            defaults={
                "enabled": True,
                "base_url": default_base_url,
                "batch_size": default_batch_size,
                "max_url_length": default_max_url_length,
            },
            batch_size_cap=self.max_batch_size,
        )

        self.api_client = chembl_context.client
        self.register_client(self.api_client)
        self.batch_size = chembl_context.batch_size
        resolved_max_url = chembl_context.max_url_length or default_max_url_length
        self.max_url_length = max(1, int(resolved_max_url))
        self._document_cache: dict[str, dict[str, Any]] = {}

        # Initialize external adapters if enabled
        self.external_adapters: dict[str, Any] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()

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

        self.external_adapters.clear()
        sources = self.config.sources

        for source_name, definition in _ADAPTER_DEFINITIONS.items():
            source_cfg = sources.get(source_name)
            if source_cfg is None:
                continue

            enabled = bool(self._get_source_attribute(source_cfg, "enabled", True))
            if not enabled:
                logger.info("adapter_skipped", source=source_name, reason="disabled")
                continue

            api_config, adapter_config = self._build_adapter_configs(
                source_name, source_cfg, definition
            )
            adapter = definition.adapter_cls(api_config, adapter_config)
            self.external_adapters[source_name] = adapter

        logger.info("adapters_initialized", count=len(self.external_adapters))

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
    ) -> tuple[APIConfig, AdapterConfig]:
        """Construct API and adapter configuration objects for a source."""

        def _log(event: str, **kwargs: Any) -> None:
            logger.warning(event, source=source_name, **kwargs)

        cache_enabled = bool(self.config.cache.enabled)
        cache_enabled_raw = self._get_source_attribute(source_cfg, "cache_enabled")
        if cache_enabled_raw is not None:
            coerced = coerce_optional_bool(cache_enabled_raw)
            if coerced is not pd.NA:
                cache_enabled = bool(coerced)

        cache_ttl_default = int(self.config.cache.ttl)
        cache_ttl_raw = self._get_source_attribute(source_cfg, "cache_ttl")
        cache_ttl = coerce_int_config(
            cache_ttl_raw,
            cache_ttl_default,
            field="cache_ttl",
            log=_log,
            invalid_event="adapter_config_invalid_int",
        )

        cache_maxsize_default = getattr(self.config.cache, "maxsize", None)
        if cache_maxsize_default is None:
            cache_maxsize_default = APIConfig.__dataclass_fields__["cache_maxsize"].default  # type: ignore[index]
        cache_maxsize_raw = self._get_source_attribute(source_cfg, "cache_maxsize")
        cache_maxsize = coerce_int_config(
            cache_maxsize_raw,
            int(cache_maxsize_default),
            field="cache_maxsize",
            log=_log,
            invalid_event="adapter_config_invalid_int",
        )

        http_profiles = getattr(self.config, "http", None)
        global_http = None
        if isinstance(http_profiles, Mapping):
            global_http = http_profiles.get("global")

        timeout_default: float | None = None
        connect_default: float | None = None
        read_default: float | None = None
        if global_http is not None:
            timeout_default = getattr(global_http, "timeout_sec", None)
            if timeout_default is not None:
                timeout_default = float(timeout_default)
            connect_default = getattr(global_http, "connect_timeout_sec", None)
            if connect_default is not None:
                connect_default = float(connect_default)
            read_default = getattr(global_http, "read_timeout_sec", None)
            if read_default is not None:
                read_default = float(read_default)

        if connect_default is None:
            connect_default = (
                timeout_default
                if timeout_default is not None
                else float(APIConfig.__dataclass_fields__["timeout_connect"].default)  # type: ignore[index]
            )

        if read_default is None:
            read_default = (
                timeout_default
                if timeout_default is not None
                else float(APIConfig.__dataclass_fields__["timeout_read"].default)  # type: ignore[index]
            )

        if timeout_default is None:
            timeout_default = read_default

        timeout_override = self._get_source_attribute(source_cfg, "timeout")
        if timeout_override is None:
            timeout_override = self._get_source_attribute(source_cfg, "timeout_sec")

        timeout_value = coerce_float_config(
            timeout_override,
            float(timeout_default),
            field="timeout",
            log=_log,
            invalid_event="adapter_config_invalid_float",
        )

        connect_override = self._get_source_attribute(source_cfg, "connect_timeout_sec")
        connect_default_final = (
            float(timeout_value) if timeout_override is not None else float(connect_default)
        )
        timeout_connect = coerce_float_config(
            connect_override,
            connect_default_final,
            field="connect_timeout_sec",
            log=_log,
            invalid_event="adapter_config_invalid_float",
        )

        read_override = self._get_source_attribute(source_cfg, "read_timeout_sec")
        read_default_final = (
            float(timeout_value) if timeout_override is not None else float(read_default)
        )
        timeout_read = coerce_float_config(
            read_override,
            read_default_final,
            field="read_timeout_sec",
            log=_log,
            invalid_event="adapter_config_invalid_float",
        )

        api_kwargs: dict[str, Any] = {
            "name": source_name,
            "cache_enabled": cache_enabled,
            "cache_ttl": cache_ttl,
            "cache_maxsize": cache_maxsize,
            "timeout_connect": timeout_connect,
            "timeout_read": timeout_read,
        }

        for field_name, spec in definition.api_fields.items():
            raw_value = self._get_source_attribute(source_cfg, field_name)
            value = self._resolve_field_value(raw_value, spec)
            api_kwargs[field_name] = value

        adapter_kwargs: dict[str, Any] = {"enabled": True}
        for field_name, spec in definition.adapter_fields.items():
            raw_value = self._get_source_attribute(source_cfg, field_name)
            value = self._resolve_field_value(raw_value, spec)
            adapter_kwargs[field_name] = value

        api_config = APIConfig(**api_kwargs)
        adapter_config = AdapterConfig(**adapter_kwargs)
        return api_config, adapter_config

    @staticmethod
    def _get_source_attribute(source_cfg: Any, attr: str, default: Any = None) -> Any:
        """Retrieve attribute from a TargetSourceConfig or mapping."""

        if isinstance(source_cfg, dict):
            return source_cfg.get(attr, default)
        return getattr(source_cfg, attr, default)

    def _resolve_field_value(self, raw_value: Any, spec: FieldSpec) -> Any:
        """Resolve value from configuration applying env substitutions and defaults."""

        default_value = spec.get_default()
        value = default_value if raw_value is None else raw_value
        value = self._apply_env_substitutions(value)

        if (value is None or (isinstance(value, str) and value == "")) and spec.env:
            env_value = os.getenv(spec.env)
            if env_value is not None:
                value = env_value

        if (
            isinstance(default_value, (int, float))
            and not isinstance(default_value, bool)
            and isinstance(value, str)
            and value
        ):
            try:
                value = type(default_value)(value)
            except ValueError:
                pass

        if spec.coalesce_default_on_blank and isinstance(value, str) and not value.strip():
            value = default_value

        if value is None:
            value = default_value

        return value

    def _apply_env_substitutions(self, value: Any) -> Any:
        """Recursively resolve environment placeholders in configuration values."""

        if isinstance(value, str):
            return self._resolve_env_reference(value)
        if isinstance(value, dict):
            return {key: self._apply_env_substitutions(val) for key, val in value.items()}
        if isinstance(value, list):
            return [self._apply_env_substitutions(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._apply_env_substitutions(item) for item in value)
        return value

    @staticmethod
    def _resolve_env_reference(value: str) -> str:
        """Resolve env-style placeholders like ``${VAR}`` or ``env:VAR``."""

        candidate = value.strip()
        if candidate.startswith("env:"):
            env_name = candidate.split(":", 1)[1]
            return os.getenv(env_name, "")
        if candidate.startswith("${") and candidate.endswith("}"):
            env_name = candidate[2:-1]
            return os.getenv(env_name, "")
        return value

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
            return chembl_df

        # Extract PMIDs and DOIs from ChEMBL data
        pmids = []
        dois = []

        # Check for multiple possible PMID column names
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
                    if not doi_str:
                        continue
                    if doi_str in seen_dois:
                        continue
                    seen_dois.add(doi_str)
                    ordered_dois.append(doi_str)
            dois = ordered_dois

        logger.info(
            "enrichment_data",
            pmids_count=len(pmids),
            dois_count=len(dois),
            doi_columns=doi_columns,
            sample_pmids=pmids[:3] if pmids else [],
        )

        # Fetch from external sources in parallel
        pubmed_df = None
        crossref_df = None
        openalex_df = None
        semantic_scholar_df = None
        adapter_errors: dict[str, str] = {}

        # Use ThreadPoolExecutor for parallel fetching
        workers = min(4, len(self.external_adapters))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}

            if "pubmed" in self.external_adapters and pmids:
                futures["pubmed"] = executor.submit(self.external_adapters["pubmed"].process, pmids)

            if "crossref" in self.external_adapters and dois:
                futures["crossref"] = executor.submit(self.external_adapters["crossref"].process, dois)

            if "openalex" in self.external_adapters and dois:
                futures["openalex"] = executor.submit(self.external_adapters["openalex"].process, dois)

            if "semantic_scholar" in self.external_adapters:
                if pmids:
                    # Primary: fetch by PMIDs
                    logger.info("semantic_scholar_by_pmids", count=len(pmids))
                    futures["semantic_scholar"] = executor.submit(self.external_adapters["semantic_scholar"].process, pmids)
                elif "_original_title" in chembl_df.columns:
                    # Fallback: fetch by titles if no PMIDs
                    titles = chembl_df["_original_title"].dropna().tolist()
                    logger.info("semantic_scholar_by_titles_fallback", count=len(titles))
                    # Call process_titles wrapper method
                    futures["semantic_scholar"] = executor.submit(
                        self.external_adapters["semantic_scholar"].process_titles, titles
                    )
                else:
                    logger.warning("semantic_scholar_skipped", reason="no_pmids_or_titles")

            # Collect results
            for source, future in futures.items():
                try:
                    result = future.result(timeout=300)  # 5 min timeout
                    if source == "pubmed":
                        pubmed_df = result
                    elif source == "crossref":
                        crossref_df = result
                    elif source == "openalex":
                        openalex_df = result
                    elif source == "semantic_scholar":
                        semantic_scholar_df = result
                    logger.info("adapter_completed", source=source, rows=len(result) if not result.empty else 0)
                    # DEBUG: Log returned columns
                    if not result.empty:
                        logger.info("adapter_columns", source=source, columns=list(result.columns))
                except Exception as e:
                    error_message = str(e) or e.__class__.__name__
                    adapter_errors[source] = error_message
                    logger.error("adapter_failed", source=source, error=error_message)

        # DEBUG: Log before merge
        logger.info("before_merge", chembl_cols=len(chembl_df.columns), chembl_rows=len(chembl_df))
        if pubmed_df is not None and not pubmed_df.empty:
            logger.info("pubmed_df_size", rows=len(pubmed_df), cols=len(pubmed_df.columns))
        if crossref_df is not None and not crossref_df.empty:
            logger.info("crossref_df_size", rows=len(crossref_df), cols=len(crossref_df.columns))
        if openalex_df is not None and not openalex_df.empty:
            logger.info("openalex_df_size", rows=len(openalex_df), cols=len(openalex_df.columns))
        if semantic_scholar_df is not None and not semantic_scholar_df.empty:
            logger.info("semantic_scholar_df_size", rows=len(semantic_scholar_df), cols=len(semantic_scholar_df.columns))

        # Merge with precedence
        enriched_df = merge_with_precedence(
            chembl_df, pubmed_df, crossref_df, openalex_df, semantic_scholar_df
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
        """Normalize and validate input identifiers using Pandera schema."""

        if "document_chembl_id" not in df.columns:
            raise ValueError("Input file must contain 'document_chembl_id' column")

        regex = re.compile(r"^CHEMBL\d+$")
        valid_ids: list[str] = []
        rejected: list[dict[str, str]] = []
        seen: set[str] = set()

        for raw_value in df["document_chembl_id"].tolist():
            normalized, reason = self._normalize_identifier(raw_value, regex)
            if reason:
                rejected.append(
                    {
                        "document_chembl_id": "" if raw_value is None else str(raw_value),
                        "reason": reason,
                    }
                )
                continue

            if normalized in seen:
                logger.debug("duplicate_id_skipped", document_chembl_id=normalized)
                continue

            if normalized is not None:
                seen.add(normalized)
                valid_ids.append(normalized)

        if valid_ids:
            DocumentInputSchema.validate(pd.DataFrame({"document_chembl_id": valid_ids}))

        return valid_ids, rejected

    def _normalize_identifier(
        self, value: Any, pattern: re.Pattern[str]
    ) -> tuple[str | None, str | None]:
        """Normalize identifier to uppercase CHEMBL format with validation reason."""

        if value is None or pd.isna(value):
            return None, "missing"

        text = str(value).strip().upper()
        if not text or text in {"#N/A", "N/A", "NONE", "NULL"}:
            return None, "missing"

        if not pattern.fullmatch(text):
            return None, "invalid_format"

        return text, None

    def _persist_rejected_inputs(self, rows: list[dict[str, str]]) -> None:
        """Persist rejected inputs for auditability."""

        rejected_df = pd.DataFrame(rows).convert_dtypes()
        relative_output = Path("qc") / "document_rejected_inputs.csv"
        logger.warning(
            "rejected_inputs_found",
            count=len(rejected_df),
            path=str(relative_output),
        )
        self.add_additional_table(
            "document_rejected_inputs",
            rejected_df,
            relative_path=relative_output,
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

        results: list[dict[str, Any]] = []
        for chunk in self._chunked(ids, max(1, self.batch_size)):
            cached_records, to_fetch = self._separate_cached(chunk)
            results.extend(cached_records)

            if not to_fetch:
                continue

            try:
                fetched = self._fetch_documents_recursive(to_fetch)
                for record in fetched:
                    document_id = record.get("document_chembl_id")
                    if document_id:
                        self._document_cache[self._document_cache_key(str(document_id))] = record
                results.extend(fetched)
            except Exception as exc:  # noqa: BLE001
                error_type = self._classify_error(exc)
                logger.error(
                    "document_fetch_failed",
                    chunk=list(to_fetch),
                    error=str(exc),
                    error_type=error_type,
                )
                for document_id in to_fetch:
                    fallback = self._create_fallback_row(document_id, error_type, str(exc), exc)
                    self._document_cache[self._document_cache_key(document_id)] = fallback
                    results.append(fallback)

        return results

    def _fetch_documents_recursive(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        """Recursively fetch documents handling URL length and timeouts."""

        if not ids:
            return []

        if len(ids) > self.max_batch_size:
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(ids[midpoint:])

        params = {"document_chembl_id__in": ",".join(ids)}
        full_url = self._build_full_url("/document.json", params)

        if len(full_url) > self.max_url_length and len(ids) > 1:
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(ids[midpoint:])

        try:
            response = self.api_client.request_json("/document.json", params=params)
        except requests.exceptions.ReadTimeout:
            if len(ids) <= 1:
                raise
            midpoint = max(1, len(ids) // 2)
            return self._fetch_documents_recursive(ids[:midpoint]) + self._fetch_documents_recursive(ids[midpoint:])

        documents = response.get("documents") or response.get("document") or []
        return [doc for doc in documents if isinstance(doc, dict)]

    def _build_full_url(self, endpoint: str, params: dict[str, Any]) -> str:
        base = self.api_client.config.base_url.rstrip("/")
        query = urlencode(params, doseq=False)
        return f"{base}{endpoint}?{query}" if query else f"{base}{endpoint}"

    def _chunked(self, items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
        for idx in range(0, len(items), size):
            yield items[idx : idx + size]

    def _separate_cached(self, ids: Sequence[str]) -> tuple[list[dict[str, Any]], list[str]]:
        cached: list[dict[str, Any]] = []
        missing: list[str] = []
        for document_id in ids:
            key = self._document_cache_key(document_id)
            if key in self._document_cache:
                cached.append(self._document_cache[key])
            else:
                missing.append(document_id)
        return cached, missing

    def _document_cache_key(self, document_id: str) -> str:
        release = self._chembl_release or "unknown"
        return f"document:{release}:{document_id}"

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
        return build_fallback_payload(
            entity="document",
            reason="exception",
            error=error,
            source="DOCUMENT_FALLBACK",
            message=error_message,
            context={
                "document_chembl_id": document_id,
                "chembl_release": self._chembl_release,
                "fallback_error_code": error_type,
            },
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

        # Map ChEMBL input fields according to schema
        # Map pubmed_id from CSV to document_pubmed_id
        if "pubmed_id" in df.columns:
            df["document_pubmed_id"] = df["pubmed_id"].apply(lambda x: int(x) if pd.notna(x) and str(x).isdigit() else None)

        # Map classification
        if "classification" in df.columns:
            df["document_classification"] = df["classification"]

        # Map is_experimental_doc to original_experimental_document (boolean)
        if "is_experimental_doc" in df.columns:
            df["original_experimental_document"] = df["is_experimental_doc"].apply(
                coerce_optional_bool
            )

        # Map document_contains_external_links to referenses_on_previous_experiments
        if "document_contains_external_links" in df.columns:
            df["referenses_on_previous_experiments"] = df[
                "document_contains_external_links"
            ].apply(coerce_optional_bool)

        # IMPORTANT: Map pubmed_id to chembl_pmid BEFORE normalization
        # Map pubmed_id -> chembl_pmid (convert to int)
        if "pubmed_id" in df.columns:
            df["chembl_pmid"] = df["pubmed_id"].apply(lambda x: int(x) if pd.notna(x) and str(x).isdigit() else None)

        # Normalize identifiers
        from bioetl.normalizers import registry

        # Normalize IDs before enrichment (now pubmed_id is safe to normalize)
        for col in ["document_chembl_id", "doi", "pmid", "pubmed_id"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Save original title for semantic scholar enrichment BEFORE renaming
        if "title" in df.columns:
            df["_original_title"] = df["title"]

        # Rename ChEMBL fields to multi-source naming convention BEFORE enrichment
        field_mapping = {
            "title": "chembl_title",
            "journal": "chembl_journal",
            "year": "chembl_year",
            "authors": "chembl_authors",
            "abstract": "chembl_abstract",
            "volume": "chembl_volume",
            "issue": "chembl_issue",
        }

        for old_col, new_col in field_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col].apply(
                    lambda x: registry.normalize("string", x) if pd.notna(x) else None
                )
                if old_col != new_col:
                    df = df.drop(columns=[old_col])

        # Add chembl_doi from doi (but keep doi for enrichment)
        if "doi" in df.columns:
            df["chembl_doi"] = df["doi"]

        # Set chembl_doc_type - default to journal-article for ChEMBL documents
        df["chembl_doc_type"] = "journal-article"

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

    # _coerce_optional_bool заменена на coerce_optional_bool из bioetl.utils.dtype

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
            self.add_qc_summary_section("row_counts", {"documents": 0})
            self.add_qc_summary_section("datasets", {"documents": {"rows": 0}})
            self.add_qc_summary_section(
                "duplicates",
                {"documents": duplicate_summary(0, 0, field="document_chembl_id")},
            )
            self.refresh_validation_issue_summary()
            return df

        working_df = df.copy().convert_dtypes()
        initial_rows = int(len(working_df))

        canonical_order = DocumentSchema.get_column_order()
        if canonical_order:
            missing_columns = [column for column in canonical_order if column not in working_df.columns]
            for column in missing_columns:
                working_df[column] = pd.NA

            extra_columns = [column for column in working_df.columns if column not in canonical_order]
            if extra_columns:
                working_df = working_df.drop(columns=extra_columns)

            working_df = working_df.loc[:, canonical_order]

        # Ensure schema-driven dtypes are respected even for newly added columns.
        working_df = self._enforce_schema_dtypes(working_df)

        # Defensive: гарантируем float64 для retry-after перед Pandera-валидацией
        if "fallback_retry_after_sec" in working_df.columns:
            numeric_retry = pd.to_numeric(working_df["fallback_retry_after_sec"], errors="coerce")
            working_df.loc[:, "fallback_retry_after_sec"] = pd.Series(
                pd.array(numeric_retry, dtype="Float64"), index=working_df.index
            )

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

        def _update_qc_summary(validated_df: pd.DataFrame) -> None:
            nonlocal metrics

            metrics = self._compute_qc_metrics(validated_df)
            self.set_qc_metrics(metrics)
            row_count = int(len(validated_df))
            self.add_qc_summary_section("row_counts", {"documents": row_count})
            self.add_qc_summary_section("datasets", {"documents": {"rows": row_count}})
            self.add_qc_summary_section(
                "duplicates",
                {
                    "documents": duplicate_summary(
                        initial_rows,
                        int(duplicate_count),
                        field="document_chembl_id",
                    )
                },
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
        self.add_qc_summary_section("coverage", {"documents": coverage_payload})
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

