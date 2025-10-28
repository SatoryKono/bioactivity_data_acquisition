"""Target Pipeline - ChEMBL target data extraction with multi-stage enrichment."""

from __future__ import annotations

import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.config.models import CircuitBreakerConfig, HttpConfig, RateLimitConfig, RetryConfig, TargetSourceConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import TargetSchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("target", "1.0.0", TargetSchema)


class TargetPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL target data with multi-stage enrichment.

    Stages:
    1. ChEMBL extraction (primary)
    2. UniProt enrichment (optional)
    3. IUPHAR enrichment (optional)
    4. Post-processing and materialization
    """

    _UNIPROT_FIELDS = (
        "accession,primaryAccession,secondaryAccession,protein_name,gene_primary,gene_synonym,"
        "organism_name,organism_id,lineage,sequence_length,cc_subcellular_location,cc_ptm,features"
    )
    _ORTHOLOG_FIELDS = "accession,organism_name,organism_id,protein_name"
    _UNIPROT_BATCH_SIZE = 50
    _IDMAPPING_BATCH_SIZE = 200
    _IDMAPPING_POLL_INTERVAL = 2.0
    _IDMAPPING_MAX_WAIT = 120.0
    _ORTHOLOG_PRIORITY = {
        "9606": 0,  # Homo sapiens
        "10090": 1,  # Mus musculus
        "10116": 2,  # Rattus norvegicus
    }

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        self.source_configs: dict[str, TargetSourceConfig] = {}
        self.api_clients: dict[str, UnifiedAPIClient] = {}

        for source_name, source in config.sources.items():
            source_config = source if isinstance(source, TargetSourceConfig) else TargetSourceConfig.model_validate(source)
            if not source_config.enabled:
                continue

            self.source_configs[source_name] = source_config
            api_client_config = self._build_api_client_config(source_name, source_config)
            self.api_clients[source_name] = UnifiedAPIClient(api_client_config)

        self.chembl_client = self.api_clients.get("chembl")
        self.uniprot_client = self.api_clients.get("uniprot")
        self.uniprot_idmapping_client = self.api_clients.get("uniprot_idmapping")
        self.uniprot_orthologs_client = self.api_clients.get("uniprot_orthologs")
        self.iuphar_client = self.api_clients.get("iuphar")

        # Backwards compatibility
        self.api_client = self.chembl_client

        chembl_source = self.source_configs.get("chembl")
        self.batch_size = chembl_source.batch_size if chembl_source and chembl_source.batch_size else 25

    def _build_api_client_config(
        self,
        source_name: str,
        source_config: TargetSourceConfig,
    ) -> APIConfig:
        """Create API client configuration for the given source."""

        http_profile = self._resolve_http_profile(source_name, source_config)
        global_http = self.config.http.get("global")

        timeout_sec = source_config.timeout_sec
        if timeout_sec is None and http_profile is not None:
            timeout_sec = http_profile.timeout_sec
        if timeout_sec is None and global_http is not None:
            timeout_sec = global_http.timeout_sec
        if timeout_sec is None:
            timeout_sec = 60.0

        connect_timeout = self._resolve_timeout(http_profile, global_http, "connect_timeout_sec", timeout_sec)
        read_timeout = self._resolve_timeout(http_profile, global_http, "read_timeout_sec", timeout_sec)

        retries = self._resolve_retries(http_profile, global_http)
        rate_limit = self._resolve_rate_limit(source_config, http_profile, global_http)
        rate_limit_jitter = self._resolve_rate_limit_jitter(http_profile, global_http)

        cache_enabled = source_config.cache_enabled if source_config.cache_enabled is not None else self.config.cache.enabled
        cache_ttl = source_config.cache_ttl if source_config.cache_ttl is not None else self.config.cache.ttl
        cache_maxsize = (
            source_config.cache_maxsize
            if source_config.cache_maxsize is not None
            else getattr(self.config.cache, "maxsize", 1024)
        )

        fallback_config = self.config.fallbacks
        fallback_enabled = fallback_config.enabled
        fallback_strategies = source_config.fallback_strategies or fallback_config.strategies
        partial_retry_max = (
            source_config.partial_retry_max
            if source_config.partial_retry_max is not None
            else fallback_config.partial_retry_max
        )
        circuit_breaker: CircuitBreakerConfig = (
            source_config.circuit_breaker or fallback_config.circuit_breaker
        )

        headers: dict[str, str] = {}
        if global_http:
            headers.update(global_http.headers)
        if http_profile:
            headers.update(http_profile.headers)
        headers.update(source_config.headers)

        return APIConfig(
            name=source_name,
            base_url=source_config.base_url,
            headers=headers,
            cache_enabled=cache_enabled,
            cache_ttl=cache_ttl,
            cache_maxsize=cache_maxsize,
            rate_limit_max_calls=rate_limit.max_calls,
            rate_limit_period=rate_limit.period,
            rate_limit_jitter=rate_limit_jitter,
            retry_total=retries.total,
            retry_backoff_factor=retries.backoff_multiplier,
            partial_retry_max=partial_retry_max,
            timeout_connect=connect_timeout,
            timeout_read=read_timeout,
            cb_failure_threshold=circuit_breaker.failure_threshold,
            cb_timeout=circuit_breaker.timeout_sec,
            fallback_enabled=fallback_enabled,
            fallback_strategies=fallback_strategies,
        )

    def _resolve_http_profile(
        self,
        source_name: str,
        source_config: TargetSourceConfig,
    ) -> HttpConfig | None:
        """Resolve HTTP profile configuration for a source."""

        profile_name = source_config.http_profile or source_name
        if profile_name and profile_name in self.config.http:
            return self.config.http[profile_name]
        return None

    def _resolve_timeout(
        self,
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
        attr: str,
        default: float,
    ) -> float:
        """Resolve timeout value using profile, global configuration, or default."""

        profile_value = getattr(http_profile, attr) if http_profile else None
        if profile_value is not None:
            return profile_value

        global_value = getattr(global_http, attr) if global_http else None
        if global_value is not None:
            return global_value

        return default

    def _resolve_retries(
        self,
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
    ) -> RetryConfig:
        """Resolve retry configuration with sensible defaults."""

        if http_profile is not None:
            return http_profile.retries
        if global_http is not None:
            return global_http.retries
        raise ValueError("Retry configuration is required for API clients")

    def _resolve_rate_limit(
        self,
        source_config: TargetSourceConfig,
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
    ) -> RateLimitConfig:
        """Resolve rate limit configuration for a source."""

        if source_config.rate_limit is not None:
            return source_config.rate_limit
        if http_profile is not None:
            return http_profile.rate_limit
        if global_http is not None:
            return global_http.rate_limit
        raise ValueError("Rate limit configuration is required for API clients")

    def _resolve_rate_limit_jitter(
        self,
        http_profile: HttpConfig | None,
        global_http: HttpConfig | None,
    ) -> bool:
        """Resolve rate limit jitter flag."""

        if http_profile is not None:
            return http_profile.rate_limit_jitter
        if global_http is not None:
            return global_http.rate_limit_jitter
        return True

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract target data from input file."""
        if input_file is None:
            # Default to data/input/target.csv
            input_file = Path("data/input/target.csv")

        logger.info("reading_input", path=input_file)

        # Read input file with target IDs
        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            # Return empty DataFrame with schema structure
            return pd.DataFrame(columns=[
                "target_chembl_id", "pref_name", "target_type", "organism",
                "taxonomy", "hgnc_id", "uniprot_accession",
                "iuphar_type", "iuphar_class", "iuphar_subclass",
            ])

        df = pd.read_csv(input_file)  # Read all records

        logger.info("extraction_completed", rows=len(df))
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform target data."""
        if df.empty:
            return df

        # Normalize identifiers
        from bioetl.normalizers import registry

        for col in ["target_chembl_id", "hgnc_id", "uniprot_accession"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Normalize names
        if "pref_name" in df.columns:
            df["pref_name"] = df["pref_name"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        enrichment_metrics: dict[str, Any] = {}
        uniprot_silver = pd.DataFrame()
        component_enrichment = pd.DataFrame()

        if (
            self.uniprot_client is not None
            and "uniprot_accession" in df.columns
            and df["uniprot_accession"].notna().any()
        ):
            try:
                df, uniprot_silver, component_enrichment, enrichment_metrics = self._enrich_uniprot(df)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("uniprot_enrichment_failed", error=str(exc))
                self.record_validation_issue(
                    {
                        "metric": "enrichment.uniprot",
                        "issue_type": "enrichment",
                        "severity": "warning",
                        "status": "failed",
                        "error": str(exc),
                    }
                )
            else:
                if enrichment_metrics:
                    self.qc_metrics.update(enrichment_metrics)
                if not uniprot_silver.empty or not component_enrichment.empty:
                    self._materialize_silver(uniprot_silver, component_enrichment)
        else:
            logger.info(
                "uniprot_enrichment_skipped",
                reason="no_accessions" if "uniprot_accession" not in df.columns else "no_client",
            )

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = None
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate target data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        try:
            # Check for duplicates
            duplicate_count = df["target_chembl_id"].duplicated().sum() if "target_chembl_id" in df.columns else 0
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                # Remove duplicates, keeping first occurrence
                df = df.drop_duplicates(subset=["target_chembl_id"], keep="first")

            logger.info("validation_completed", rows=len(df), duplicates_removed=duplicate_count)
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

    @staticmethod
    def _chunked(values: Iterable[str], size: int) -> Iterator[list[str]]:
        """Yield chunks of values honoring API limits."""

        batch: list[str] = []
        for value in values:
            batch.append(value)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _fetch_uniprot_entries(self, accessions: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Fetch UniProt entries for the provided accessions using the configured client."""

        if self.uniprot_client is None:
            return {}

        entries: dict[str, dict[str, Any]] = {}
        for chunk in self._chunked(accessions, self._UNIPROT_BATCH_SIZE):
            query_terms = [f"(accession:{acc})" for acc in chunk]
            params = {
                "query": " OR ".join(query_terms),
                "fields": self._UNIPROT_FIELDS,
                "format": "json",
                "size": max(len(chunk), 25),
            }

            try:
                payload = self.uniprot_client.request_json("/search", params=params)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_search_failed", error=str(exc), query=params["query"])
                continue

            results = payload.get("results") or payload.get("entries") or []
            for result in results:
                primary = result.get("primaryAccession") or result.get("accession")
                if not primary:
                    continue
                entries[primary] = result

        return entries

    def _enrich_uniprot(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich the target dataframe using UniProt entries and compute QC metrics."""

        working_df = df.reset_index(drop=True).copy()
        working_df = working_df.convert_dtypes()

        accessions_series = working_df.get("uniprot_accession")
        if accessions_series is None:
            return working_df, pd.DataFrame(), pd.DataFrame(), {}

        accessions = {
            str(value).strip()
            for value in accessions_series.dropna().tolist()
            if str(value).strip()
        }

        if not accessions:
            logger.info("uniprot_enrichment_skipped", reason="empty_accessions")
            return working_df, pd.DataFrame(), pd.DataFrame(), {}

        logger.info("uniprot_enrichment_started", accession_count=len(accessions))

        entries = self._fetch_uniprot_entries(accessions)
        secondary_index: dict[str, dict[str, Any]] = {}
        for entry in entries.values():
            for field in ("secondaryAccession", "secondaryAccessions"):
                secondary_values = entry.get(field, [])
                if isinstance(secondary_values, str):
                    secondary_values = [secondary_values]
                for secondary in secondary_values or []:
                    secondary_index[str(secondary)] = entry

        used_entries: dict[str, dict[str, Any]] = {}
        unresolved: dict[int, str] = {}
        metrics: dict[str, Any] = {}
        fallback_counts: dict[str, int] = defaultdict(int)
        resolved_rows = 0
        total_with_accession = 0

        for idx, row in working_df.iterrows():
            accession = row.get("uniprot_accession")
            if pd.isna(accession) or not str(accession).strip():
                continue

            accession_str = str(accession).strip()
            total_with_accession += 1
            entry = entries.get(accession_str)
            merge_strategy = "direct"

            if entry is None:
                entry = secondary_index.get(accession_str)
                if entry is not None:
                    merge_strategy = "secondary_accession"
                    fallback_counts["secondary_accession"] += 1

            if entry is None:
                unresolved[idx] = accession_str
                continue

            resolved_rows += 1
            canonical = entry.get("primaryAccession") or entry.get("accession")
            used_entries[canonical] = entry
            self._apply_entry_enrichment(working_df, idx, entry, merge_strategy)

        # Attempt ID mapping for unresolved accessions
        if unresolved:
            mapping_df = self._run_id_mapping(unresolved.values())
            if not mapping_df.empty:
                for idx, submitted in list(unresolved.items()):
                    mapped_rows = mapping_df[mapping_df["submitted_id"] == submitted]
                    if mapped_rows.empty:
                        continue

                    mapped_row = mapped_rows.iloc[0]
                    canonical_acc = mapped_row.get("canonical_accession")
                    if not canonical_acc:
                        continue

                    entry = entries.get(canonical_acc)
                    if entry is None:
                        if canonical_acc not in used_entries:
                            extra_entries = self._fetch_uniprot_entries([canonical_acc])
                            if extra_entries:
                                entries.update(extra_entries)
                                entry = extra_entries.get(canonical_acc)
                    if entry is None:
                        continue

                    fallback_counts["idmapping"] += 1
                    used_entries[canonical_acc] = entry
                    self._apply_entry_enrichment(working_df, idx, entry, "idmapping")
                    resolved_rows += 1
                    unresolved.pop(idx, None)

        # Gene symbol fallback
        if unresolved:
            gene_symbol_cache: dict[str, dict[str, Any] | None] = {}
            for idx, accession in list(unresolved.items()):
                gene_symbol = working_df.at[idx, "gene_symbol"] if "gene_symbol" in working_df.columns else None
                organism = working_df.at[idx, "organism"] if "organism" in working_df.columns else None
                gene_key = f"{gene_symbol}|{organism}" if gene_symbol else None
                if not gene_symbol:
                    continue

                if gene_key not in gene_symbol_cache:
                    gene_symbol_cache[gene_key] = self._search_uniprot_by_gene(str(gene_symbol), organism)

                entry = gene_symbol_cache[gene_key]
                if entry is None:
                    continue

                canonical_acc = entry.get("primaryAccession") or entry.get("accession")
                if not canonical_acc:
                    continue

                fallback_counts["gene_symbol"] += 1
                used_entries[canonical_acc] = entry
                self._apply_entry_enrichment(working_df, idx, entry, "gene_symbol")
                resolved_rows += 1
                unresolved.pop(idx, None)

        # Ortholog fallback for remaining unresolved rows
        if unresolved:
            for idx, accession in list(unresolved.items()):
                canonical = working_df.at[idx, "uniprot_accession"]
                taxonomy_id = None
                if "taxonomy_id" in working_df.columns:
                    taxonomy_id = working_df.at[idx, "taxonomy_id"]
                orthologs = self._fetch_orthologs(str(canonical), taxonomy_id)
                if orthologs.empty:
                    continue

                orthologs = orthologs.sort_values(by="priority").reset_index(drop=True)
                best = orthologs.iloc[0]
                ortholog_acc = best.get("ortholog_accession")
                if not ortholog_acc:
                    continue

                entry = entries.get(ortholog_acc)
                if entry is None:
                    extra_entries = self._fetch_uniprot_entries([ortholog_acc])
                    if extra_entries:
                        entries.update(extra_entries)
                        entry = extra_entries.get(ortholog_acc)

                if entry is None:
                    continue

                fallback_counts["ortholog"] += 1
                used_entries[ortholog_acc] = entry
                self._apply_entry_enrichment(working_df, idx, entry, "ortholog")
                working_df.at[idx, "ortholog_source"] = best.get("organism")
                resolved_rows += 1
                unresolved.pop(idx, None)

        if unresolved:
            for idx, accession in unresolved.items():
                logger.warning("uniprot_enrichment_unresolved", accession=accession)

        coverage = resolved_rows / total_with_accession if total_with_accession else 0.0
        metrics["enrichment_success.uniprot"] = coverage
        metrics["enrichment.total_with_accession"] = total_with_accession
        metrics["enrichment.resolved"] = resolved_rows

        for fallback_name, count in fallback_counts.items():
            metrics[f"fallback.{fallback_name}.count"] = count
            if total_with_accession:
                metrics[f"fallback.{fallback_name}.rate"] = count / total_with_accession

        logger.info(
            "uniprot_enrichment_completed",
            resolved=resolved_rows,
            total=total_with_accession,
            coverage=coverage,
            fallback_counts=dict(fallback_counts),
        )

        uniprot_silver_records: list[dict[str, Any]] = []
        component_records: list[dict[str, Any]] = []
        for canonical, entry in used_entries.items():
            record = {
                "canonical_accession": canonical,
                "protein_name": self._extract_protein_name(entry),
                "gene_primary": self._extract_gene_primary(entry),
                "gene_synonyms": "; ".join(self._extract_gene_synonyms(entry)),
                "organism_name": self._extract_organism(entry),
                "organism_id": self._extract_taxonomy_id(entry),
                "lineage": "; ".join(self._extract_lineage(entry)),
                "sequence_length": self._extract_sequence_length(entry),
                "secondary_accessions": "; ".join(self._extract_secondary(entry)),
            }
            uniprot_silver_records.append(record)

            isoform_df = self._expand_isoforms(entry)
            if not isoform_df.empty:
                component_records.extend(isoform_df.to_dict("records"))

        uniprot_silver_df = pd.DataFrame(uniprot_silver_records).convert_dtypes()
        component_enrichment_df = pd.DataFrame(component_records).convert_dtypes()

        return working_df, uniprot_silver_df, component_enrichment_df, metrics

    def _apply_entry_enrichment(
        self,
        df: pd.DataFrame,
        idx: int,
        entry: dict[str, Any],
        strategy: str,
    ) -> None:
        """Apply UniProt entry information to the target dataframe row."""

        canonical = entry.get("primaryAccession") or entry.get("accession")
        df.at[idx, "uniprot_canonical_accession"] = canonical
        df.at[idx, "uniprot_merge_strategy"] = strategy
        df.at[idx, "uniprot_gene_primary"] = self._extract_gene_primary(entry)
        df.at[idx, "uniprot_gene_synonyms"] = "; ".join(self._extract_gene_synonyms(entry))
        df.at[idx, "uniprot_protein_name"] = self._extract_protein_name(entry)
        df.at[idx, "uniprot_sequence_length"] = self._extract_sequence_length(entry)
        df.at[idx, "uniprot_taxonomy_id"] = self._extract_taxonomy_id(entry)
        df.at[idx, "uniprot_taxonomy_name"] = self._extract_organism(entry)
        df.at[idx, "uniprot_lineage"] = "; ".join(self._extract_lineage(entry))
        df.at[idx, "uniprot_secondary_accessions"] = "; ".join(self._extract_secondary(entry))

    def _run_id_mapping(self, accessions: Iterable[str]) -> pd.DataFrame:
        """Resolve obsolete accessions via UniProt ID mapping service."""

        if self.uniprot_idmapping_client is None:
            return pd.DataFrame(columns=["submitted_id", "canonical_accession"])

        all_rows: list[dict[str, Any]] = []
        for chunk in self._chunked(accessions, self._IDMAPPING_BATCH_SIZE):
            payload = {
                "from": "UniProtKB_AC-ID",
                "to": "UniProtKB",
                "ids": ",".join(chunk),
            }

            try:
                response = self.uniprot_idmapping_client.request_json("/run", method="POST", data=payload)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_submission_failed", error=str(exc))
                continue

            job_id = response.get("jobId") or response.get("job_id")
            if not job_id:
                logger.warning("uniprot_idmapping_no_job", payload=payload)
                continue

            job_result = self._poll_id_mapping(job_id)
            if not job_result:
                continue

            results = job_result.get("results") or job_result.get("mappedTo") or []
            for item in results:
                submitted = item.get("from") or item.get("accession") or item.get("input")
                to_entry = item.get("to") or item.get("mappedTo") or {}
                if isinstance(to_entry, dict):
                    canonical = to_entry.get("primaryAccession") or to_entry.get("accession")
                    isoform = to_entry.get("isoformAccession") or to_entry.get("isoform")
                else:
                    canonical = to_entry
                    isoform = None

                all_rows.append(
                    {
                        "submitted_id": submitted,
                        "canonical_accession": canonical,
                        "isoform_accession": isoform,
                    }
                )

        return pd.DataFrame(all_rows).convert_dtypes()

    def _poll_id_mapping(self, job_id: str) -> dict[str, Any] | None:
        """Poll UniProt ID mapping job until completion respecting rate limits."""

        if self.uniprot_idmapping_client is None:
            return None

        elapsed = 0.0
        while elapsed < self._IDMAPPING_MAX_WAIT:
            try:
                status = self.uniprot_idmapping_client.request_json(f"/status/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_status_failed", job_id=job_id, error=str(exc))
                return None

            job_status = status.get("jobStatus") or status.get("status")
            if job_status in {"RUNNING", "PENDING"}:
                time.sleep(self._IDMAPPING_POLL_INTERVAL)
                elapsed += self._IDMAPPING_POLL_INTERVAL
                continue

            if job_status not in {"FINISHED", "finished", "SUCCESS"}:
                logger.warning("uniprot_idmapping_failed", job_id=job_id, status=job_status)
                return None

            try:
                return self.uniprot_idmapping_client.request_json(f"/results/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_results_failed", job_id=job_id, error=str(exc))
                return None

        logger.warning("uniprot_idmapping_timeout", job_id=job_id)
        return None

    def _expand_isoforms(self, entry: dict[str, Any]) -> pd.DataFrame:
        """Expand isoform details from a UniProt entry."""

        canonical = entry.get("primaryAccession") or entry.get("accession")
        sequence_length = self._extract_sequence_length(entry)
        records: list[dict[str, Any]] = [
            {
                "canonical_accession": canonical,
                "isoform_accession": canonical,
                "isoform_name": self._extract_protein_name(entry),
                "is_canonical": True,
                "sequence_length": sequence_length,
                "source": "canonical",
            }
        ]

        comments = entry.get("comments", []) or []
        for comment in comments:
            if comment.get("commentType") != "ALTERNATIVE PRODUCTS":
                continue
            for isoform in comment.get("isoforms", []) or []:
                isoform_ids = isoform.get("isoformIds") or isoform.get("isoformId")
                if isinstance(isoform_ids, list) and isoform_ids:
                    isoform_acc = isoform_ids[0]
                else:
                    isoform_acc = isoform_ids
                if not isoform_acc:
                    continue

                names = isoform.get("names") or []
                if isinstance(names, list):
                    iso_name = next((n.get("value") for n in names if isinstance(n, dict) and n.get("value")), None)
                elif isinstance(names, dict):
                    iso_name = names.get("value")
                else:
                    iso_name = None

                length = isoform.get("sequence", {}).get("length") if isinstance(isoform.get("sequence"), dict) else None
                records.append(
                    {
                        "canonical_accession": canonical,
                        "isoform_accession": isoform_acc,
                        "isoform_name": iso_name,
                        "is_canonical": False,
                        "sequence_length": length,
                        "source": "isoform",
                    }
                )

        return pd.DataFrame(records).convert_dtypes()

    def _fetch_orthologs(self, accession: str, taxonomy_id: Any | None = None) -> pd.DataFrame:
        """Fetch ortholog accessions using the dedicated UniProt client."""

        client = self.uniprot_orthologs_client or self.uniprot_client
        if client is None:
            return pd.DataFrame(columns=["source_accession", "ortholog_accession", "organism_id", "organism", "priority"])

        params = {
            "query": f"relationship_type:ortholog AND (accession:{accession} OR xref:{accession})",
            "fields": self._ORTHOLOG_FIELDS,
            "format": "json",
            "size": 200,
        }

        try:
            payload = client.request_json("/", params=params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("ortholog_fetch_failed", accession=accession, error=str(exc))
            return pd.DataFrame(columns=["source_accession", "ortholog_accession", "organism_id", "organism", "priority"])

        results = payload.get("results") or payload.get("entries") or []
        records: list[dict[str, Any]] = []
        for item in results:
            ortholog_acc = item.get("primaryAccession") or item.get("accession")
            organism = item.get("organism", {}) if isinstance(item.get("organism"), dict) else item.get("organism")
            if isinstance(organism, dict):
                organism_name = organism.get("scientificName") or organism.get("commonName")
                organism_id = organism.get("taxonId") or organism.get("taxonIdentifier")
            else:
                organism_name = organism
                organism_id = item.get("organismId")

            organism_id_str = str(organism_id) if organism_id is not None else None
            priority = self._ORTHOLOG_PRIORITY.get(organism_id_str or "", 99)
            records.append(
                {
                    "source_accession": accession,
                    "ortholog_accession": ortholog_acc,
                    "organism": organism_name,
                    "organism_id": organism_id,
                    "priority": priority,
                }
            )

        if taxonomy_id is not None:
            try:
                taxonomy_str = str(int(taxonomy_id))
            except (TypeError, ValueError):
                taxonomy_str = None
            if taxonomy_str:
                for record in records:
                    if str(record.get("organism_id")) == taxonomy_str:
                        record["priority"] = min(record.get("priority", 99), -1)

        return pd.DataFrame(records).convert_dtypes()

    def _search_uniprot_by_gene(self, gene_symbol: str, organism: Any | None = None) -> dict[str, Any] | None:
        """Search UniProt entries by gene symbol as a fallback strategy."""

        if self.uniprot_client is None:
            return None

        query = f"gene_exact:{gene_symbol}"
        if organism:
            query = f"{query} AND organism_name:\"{organism}\""

        params = {
            "query": query,
            "fields": self._UNIPROT_FIELDS,
            "format": "json",
            "size": 1,
        }

        try:
            payload = self.uniprot_client.request_json("/search", params=params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("uniprot_gene_search_failed", gene=gene_symbol, error=str(exc))
            return None

        results = payload.get("results") or payload.get("entries") or []
        if not results:
            return None
        return results[0]

    def _extract_gene_primary(self, entry: dict[str, Any]) -> str | None:
        genes = entry.get("genes") or []
        if isinstance(genes, list):
            for gene in genes:
                primary = gene.get("geneName") if isinstance(gene, dict) else None
                if isinstance(primary, dict):
                    return primary.get("value")
                if isinstance(primary, str):
                    return primary
        return entry.get("gene_primary") or entry.get("genePrimary")

    def _extract_gene_synonyms(self, entry: dict[str, Any]) -> list[str]:
        synonyms: list[str] = []
        genes = entry.get("genes") or []
        if isinstance(genes, list):
            for gene in genes:
                syn_list = gene.get("synonyms") if isinstance(gene, dict) else None
                if isinstance(syn_list, list):
                    for synonym in syn_list:
                        if isinstance(synonym, dict) and synonym.get("value"):
                            synonyms.append(synonym["value"])
                        elif isinstance(synonym, str):
                            synonyms.append(synonym)
        extra = entry.get("gene_synonym") or entry.get("geneSynonym")
        if isinstance(extra, list):
            synonyms.extend(str(v) for v in extra if v)
        elif isinstance(extra, str):
            synonyms.append(extra)
        return sorted({syn.strip() for syn in synonyms if syn})

    def _extract_secondary(self, entry: dict[str, Any]) -> list[str]:
        values = entry.get("secondaryAccession") or entry.get("secondaryAccessions") or []
        if isinstance(values, str):
            values = [values]
        return [str(value).strip() for value in values if value]

    def _extract_protein_name(self, entry: dict[str, Any]) -> str | None:
        protein = entry.get("proteinDescription") or entry.get("protein")
        if isinstance(protein, dict):
            recommended = protein.get("recommendedName")
            if isinstance(recommended, dict):
                full_name = recommended.get("fullName")
                if isinstance(full_name, dict):
                    return full_name.get("value")
                if isinstance(full_name, str):
                    return full_name
        if isinstance(protein, str):
            return protein
        return entry.get("protein_name")

    def _extract_sequence_length(self, entry: dict[str, Any]) -> int | None:
        sequence = entry.get("sequence")
        if isinstance(sequence, dict):
            length = sequence.get("length")
            if isinstance(length, int):
                return length
            if isinstance(length, str) and length.isdigit():
                return int(length)
        length_field = entry.get("sequence_length") or entry.get("sequenceLength")
        if isinstance(length_field, int):
            return length_field
        if isinstance(length_field, str) and length_field.isdigit():
            return int(length_field)
        return None

    def _extract_taxonomy_id(self, entry: dict[str, Any]) -> int | None:
        organism = entry.get("organism")
        if isinstance(organism, dict):
            taxon_id = organism.get("taxonId") or organism.get("taxonIdentifier")
            if isinstance(taxon_id, int):
                return taxon_id
            if isinstance(taxon_id, str) and taxon_id.isdigit():
                return int(taxon_id)
        identifier = entry.get("organism_id") or entry.get("organismId")
        if isinstance(identifier, int):
            return identifier
        if isinstance(identifier, str) and identifier.isdigit():
            return int(identifier)
        return None

    def _extract_organism(self, entry: dict[str, Any]) -> str | None:
        organism = entry.get("organism")
        if isinstance(organism, dict):
            return organism.get("scientificName") or organism.get("commonName")
        if isinstance(organism, str):
            return organism
        return entry.get("organism_name")

    def _extract_lineage(self, entry: dict[str, Any]) -> list[str]:
        lineage = entry.get("lineage")
        results: list[str] = []
        if isinstance(lineage, list):
            for item in lineage:
                if isinstance(item, dict) and item.get("scientificName"):
                    results.append(item["scientificName"])
                elif isinstance(item, str):
                    results.append(item)
        return results

    def _materialize_silver(
        self,
        uniprot_df: pd.DataFrame,
        component_df: pd.DataFrame,
    ) -> None:
        """Persist silver-level UniProt artifacts deterministically."""

        if uniprot_df.empty and component_df.empty:
            logger.info("silver_materialization_skipped", reason="empty_frames")
            return

        silver_path_config = self.config.materialization.silver or Path("data/silver/targets_uniprot.parquet")
        silver_path = Path(silver_path_config)
        silver_path.parent.mkdir(parents=True, exist_ok=True)

        if not uniprot_df.empty:
            logger.info(
                "writing_silver_dataset",
                path=str(silver_path),
                rows=len(uniprot_df),
            )
            uniprot_df.sort_values("canonical_accession", inplace=True)
            uniprot_df.to_parquet(silver_path, index=False)

        component_path = silver_path.parent / "component_enrichment.parquet"
        if not component_df.empty:
            logger.info(
                "writing_component_enrichment",
                path=str(component_path),
                rows=len(component_df),
            )
            component_df.sort_values(["canonical_accession", "isoform_accession"], inplace=True)
            component_df.to_parquet(component_path, index=False)
        elif component_path.exists():
            logger.info("component_enrichment_empty", path=str(component_path))

