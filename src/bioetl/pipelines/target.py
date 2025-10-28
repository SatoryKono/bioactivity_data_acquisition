"""Target Pipeline - ChEMBL target data extraction with multi-stage enrichment."""

from __future__ import annotations

from pathlib import Path

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

