"""Target ETL pipeline implementation using PipelineBase."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlencode

import backoff
import pandas as pd

from bioetl.core.api_client import APIConfig, UnifiedAPIClient

from library.common.pipeline_base import PipelineBase
from library.target.config import TargetConfig
from library.target.normalize import TargetNormalizer
from library.target.quality import TargetQualityFilter
from library.target.validate import TargetValidator

logger = logging.getLogger(__name__)


class TargetPipeline(PipelineBase[TargetConfig]):
    """Target ETL pipeline using unified PipelineBase."""
    
    def __init__(self, config: TargetConfig) -> None:
        """Initialize target pipeline with configuration."""
        super().__init__(config)
        self.validator = TargetValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.normalizer = TargetNormalizer(config.model_dump() if hasattr(config, 'model_dump') else config)
        self.quality_filter = TargetQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    def _setup_clients(self) -> None:
        """Initialize HTTP clients for target sources."""
        self.clients = {}
        
        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].get("enabled", False):
            self.clients["chembl"] = self._create_chembl_client()
    
    def _create_chembl_client(self) -> UnifiedAPIClient:
        """Create a UnifiedAPIClient configured for the ChEMBL API."""

        source_config = self._get_source_config("chembl")

        base_url = source_config.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")

        http_config = source_config.get("http", {})
        global_http = self._get_http_profile("global")
        profile_http = None
        profile_name = source_config.get("http_profile")
        if profile_name:
            profile_http = self._get_http_profile(profile_name)

        timeout_connect = self._get_timeout(http_config, profile_http, global_http, "connect_timeout_sec", 10.0)
        timeout_read = self._get_timeout(http_config, profile_http, global_http, "read_timeout_sec", 60.0)

        retries = self._get_retries(http_config, profile_http, global_http)
        rate_limit = source_config.get("rate_limit") or getattr(profile_http, "rate_limit", None)
        if rate_limit is None and global_http is not None:
            rate_limit = getattr(global_http, "rate_limit", None)

        rate_limit_max_calls = 10
        rate_limit_period = 1.0
        if rate_limit is not None:
            if hasattr(rate_limit, "model_dump"):
                rate_limit = rate_limit.model_dump()
            if isinstance(rate_limit, dict):
                rate_limit_max_calls = rate_limit.get("max_calls", rate_limit_max_calls)
                rate_limit_period = rate_limit.get("period", rate_limit_period)
            else:
                rate_limit_max_calls = getattr(rate_limit, "max_calls", rate_limit_max_calls)
                rate_limit_period = getattr(rate_limit, "period", rate_limit_period)

        rate_limit_jitter = source_config.get("rate_limit_jitter")
        if rate_limit_jitter is None and profile_http is not None:
            rate_limit_jitter = getattr(profile_http, "rate_limit_jitter", None)
        if rate_limit_jitter is None and global_http is not None:
            rate_limit_jitter = getattr(global_http, "rate_limit_jitter", None)
        if isinstance(rate_limit_jitter, str):
            rate_limit_jitter = rate_limit_jitter.lower() in {"1", "true", "yes", "on"}
        if rate_limit_jitter is None:
            rate_limit_jitter = True

        headers = self._get_headers("chembl")
        headers.update(self._extract_headers(global_http))
        headers.update(self._extract_headers(profile_http))
        headers.update(self._extract_headers(http_config))
        headers.update(self._extract_headers(source_config))
        processed_headers = self._process_headers(headers)

        api_config = APIConfig(
            name="chembl",
            base_url=base_url,
            headers=processed_headers,
            cache_enabled=source_config.get("cache_enabled", True),
            cache_ttl=source_config.get("cache_ttl", 3600),
            cache_maxsize=source_config.get("cache_maxsize", 4096),
            rate_limit_max_calls=rate_limit_max_calls,
            rate_limit_period=rate_limit_period,
            rate_limit_jitter=bool(rate_limit_jitter),
            retry_total=retries.get("total", 3),
            retry_backoff_factor=retries.get("backoff_multiplier", 2.0),
            timeout_connect=timeout_connect,
            timeout_read=timeout_read,
            cb_failure_threshold=source_config.get("circuit_breaker", {}).get("failure_threshold", 5),
            cb_timeout=source_config.get("circuit_breaker", {}).get("timeout_sec", 90.0),
        )

        return UnifiedAPIClient(api_config)
    
    def _get_headers(self, source: str) -> dict[str, str]:
        """Get default headers for a source."""
        return {
            "Accept": "application/json",
            "User-Agent": "bioactivity-data-acquisition/0.1.0",
        }

    def _process_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Process headers with secret placeholders."""
        import os
        processed = {}
        for key, value in headers.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                processed[key] = os.getenv(env_var, value)
            else:
                processed[key] = value
        return processed
    
    def _get_entity_type(self) -> str:
        """Get entity type for this pipeline."""
        return "targets"
    
    def _create_qc_validator(self):
        """Create QC validator for target data."""
        from library.common.qc_profiles import QCValidator
        return QCValidator(self.config, "targets")
    
    def _create_postprocessor(self):
        """Create postprocessor for target data."""
        from library.common.postprocess_base import BasePostprocessor
        return BasePostprocessor(self.config, "targets")

    def _create_etl_writer(self):
        """Create ETL writer for target data."""
        from library.common.writer_base import ETLWriter
        return ETLWriter(self.config, "targets")

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    def _get_source_config(self, name: str) -> dict[str, Any]:
        """Return source configuration as a plain dictionary."""

        sources = getattr(self.config, "sources", {})
        if hasattr(sources, "model_dump"):
            sources = sources.model_dump()
        if isinstance(sources, dict):
            value = sources.get(name, {})
            if hasattr(value, "model_dump"):
                return value.model_dump()
            return value
        return {}

    def _get_http_profile(self, name: str | None) -> Any:
        """Return HTTP profile configuration if available."""

        if not name:
            return None

        http_settings = getattr(self.config, "http", None)
        if http_settings is None:
            return None

        if isinstance(http_settings, dict):
            return http_settings.get(name)

        if hasattr(http_settings, name):
            return getattr(http_settings, name)

        if hasattr(http_settings, "model_dump"):
            http_dump = http_settings.model_dump()
            return http_dump.get(name)

        return None

    def _get_timeout(
        self,
        http_config: dict[str, Any],
        profile_http: Any,
        global_http: Any,
        attribute: str,
        default: float,
    ) -> float:
        """Resolve timeout from layered configuration."""

        for candidate in (http_config, profile_http, global_http):
            if not candidate:
                continue
            if isinstance(candidate, dict) and attribute in candidate:
                value = candidate.get(attribute)
                if value is not None:
                    return float(value)
            if hasattr(candidate, attribute):
                value = getattr(candidate, attribute)
                if value is not None:
                    return float(value)
        return default

    def _get_retries(self, http_config: dict[str, Any], profile_http: Any, global_http: Any) -> dict[str, Any]:
        """Resolve retry configuration into a dictionary."""

        for candidate in (http_config, profile_http, global_http):
            if not candidate:
                continue
            retries = None
            if isinstance(candidate, dict):
                retries = candidate.get("retries")
            elif hasattr(candidate, "retries"):
                retries = getattr(candidate, "retries")
            if retries is None:
                continue
            if hasattr(retries, "model_dump"):
                return retries.model_dump()
            if isinstance(retries, dict):
                return retries
            # For Pydantic models with attributes
            return {
                "total": getattr(retries, "total", 3),
                "backoff_multiplier": getattr(retries, "backoff_multiplier", 2.0),
                "backoff_max": getattr(retries, "backoff_max", 60.0),
            }

        return {"total": 3, "backoff_multiplier": 2.0, "backoff_max": 60.0}

    def _extract_headers(self, source: Any) -> dict[str, str]:
        """Extract headers mapping from arbitrary configuration objects."""

        if not source:
            return {}

        if isinstance(source, dict):
            headers = source.get("headers")
            return headers if isinstance(headers, dict) else {}

        if hasattr(source, "headers"):
            headers_attr = getattr(source, "headers")
            if isinstance(headers_attr, dict):
                return headers_attr
            if hasattr(headers_attr, "model_dump"):
                return headers_attr.model_dump()

        return {}

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _fetch_targets(
        self,
        client: UnifiedAPIClient,
        target_ids: list[str],
        batch_size: int,
        max_url_length: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch target payloads from ChEMBL."""

        all_records: list[dict[str, Any]] = []
        for batch in self._chunk_ids_for_request(
            client,
            "/target.json",
            target_ids,
            batch_size,
            max_url_length,
            "target_chembl_id__in",
        ):
            params = {
                "target_chembl_id__in": ",".join(batch),
                "limit": 1000,
            }
            response = self._request_with_backoff(client, "/target.json", params)
            all_records.extend(response.get("targets", []))
            all_records.extend(self._consume_pagination(client, response, "targets"))
        return all_records

    def _fetch_components(
        self,
        client: UnifiedAPIClient,
        target_ids: list[str],
        batch_size: int,
        max_url_length: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch target component payloads from ChEMBL."""

        records: list[dict[str, Any]] = []
        for batch in self._chunk_ids_for_request(
            client,
            "/target_component.json",
            target_ids,
            batch_size,
            max_url_length,
            "target_chembl_id__in",
        ):
            params = {
                "target_chembl_id__in": ",".join(batch),
                "limit": 1000,
            }
            response = self._request_with_backoff(client, "/target_component.json", params)
            records.extend(response.get("target_components", []))
            records.extend(self._consume_pagination(client, response, "target_components"))
        return records

    def _fetch_protein_class(
        self,
        client: UnifiedAPIClient,
        target_ids: list[str],
        batch_size: int,
        max_url_length: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch protein classification payloads."""

        records: list[dict[str, Any]] = []
        for batch in self._chunk_ids_for_request(
            client,
            "/protein_classification.json",
            target_ids,
            batch_size,
            max_url_length,
            "target_chembl_id__in",
        ):
            params = {
                "target_chembl_id__in": ",".join(batch),
                "limit": 1000,
            }
            response = self._request_with_backoff(client, "/protein_classification.json", params)
            records.extend(response.get("protein_classifications", []))
            records.extend(self._consume_pagination(client, response, "protein_classifications"))
        return records

    def _fetch_relations(
        self,
        client: UnifiedAPIClient,
        target_ids: list[str],
        batch_size: int,
        max_url_length: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch target relation payloads."""

        records: list[dict[str, Any]] = []
        for batch in self._chunk_ids_for_request(
            client,
            "/target_relation.json",
            target_ids,
            batch_size,
            max_url_length,
            "target_chembl_id__in",
        ):
            params = {
                "target_chembl_id__in": ",".join(batch),
                "limit": 1000,
            }
            response = self._request_with_backoff(client, "/target_relation.json", params)
            records.extend(response.get("target_relations", []))
            records.extend(self._consume_pagination(client, response, "target_relations"))
        return records

    def _fetch_component_xrefs(
        self,
        client: UnifiedAPIClient,
        component_records: list[dict[str, Any]],
        max_url_length: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch component cross-reference payloads."""

        if not component_records:
            return []

        component_ids = sorted({str(record.get("component_id")) for record in component_records if record.get("component_id")})
        records: list[dict[str, Any]] = []

        for batch in self._chunk_ids_for_request(
            client,
            "/component_xref.json",
            component_ids,
            batch_size=100,
            max_url_length=max_url_length,
            id_param="component_id__in",
        ):
            params = {
                "component_id__in": ",".join(batch),
                "limit": 1000,
            }
            response = self._request_with_backoff(client, "/component_xref.json", params)
            records.extend(response.get("component_xrefs", []))
            records.extend(self._consume_pagination(client, response, "component_xrefs"))
        return records

    def _consume_pagination(
        self,
        client: UnifiedAPIClient,
        response: dict[str, Any],
        collection_key: str,
    ) -> list[dict[str, Any]]:
        """Consume paginated ChEMBL responses until exhausted."""

        collected: list[dict[str, Any]] = []
        page_meta = response.get("page_meta", {})
        next_url = page_meta.get("next")

        while next_url:
            payload = self._request_with_backoff(client, next_url)
            collected.extend(payload.get(collection_key, []))
            page_meta = payload.get("page_meta", {})
            next_url = page_meta.get("next")

        return collected

    def _attach_nested_payloads(
        self,
        targets_df: pd.DataFrame,
        components_df: pd.DataFrame,
        protein_class_df: pd.DataFrame,
        relations_df: pd.DataFrame,
        component_xrefs_df: pd.DataFrame,
    ) -> None:
        """Attach nested payloads as JSON columns to the targets DataFrame."""

        def to_grouped_json(df: pd.DataFrame, key: str) -> dict[str, str]:
            if df.empty or key not in df.columns:
                return {}
            grouped: dict[str, list[dict[str, Any]]] = {}
            for value, group in df.groupby(key):
                group_records = group.drop(columns=[key], errors="ignore").to_dict(orient="records")
                grouped[str(value)] = group_records
            return {k: json.dumps(v, sort_keys=True, default=str) for k, v in grouped.items()}

        components_json = to_grouped_json(components_df, "target_chembl_id")
        protein_class_json = to_grouped_json(protein_class_df, "target_chembl_id")
        relations_json = to_grouped_json(relations_df, "target_chembl_id")

        # Component xrefs are keyed by component_id, map them back to targets
        xref_by_component = to_grouped_json(component_xrefs_df, "component_id")
        target_component_map: dict[str, list[str]] = defaultdict(list)
        if not components_df.empty and "target_chembl_id" in components_df.columns and "component_id" in components_df.columns:
            for row in components_df.itertuples(index=False):
                component_id = getattr(row, "component_id")
                if component_id is None:
                    continue
                target_component_map[str(getattr(row, "target_chembl_id"))].append(str(component_id))

        xrefs_json: dict[str, str] = {}
        for target_id, comp_ids in target_component_map.items():
            merged_records: list[dict[str, Any]] = []
            for comp_id in comp_ids:
                data = xref_by_component.get(comp_id)
                if data:
                    merged_records.extend(json.loads(data))
            xrefs_json[target_id] = json.dumps(merged_records, sort_keys=True, default=str)

        def assign_json_column(df: pd.DataFrame, column: str, values: dict[str, str]) -> None:
            df[column] = df["target_chembl_id"].astype(str).map(values).fillna("[]")

        assign_json_column(targets_df, "target_components", components_json)
        assign_json_column(targets_df, "protein_classifications", protein_class_json)
        assign_json_column(targets_df, "target_relations", relations_json)
        assign_json_column(targets_df, "component_xrefs", xrefs_json)

    def _materialize_bronze(
        self,
        targets_df: pd.DataFrame,
        components_df: pd.DataFrame,
        protein_class_df: pd.DataFrame,
        relations_df: pd.DataFrame,
        component_xrefs_df: pd.DataFrame,
    ) -> None:
        """Persist bronze-level materializations as parquet and CSV."""

        runtime = getattr(self.config, "runtime", None)
        dry_run = getattr(runtime, "dry_run", False) if runtime is not None else False
        if dry_run:
            logger.info("Dry-run enabled; skipping bronze materialization")
            return

        materialization_cfg = {}
        if hasattr(self.config, "model_dump"):
            materialization_cfg = self.config.model_dump().get("materialization", {})
        else:
            materialization_cfg = getattr(self.config, "materialization", {}) if isinstance(
                getattr(self.config, "materialization", {}), dict
            ) else {}

        bronze_path = materialization_cfg.get("bronze", "data/bronze/targets.parquet")
        bronze_path = Path(bronze_path)
        bronze_dir = bronze_path if bronze_path.suffix == "" else bronze_path.parent
        bronze_dir.mkdir(parents=True, exist_ok=True)

        def write(df: pd.DataFrame, name: str, explicit_path: Path | None = None) -> None:
            if df.empty:
                logger.info(f"Skipping materialization for {name}: empty DataFrame")
                return
            parquet_path = explicit_path if explicit_path is not None else bronze_dir / f"{name}.parquet"
            csv_path = parquet_path.with_suffix(".csv")
            try:
                df.to_parquet(parquet_path, index=False)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Failed to write parquet for {name}: {exc}")
            df.to_csv(csv_path, index=False)
            logger.info(
                "bronze_materialized",
                extra={"name": name, "parquet": str(parquet_path), "csv": str(csv_path), "rows": len(df)},
            )

        write(targets_df, "targets", bronze_path if bronze_path.suffix == ".parquet" else None)
        write(components_df, "target_components")
        write(protein_class_df, "protein_classifications")
        write(relations_df, "target_relations")
        write(component_xrefs_df, "component_xrefs")

    def _chunk_ids_for_request(
        self,
        client: UnifiedAPIClient,
        endpoint: str,
        ids: Iterable[str],
        batch_size: int,
        max_url_length: int | None,
        id_param: str,
    ) -> Iterator[list[str]]:
        """Yield batches of identifiers respecting batch size and max URL length."""

        current_batch: list[str] = []
        for identifier in ids:
            candidate = current_batch + [identifier]
            if len(candidate) > batch_size and current_batch:
                yield current_batch
                current_batch = [identifier]
                continue

            if max_url_length is not None and max_url_length > 0:
                params = {id_param: ",".join(candidate)}
                url_length = self._estimate_url_length(client, endpoint, params)
                if url_length > max_url_length and current_batch:
                    yield current_batch
                    current_batch = [identifier]
                    continue
            current_batch = candidate

        if current_batch:
            yield current_batch

    def _estimate_url_length(
        self,
        client: UnifiedAPIClient,
        endpoint: str,
        params: dict[str, Any],
    ) -> int:
        """Estimate final URL length for logging and batching purposes."""

        query = urlencode(params)
        base = client.config.base_url.rstrip("/")
        endpoint_path = endpoint if endpoint.startswith("http") else f"{base}/{endpoint.lstrip('/')}"
        if query:
            return len(f"{endpoint_path}?{query}")
        return len(endpoint_path)

    def _request_with_backoff(
        self,
        client: UnifiedAPIClient,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a JSON request with exponential backoff."""

        retry_total = max(1, int(getattr(client.config, "retry_total", 3) or 3))
        backoff_factor = max(1.0, float(getattr(client.config, "retry_backoff_factor", 2.0) or 2.0))

        @backoff.on_exception(  # type: ignore[misc]
            backoff.expo,
            Exception,
            max_tries=retry_total,
            base=backoff_factor,
            jitter=backoff.full_jitter,
        )
        def _call(url: str, call_params: dict[str, Any] | None) -> dict[str, Any]:
            return client.request_json(url, params=call_params)

        return _call(endpoint, params)
    
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract target data from ChEMBL with resilient batching and persistence."""

        logger.info(f"Extracting target data for {len(input_data)} targets")

        validated_data = self.validator.validate_raw_data(input_data)

        limit = getattr(self.config.runtime, "limit", None)
        if limit is not None:
            validated_data = validated_data.head(limit)
            logger.info(f"Limited to {len(validated_data)} targets")

        if validated_data.empty:
            logger.warning("No target records to extract after validation")
            return validated_data

        if validated_data["target_chembl_id"].duplicated().any():
            raise ValueError("Duplicate target_chembl_id values detected")

        chembl_client = self.clients.get("chembl")
        if chembl_client is None:
            logger.info("Initializing ChEMBL client on-demand")
            chembl_client = self._create_chembl_client()
            self.clients["chembl"] = chembl_client

        source_config = self._get_source_config("chembl")
        batch_size = source_config.get("batch_size", 25)
        max_url_length = source_config.get("max_url_length", 2000)

        target_ids = validated_data["target_chembl_id"].astype(str).tolist()

        logger.info(
            "chembl_extraction_start",
            extra={"targets": len(target_ids), "batch_size": batch_size, "max_url_length": max_url_length},
        )

        target_records = self._fetch_targets(chembl_client, target_ids, batch_size, max_url_length)
        component_records = self._fetch_components(chembl_client, target_ids, batch_size, max_url_length)
        protein_class_records = self._fetch_protein_class(chembl_client, target_ids, batch_size, max_url_length)
        relation_records = self._fetch_relations(chembl_client, target_ids, batch_size, max_url_length)
        component_xref_records = self._fetch_component_xrefs(chembl_client, component_records, max_url_length)

        targets_df = pd.DataFrame(target_records)
        if not targets_df.empty and "target_chembl_id" in targets_df.columns:
            targets_df["target_chembl_id"] = targets_df["target_chembl_id"].astype(str)

        if targets_df.empty:
            logger.warning("No primary target payloads were returned by ChEMBL")
            targets_df = pd.DataFrame({"target_chembl_id": target_ids})

        # Ensure coverage for all requested targets
        missing_ids = set(target_ids) - set(targets_df["target_chembl_id"].astype(str))
        if missing_ids:
            logger.warning(f"Missing {len(missing_ids)} targets from ChEMBL response: {list(missing_ids)[:5]}")
            if missing_ids:
                filler = pd.DataFrame({"target_chembl_id": list(missing_ids)})
                targets_df = pd.concat([targets_df, filler], ignore_index=True, sort=False)

        components_df = pd.DataFrame(component_records)
        protein_class_df = pd.DataFrame(protein_class_records)
        relations_df = pd.DataFrame(relation_records)
        component_xrefs_df = pd.DataFrame(component_xref_records)

        self._attach_nested_payloads(
            targets_df,
            components_df,
            protein_class_df,
            relations_df,
            component_xrefs_df,
        )

        targets_df = targets_df.drop_duplicates(subset=["target_chembl_id"])

        coverage = len(targets_df[targets_df["target_chembl_id"].isin(target_ids)]) / max(len(target_ids), 1)
        logger.info(
            "chembl_extraction_metrics",
            extra={
                "rows_fetched": len(targets_df),
                "coverage": coverage,
                "components_rows": len(components_df),
                "protein_class_rows": len(protein_class_df),
                "relations_rows": len(relations_df),
                "component_xrefs_rows": len(component_xrefs_df),
            },
        )

        self._materialize_bronze(
            targets_df,
            components_df,
            protein_class_df,
            relations_df,
            component_xrefs_df,
        )

        return targets_df
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize target data."""
        logger.info(f"Normalizing {len(raw_data)} target records")
        
        # Use the normalizer
        normalized_data = self.normalizer.normalize(raw_data)
        
        logger.info(f"Normalization completed. Records: {len(normalized_data)}")
        return normalized_data
    
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate target data."""
        logger.info(f"Validating {len(data)} target records")
        
        # Use the validator
        validated_data = self.validator.validate_normalized_data(data)
        
        logger.info(f"Validation completed. Records: {len(validated_data)}")
        return validated_data
