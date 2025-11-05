"""Activity pipeline implementation for ChEMBL."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd
import pandera.errors
from requests.exceptions import RequestException

from bioetl.config import PipelineConfig
from bioetl.config.models import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.core.api_client import CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.schemas.activity import (
    ACTIVITY_PROPERTY_KEYS,
    COLUMN_ORDER,
    RELATIONS,
    STANDARD_TYPES,
)

from ..base import PipelineBase, RunResult

API_ACTIVITY_FIELDS: tuple[str, ...] = (
    "activity_id",
    "assay_chembl_id",
    "testitem_chembl_id",
    "molecule_chembl_id",
    "target_chembl_id",
    "document_chembl_id",
    "type",
    "relation",
    "value",
    "units",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_units",
    "standard_text_value",
    "standard_flag",
    "upper_value",
    "lower_value",
    "pchembl_value",
    "activity_comment",
    "bao_endpoint",
    "bao_format",
    "bao_label",
    "canonical_smiles",
    "ligand_efficiency",
    "target_organism",
    "target_tax_id",
    "data_validity_comment",
    "potential_duplicate",
    "activity_properties",
)


class ChemblActivityPipeline(PipelineBase):
    """ETL pipeline extracting activity records from the ChEMBL API."""

    actor = "activity_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)
        self._chembl_release: str | None = None
        self._last_batch_extract_stats: dict[str, Any] | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release captured during extraction."""

        return self._chembl_release

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch activity payloads from ChEMBL using the unified HTTP client."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_activity_client", client)

        self._chembl_release = self._fetch_chembl_release(client, log)

        batch_size = self._resolve_batch_size(source_config)
        limit = self.config.cli.limit
        page_size = min(batch_size, 25)
        if limit is not None:
            page_size = min(page_size, limit)
        page_size = max(page_size, 1)

        payload_activity_ids = kwargs.get("activity_ids")
        if payload_activity_ids is not None:
            batch_dataframe = self._extract_from_chembl(
                payload_activity_ids,
                client,
                batch_size=batch_size,
                limit=limit,
            )
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            batch_stats = self._last_batch_extract_stats or {}
            log.info(
                "chembl_activity.extract_summary",
                rows=int(batch_dataframe.shape[0]),
                duration_ms=duration_ms,
                chembl_release=self._chembl_release,
                batches=batch_stats.get("batches"),
                api_calls=batch_stats.get("api_calls"),
                cache_hits=batch_stats.get("cache_hits"),
            )
            return batch_dataframe

        records: list[dict[str, Any]] = []
        next_endpoint: str | None = "/activity.json"
        params: Mapping[str, Any] | None = {
            "limit": page_size,
            "only": ",".join(API_ACTIVITY_FIELDS),
        }
        pages = 0

        while next_endpoint:
            page_start = time.perf_counter()
            response = client.get(next_endpoint, params=params)
            payload = self._coerce_mapping(response.json())
            page_items = self._extract_page_items(payload)

            if limit is not None:
                remaining = max(limit - len(records), 0)
                if remaining == 0:
                    break
                page_items = page_items[:remaining]

            records.extend(page_items)
            pages += 1
            page_duration_ms = (time.perf_counter() - page_start) * 1000.0
            log.debug(
                "chembl_activity.page_fetched",
                endpoint=next_endpoint,
                batch_size=len(page_items),
                total_records=len(records),
                duration_ms=page_duration_ms,
            )

            next_link = self._next_link(payload, base_url=base_url)
            if not next_link or (limit is not None and len(records) >= limit):
                break
            # Log the next_link before using it for debugging
            log.info(
                "chembl_activity.next_link_resolved",
                next_link=next_link,
                base_url=base_url,
            )
            next_endpoint = next_link
            params = None

        dataframe: pd.DataFrame = pd.DataFrame.from_records(records)  # pyright: ignore[reportUnknownMemberType]; type: ignore
        if dataframe.empty:
            dataframe = pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})
        elif "activity_id" in dataframe.columns:
            dataframe = dataframe.sort_values("activity_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_activity.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            pages=pages,
        )
        return dataframe

    def transform(self, payload: object) -> pd.DataFrame:
        """Transform raw activity data by normalizing measurements, identifiers, and data types."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.transform")

        if not isinstance(payload, pd.DataFrame):
            if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
                df = pd.DataFrame(payload)
            elif isinstance(payload, Mapping):
                df = pd.DataFrame([payload])
            else:
                log.warning("transform_invalid_payload", payload_type=type(payload).__name__)
                return pd.DataFrame()
        else:
            df = payload.copy()

        df = self._harmonize_identifier_columns(df, log)
        df = self._ensure_schema_columns(df, log)

        if df.empty:
            log.debug("transform_empty_dataframe")
            return df

        log.info("transform_started", rows=len(df))

        df = self._normalize_identifiers(df, log)
        df = self._normalize_measurements(df, log)
        df = self._normalize_string_fields(df, log)
        df = self._normalize_nested_structures(df, log)
        df = self._normalize_data_types(df, log)
        df = self._validate_foreign_keys(df, log)
        df = self._ensure_schema_columns(df, log)
        df = self._order_schema_columns(df)

        df = self._finalize_identifier_columns(df, log)
        df = self._finalize_output_columns(df, log)

        log.info("transform_completed", rows=len(df))
        return df

    def validate(self, payload: object) -> pd.DataFrame:
        """Validate payload against ActivitySchema with detailed error handling."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.validate")

        if not isinstance(payload, pd.DataFrame):
            msg = "ChemblActivityPipeline.validate expects a pandas DataFrame payload"
            raise TypeError(msg)

        if payload.empty:
            log.debug("validate_empty_dataframe")
            return payload

        if self.config.validation.strict:
            allowed_columns = set(COLUMN_ORDER)
            extra_columns = [column for column in payload.columns if column not in allowed_columns]
            if extra_columns:
                log.debug(
                    "drop_extra_columns_before_validation",
                    extras=extra_columns,
                )
                payload = payload.drop(columns=extra_columns)

        log.info("validate_started", rows=len(payload))

        # Ensure target_tax_id has correct nullable integer type before validation
        if "target_tax_id" in payload.columns:
            dtype_name: str = str(payload["target_tax_id"].dtype.name)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
            if dtype_name != "Int64":
                # Convert to nullable Int64 if not already
                numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    payload["target_tax_id"], errors="coerce"
                )
                payload["target_tax_id"] = numeric_series.astype("Int64")

        # Pre-validation checks
        self._check_activity_id_uniqueness(payload, log)
        self._check_foreign_key_integrity(payload, log)

        # Call base validation with error handling
        # Temporarily disable coercion to preserve nullable Int64 types for target_tax_id
        # Schema has coerce=False, but base.validate uses config.validation.coerce
        original_coerce = self.config.validation.coerce
        try:
            self.config.validation.coerce = False
            validated = super().validate(payload)
            log.info(
                "validate_completed",
                rows=len(validated),
                schema=self.config.validation.schema_out,
                strict=self.config.validation.strict,
                coerce=original_coerce,  # Log original value for clarity
            )
            return validated
        except pandera.errors.SchemaErrors as exc:
            # Extract detailed error information
            failure_cases_df: pd.DataFrame | None = None
            if hasattr(exc, "failure_cases"):
                failure_cases_df = cast(pd.DataFrame, exc.failure_cases)
            error_count = len(failure_cases_df) if failure_cases_df is not None else 0
            error_summary = self._extract_validation_errors(exc)

            log.error(
                "validation_failed",
                error_count=error_count,
                schema=self.config.validation.schema_out,
                strict=self.config.validation.strict,
                coerce=original_coerce,  # Log original value for clarity
                error_summary=error_summary,
                exc_info=True,
            )

            # Log detailed failure cases if available
            if failure_cases_df is not None and not failure_cases_df.empty:
                failure_cases_summary = ChemblActivityPipeline._format_failure_cases(
                    failure_cases_df
                )
                log.error("validation_failure_cases", failure_cases=failure_cases_summary)
                # Log individual errors with row index and activity_id as per documentation
                self._log_detailed_validation_errors(failure_cases_df, payload, log)

            msg = (
                f"Validation failed with {error_count} error(s) against schema "
                f"{self.config.validation.schema_out}: {error_summary}"
            )
            raise ValueError(msg) from exc
        except Exception as exc:
            log.error(
                "validation_error",
                error=str(exc),
                schema=self.config.validation.schema_out,
                exc_info=True,
            )
            raise
        finally:
            self.config.validation.coerce = original_coerce

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str) -> SourceConfig:
        try:
            return self.config.sources[name]
        except KeyError as exc:  # pragma: no cover - configuration error path
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(parameters: Mapping[str, Any]) -> str:
        base_url = parameters.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url

    @staticmethod
    def _resolve_batch_size(source_config: SourceConfig) -> int:
        batch_size: int | None = getattr(source_config, "batch_size", None)
        if batch_size is None:
            parameters = getattr(source_config, "parameters", {})
            if isinstance(parameters, Mapping):
                candidate: Any = parameters.get("batch_size")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                if isinstance(candidate, int) and candidate > 0:
                    batch_size = candidate
        if batch_size is None or batch_size <= 0:
            batch_size = 25
        return batch_size

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient,
        log: Any,
    ) -> str | None:
        response = client.get("/status.json")
        status_payload = self._coerce_mapping(response.json())
        release_value = self._extract_chembl_release(status_payload)
        log.info("chembl_activity.status", chembl_release=release_value)
        return release_value

    def _extract_from_chembl(
        self,
        dataset: object,
        client: UnifiedAPIClient,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Extract activity records by batching ``activity_id`` values."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        method_start = time.perf_counter()
        self._last_batch_extract_stats = None

        if isinstance(dataset, pd.Series):
            input_frame = dataset.to_frame(name="activity_id")
        elif isinstance(dataset, pd.DataFrame):
            input_frame = dataset
        elif isinstance(dataset, Mapping):
            input_frame = pd.DataFrame([cast(dict[str, Any], dataset)])
        elif isinstance(dataset, Sequence) and not isinstance(dataset, (str, bytes)):
            dataset_list: list[Any] = list(dataset)  # pyright: ignore[reportUnknownArgumentType]
            input_frame = pd.DataFrame({"activity_id": dataset_list})
        else:
            msg = (
                "ChemblActivityPipeline._extract_from_chembl expects a DataFrame, Series, "
                "mapping, or sequence of activity_id values"
            )
            raise TypeError(msg)

        if "activity_id" not in input_frame.columns:
            msg = "Input dataset must contain an 'activity_id' column"
            raise ValueError(msg)

        normalized_ids: list[tuple[int, str]] = []
        invalid_ids: list[Any] = []
        seen: set[str] = set()
        for raw_id in input_frame["activity_id"].tolist():
            if pd.isna(raw_id):
                continue
            try:
                if isinstance(raw_id, str):
                    candidate = raw_id.strip()
                    if not candidate:
                        continue
                    numeric_id = int(float(candidate)) if "." in candidate else int(candidate)
                elif isinstance(raw_id, (int, float)):
                    numeric_id = int(raw_id)
                else:
                    numeric_id = int(raw_id)
            except (TypeError, ValueError):
                invalid_ids.append(raw_id)
                continue
            key = str(numeric_id)
            if key not in seen:
                seen.add(key)
                normalized_ids.append((numeric_id, key))

        if invalid_ids:
            log.warning(
                "chembl_activity.invalid_activity_ids",
                invalid_count=len(invalid_ids),
            )

        if limit is not None:
            normalized_ids = normalized_ids[: max(int(limit), 0)]

        if not normalized_ids:
            summary: dict[str, Any] = {
                "total_activities": 0,
                "success": 0,
                "fallback": 0,
                "errors": 0,
                "api_calls": 0,
                "cache_hits": 0,
                "batches": 0,
                "duration_ms": 0.0,
                "success_rate": 0.0,
            }
            self._last_batch_extract_stats = summary
            log.info("chembl_activity.batch_summary", **summary)
            return pd.DataFrame()

        effective_batch_size = batch_size or self._resolve_batch_size(self._resolve_source_config("chembl"))
        effective_batch_size = max(min(int(effective_batch_size), 25), 1)

        records: list[dict[str, Any]] = []
        success_count = 0
        fallback_count = 0
        error_count = 0
        cache_hits = 0
        api_calls = 0
        total_batches = 0

        for index in range(0, len(normalized_ids), effective_batch_size):
            batch = normalized_ids[index : index + effective_batch_size]
            batch_keys: list[str] = [key for _, key in batch]
            batch_start = time.perf_counter()
            try:
                cached_records = self._check_cache(batch_keys, self._chembl_release)
                from_cache = cached_records is not None
                batch_records: dict[str, dict[str, Any]] = {}
                if cached_records is not None:
                    batch_records = cached_records
                    cache_hits += len(batch_keys)
                else:
                    params = {
                        "activity_id__in": ",".join(batch_keys),
                        "only": ",".join(API_ACTIVITY_FIELDS),
                    }
                    response = client.get("/activity.json", params=params)
                    api_calls += 1
                    payload = self._coerce_mapping(response.json())
                    for item in self._extract_page_items(payload):
                        activity_value = item.get("activity_id")
                        if activity_value is None:
                            continue
                        batch_records[str(activity_value)] = item
                    self._store_cache(batch_keys, batch_records, self._chembl_release)

                success_in_batch = 0
                for numeric_id, key in batch:
                    record = batch_records.get(key)
                    if record and not record.get("error"):
                        materialised = dict(record)
                        materialised.setdefault("activity_id", numeric_id)
                        records.append(materialised)
                        success_count += 1
                        success_in_batch += 1
                    else:
                        fallback_record = self._create_fallback_record(numeric_id)
                        records.append(fallback_record)
                        fallback_count += 1
                        error_count += 1
                total_batches += 1
                batch_duration_ms = (time.perf_counter() - batch_start) * 1000.0
                log.debug(
                    "chembl_activity.batch_processed",
                    batch_size=len(batch_keys),
                    from_cache=from_cache,
                    success_in_batch=success_in_batch,
                    fallback_in_batch=len(batch_keys) - success_in_batch,
                    duration_ms=batch_duration_ms,
                )
            except CircuitBreakerOpenError as exc:
                total_batches += 1
                log.warning(
                    "chembl_activity.batch_circuit_breaker",
                    batch_size=len(batch_keys),
                    error=str(exc),
                )
                for numeric_id, _ in batch:
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1
            except RequestException as exc:
                total_batches += 1
                log.error(
                    "chembl_activity.batch_request_error",
                    batch_size=len(batch_keys),
                    error=str(exc),
                )
                for numeric_id, _ in batch:
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1
            except Exception as exc:  # pragma: no cover - defensive path
                total_batches += 1
                log.error(
                    "chembl_activity.batch_unhandled_error",
                    batch_size=len(batch_keys),
                    error=str(exc),
                    exc_info=True,
                )
                for numeric_id, _ in batch:
                    records.append(self._create_fallback_record(numeric_id, exc))
                    fallback_count += 1
                    error_count += 1

        duration_ms = (time.perf_counter() - method_start) * 1000.0
        total_records = len(normalized_ids)
        success_rate = (
            float(success_count + fallback_count) / float(total_records)
            if total_records
            else 0.0
        )
        summary = {
            "total_activities": total_records,
            "success": success_count,
            "fallback": fallback_count,
            "errors": error_count,
            "api_calls": api_calls,
            "cache_hits": cache_hits,
            "batches": total_batches,
            "duration_ms": duration_ms,
            "success_rate": success_rate,
        }
        self._last_batch_extract_stats = summary
        log.info("chembl_activity.batch_summary", **summary)

        dataframe: pd.DataFrame = pd.DataFrame.from_records(records)  # pyright: ignore[reportUnknownMemberType]; type: ignore
        if dataframe.empty:
            dataframe = pd.DataFrame({"activity_id": pd.Series(dtype="Int64")})
        elif "activity_id" in dataframe.columns:
            dataframe = dataframe.sort_values("activity_id").reset_index(drop=True)
        return dataframe

    def _check_cache(
        self,
        batch_ids: Sequence[str],
        release: str | None,
    ) -> dict[str, dict[str, Any]] | None:
        cache_config = self.config.cache
        if not cache_config.enabled:
            return None

        normalized_ids = [str(identifier) for identifier in batch_ids]
        cache_file = self._cache_file_path(normalized_ids, release)
        if not cache_file.exists():
            return None

        try:
            stat = cache_file.stat()
        except OSError:
            return None

        ttl_seconds = int(cache_config.ttl)
        if ttl_seconds > 0 and (time.time() - stat.st_mtime) > ttl_seconds:
            try:
                cache_file.unlink(missing_ok=True)
            except OSError:
                pass
            return None

        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            try:
                cache_file.unlink(missing_ok=True)
            except OSError:
                pass
            return None

        if not isinstance(payload, dict):
            return None

        missing = [identifier for identifier in normalized_ids if identifier not in payload]
        if missing:
            return None

        return {
            identifier: cast(dict[str, Any], payload[identifier])
            for identifier in normalized_ids
        }

    def _store_cache(
        self,
        batch_ids: Sequence[str],
        batch_data: Mapping[str, Mapping[str, Any]],
        release: str | None,
    ) -> None:
        cache_config = self.config.cache
        if not cache_config.enabled or not batch_ids or not batch_data:
            return

        normalized_ids = [str(identifier) for identifier in batch_ids]
        cache_file = self._cache_file_path(normalized_ids, release)
        normalized_set = set(normalized_ids)
        data_to_store = {key: batch_data[key] for key in normalized_set if key in batch_data}
        if not data_to_store:
            return

        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = cache_file.with_suffix(cache_file.suffix + ".tmp")
            tmp_path.write_text(
                json.dumps(data_to_store, sort_keys=True, default=str),
                encoding="utf-8",
            )
            tmp_path.replace(cache_file)
        except Exception as exc:  # pragma: no cover - cache best-effort
            log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
            log.debug("chembl_activity.cache_store_failed", error=str(exc))

    def _cache_file_path(self, batch_ids: Sequence[str], release: str | None) -> Path:
        directory = self._cache_directory(release)
        cache_key = self._cache_key(batch_ids, release)
        return directory / f"{cache_key}.json"

    def _cache_directory(self, release: str | None) -> Path:
        cache_root = Path(self.config.paths.cache_root)
        directory_name = (self.config.cache.directory or "http_cache").strip() or "http_cache"
        release_component = self._sanitize_cache_component(release or "unknown")
        pipeline_component = self._sanitize_cache_component(self.pipeline_code)
        version_component = self._sanitize_cache_component(self.config.pipeline.version or "unknown")
        return cache_root / directory_name / pipeline_component / release_component / version_component

    def _cache_key(self, batch_ids: Sequence[str], release: str | None) -> str:
        payload = {
            "ids": list(batch_ids),
            "release": release or "unknown",
            "pipeline": self.pipeline_code,
            "pipeline_version": self.config.pipeline.version or "unknown",
        }
        raw = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _sanitize_cache_component(value: str) -> str:
        sanitized = re.sub(r"[^0-9A-Za-z_.-]", "_", value)
        return sanitized or "default"

    def _create_fallback_record(self, activity_id: int, error: Exception | None = None) -> dict[str, Any]:
        """Create fallback record enriched with error metadata."""

        base_message = "Fallback: ChEMBL activity unavailable"
        message = f"{base_message} ({error})" if error else base_message
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        metadata: dict[str, Any] = {
            "source_system": "ChEMBL_FALLBACK",
            "error_type": error.__class__.__name__ if error else None,
            "chembl_release": self._chembl_release,
            "run_id": self.run_id,
            "timestamp": timestamp,
        }
        if isinstance(error, RequestException):
            response = getattr(error, "response", None)
            status_code = getattr(response, "status_code", None)
            if status_code is not None:
                metadata["http_status"] = status_code
            metadata["error_message"] = str(error)
        elif error is not None:
            metadata["error_message"] = str(error)

        fallback_properties = [
            {
                "type": "fallback_metadata",
                "relation": None,
                "units": None,
                "value": None,
                "text_value": json.dumps(metadata, sort_keys=True, default=str),
                "result_flag": None,
            }
        ]

        return {
            "activity_id": activity_id,
            "data_validity_comment": message,
            "activity_properties": self._serialize_activity_properties(
                fallback_properties
            ),
        }

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        if isinstance(payload, Mapping):
            return cast(dict[str, Any], payload)
        return {}

    @staticmethod
    def _extract_chembl_release(payload: Mapping[str, Any]) -> str | None:
        for key in ("chembl_release", "chembl_db_version", "release", "version"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
            if value is not None:
                return str(value)
        return None

    @staticmethod
    def _extract_page_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for key in ("activities", "data", "items", "results"):
            value: Any = payload.get(key)
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)  # pyright: ignore[reportUnknownVariableType]
                    for item in value  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)  # pyright: ignore[reportUnknownVariableType]
                    for item in value  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any], base_url: str) -> str | None:
        page_meta: Any = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            next_link_raw: Any = page_meta.get("next")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            next_link: str | None = cast(str | None, next_link_raw) if next_link_raw is not None else None
            if isinstance(next_link, str) and next_link:
                base_path = urlparse(base_url).path.rstrip("/")

                # If next_link is a full URL, extract only the relative path
                if next_link.startswith("http://") or next_link.startswith("https://"):
                    parsed = urlparse(next_link)
                    base_parsed = urlparse(base_url)

                    # Get paths
                    path = parsed.path
                    base_path_from_url = base_parsed.path

                    # Normalize: remove trailing slashes for comparison
                    path_normalized = path.rstrip("/")
                    base_path_normalized = base_path_from_url.rstrip("/")

                    # Remove base_path prefix from path if it exists
                    # This ensures we only return the endpoint part relative to base_url
                    if base_path_normalized and path_normalized.startswith(base_path_normalized):
                        # Extract the part after base_path
                        relative_path = path_normalized[len(base_path_normalized) :]
                        # If empty, it means paths are identical - this shouldn't happen for pagination
                        if not relative_path:
                            return None
                        # Ensure relative_path starts with /
                        if not relative_path.startswith("/"):
                            relative_path = f"/{relative_path}"
                    else:
                        # If base_path doesn't match, try to extract using /api/data/ pattern
                        # ChEMBL API URLs typically follow: .../chembl/api/data/<endpoint>
                        if "/api/data/" in path:
                            # Extract everything after /api/data/
                            parts = path.split("/api/data/", 1)
                            if len(parts) > 1:
                                relative_path = "/" + parts[1]
                            else:
                                # Shouldn't happen, but fallback
                                relative_path = path
                        else:
                            # Fallback: use the path as-is (shouldn't normally happen)
                            relative_path = path
                            # Ensure it starts with /
                            if not relative_path.startswith("/"):
                                relative_path = f"/{relative_path}"

                    # Add query string if present (preserve original query params)
                    if parsed.query:
                        relative_path = f"{relative_path}?{parsed.query}"

                    return relative_path

                # Handle relative URLs that redundantly include the base path
                if base_path:
                    normalized_base = base_path.lstrip("/")
                    stripped_link = next_link.lstrip("/")

                    if stripped_link.startswith(normalized_base + "/"):
                        stripped_link = stripped_link[len(normalized_base) :]
                    elif stripped_link == normalized_base:
                        stripped_link = ""

                    next_link = stripped_link

                next_link = next_link.lstrip("/")
                if next_link:
                    next_link = f"/{next_link}"
                else:
                    next_link = "/"

                return next_link
        return None

    # ------------------------------------------------------------------
    # Transformation helpers
    # ------------------------------------------------------------------

    def _harmonize_identifier_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Ensure canonical identifier columns are present before normalization."""

        df = df.copy()
        actions: list[str] = []

        if "assay_chembl_id" not in df.columns and "assay_id" in df.columns:
            df["assay_chembl_id"] = df["assay_id"]
            actions.append("assay_id->assay_chembl_id")

        if "testitem_chembl_id" not in df.columns:
            if "testitem_id" in df.columns:
                df["testitem_chembl_id"] = df["testitem_id"]
                actions.append("testitem_id->testitem_chembl_id")
            elif "molecule_chembl_id" in df.columns:
                df["testitem_chembl_id"] = df["molecule_chembl_id"]
                actions.append("molecule_chembl_id->testitem_chembl_id")

        if "molecule_chembl_id" not in df.columns and "testitem_chembl_id" in df.columns:
            df["molecule_chembl_id"] = df["testitem_chembl_id"]
            actions.append("testitem_chembl_id->molecule_chembl_id")

        required_columns = ["activity_id", "assay_chembl_id", "testitem_chembl_id", "molecule_chembl_id"]
        missing_required = [column for column in required_columns if column not in df.columns]
        if missing_required:
            for column in missing_required:
                df[column] = pd.Series([None] * len(df), dtype="object")
            actions.append(f"created_missing:{','.join(missing_required)}")

        alias_columns = [column for column in ("assay_id", "testitem_id") if column in df.columns]
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
        boolean_columns = {
            "potential_duplicate",
        }

        missing = [column for column in expected if column not in df.columns]
        if missing:
            for column in missing:
                if column in boolean_columns:
                    df[column] = pd.Series([pd.NA] * len(df), dtype="boolean")
                else:
                    df[column] = pd.Series([pd.NA] * len(df), dtype="object")
            log.debug("schema_columns_added", columns=missing)

        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize ChEMBL and BAO identifiers with regex validation."""

        df = df.copy()
        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")
        bao_id_pattern = re.compile(r"^BAO_\d{7}$")

        chembl_fields = [
            "assay_chembl_id",
            "testitem_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id",
            "document_chembl_id",
        ]
        bao_fields = ["bao_endpoint", "bao_format"]

        normalized_count = 0
        invalid_count = 0

        for field in chembl_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                df.loc[mask, field] = df.loc[mask, field].astype(str).str.upper().str.strip()
                valid_mask = df[field].str.match(chembl_id_pattern.pattern, na=False)
                invalid_mask = mask & ~valid_mask
                if invalid_mask.any():
                    invalid_count += int(invalid_mask.sum())
                    df.loc[invalid_mask, field] = None
                    normalized_count += int((mask & valid_mask).astype(int).sum())

        for field in bao_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                df.loc[mask, field] = df.loc[mask, field].astype(str).str.upper().str.strip()
                valid_mask = df[field].str.match(bao_id_pattern.pattern, na=False)
                invalid_mask = mask & ~valid_mask
                if invalid_mask.any():
                    invalid_count += int(invalid_mask.sum())
                    df.loc[invalid_mask, field] = None
                    normalized_count += int((mask & valid_mask).sum())

        if normalized_count > 0 or invalid_count > 0:
            log.debug(
                "identifiers_normalized",
                normalized_count=normalized_count,
                invalid_count=invalid_count,
            )

        return df

    def _finalize_identifier_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Align identifier columns after normalization and drop aliases."""

        df = df.copy()

        if {"molecule_chembl_id", "testitem_chembl_id"}.issubset(df.columns):
            mismatch_mask = (
                df["molecule_chembl_id"].notna()
                & df["testitem_chembl_id"].notna()
                & (df["molecule_chembl_id"] != df["testitem_chembl_id"])
            )
            if mismatch_mask.any():
                mismatch_count = int(mismatch_mask.sum())
                samples_raw = (
                    df.loc[mismatch_mask, ["molecule_chembl_id", "testitem_chembl_id"]]
                    .drop_duplicates()
                    .head(5)
                    .to_dict("records")  # pyright: ignore[reportUnknownMemberType]
                )
                samples: list[dict[str, Any]] = cast(list[dict[str, Any]], samples_raw)
                log.warning(
                    "identifier_mismatch",
                    count=mismatch_count,
                    samples=samples,
                )
                df.loc[mismatch_mask, "testitem_chembl_id"] = df.loc[mismatch_mask, "molecule_chembl_id"]

        required_columns = ["activity_id", "assay_chembl_id", "testitem_chembl_id", "molecule_chembl_id"]
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            for column in missing_columns:
                df[column] = pd.Series([None] * len(df), dtype="object")
            log.warning("identifier_columns_missing", columns=missing_columns)

        return df

    def _finalize_output_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Align final column order with schema and drop unexpected fields."""

        df = df.copy()
        expected = list(COLUMN_ORDER)

        extras = [column for column in df.columns if column not in expected]
        if extras:
            df = df.drop(columns=extras)
            log.debug("output_columns_dropped", columns=extras)

        missing = [column for column in expected if column not in df.columns]
        if missing:
            for column in missing:
                df[column] = pd.Series([pd.NA] * len(df), dtype="object")
            log.warning("output_columns_missing", columns=missing)

        if not expected:
            return df

        return df[expected]

    def _normalize_measurements(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize standard_value, standard_units, standard_relation, and standard_type."""

        df = df.copy()
        normalized_count = 0

        if "standard_value" in df.columns:
            mask = df["standard_value"].notna()
            if mask.any():
                # Remove non-numeric characters and handle ranges
                # Convert to string first to handle non-numeric values
                series = df.loc[mask, "standard_value"].astype(str).str.strip()

                # Remove common non-numeric characters (spaces, commas, etc.)
                series = series.str.replace(r"[,\s]", "", regex=True)

                # Handle ranges (e.g., "10-20" -> take first value, "10±5" -> take first value)
                # Extract first numeric value from ranges
                series = series.str.extract(r"([+-]?\d*\.?\d+)", expand=False)

                # Convert to numeric (NaN for empty/invalid values)
                numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    series, errors="coerce"
                )
                df.loc[mask, "standard_value"] = numeric_series

                # Check for negative values (should be >= 0)
                negative_mask = mask & (df["standard_value"] < 0)
                if negative_mask.any():
                    log.warning("negative_standard_value", count=int(negative_mask.sum()))
                    df.loc[negative_mask, "standard_value"] = None

                normalized_count += int(mask.sum())

        if "standard_relation" in df.columns:
            unicode_to_ascii = {
                "≤": "<=",
                "≥": ">=",
                "≠": "~",
            }
            mask = df["standard_relation"].notna()
            if mask.any():
                series = df.loc[mask, "standard_relation"].astype(str).str.strip()
                for unicode_char, ascii_repl in unicode_to_ascii.items():
                    series = series.str.replace(unicode_char, ascii_repl, regex=False)
                df.loc[mask, "standard_relation"] = series
                invalid_mask = mask & ~df["standard_relation"].isin(RELATIONS)
                if invalid_mask.any():
                    log.warning("invalid_standard_relation", count=int(invalid_mask.sum()))
                    df.loc[invalid_mask, "standard_relation"] = None
                normalized_count += int(mask.sum())

        if "standard_type" in df.columns:
            mask = df["standard_type"].notna()
            if mask.any():
                df.loc[mask, "standard_type"] = (
                    df.loc[mask, "standard_type"].astype(str).str.strip()
                )
                standard_types_set: set[str] = STANDARD_TYPES
                invalid_mask = mask & ~df["standard_type"].isin(standard_types_set)  # pyright: ignore[reportUnknownMemberType]
                if invalid_mask.any():
                    log.warning("invalid_standard_type", count=int(invalid_mask.sum()))
                    df.loc[invalid_mask, "standard_type"] = None
                normalized_count += int(mask.sum())

        if "standard_units" in df.columns:
            # Normalize unit synonyms: nM, µM/μM, mM, %, ratio
            # Map various representations to canonical forms while preserving unit types
            unit_mapping = {
                # nanomolar variants -> nM
                "nanomolar": "nM",
                "nmol": "nM",
                "nm": "nM",
                "NM": "nM",
                # micromolar variants -> μM
                "µM": "μM",
                "uM": "μM",
                "UM": "μM",
                "micromolar": "μM",
                "microM": "μM",
                "umol": "μM",
                # millimolar variants -> mM
                "millimolar": "mM",
                "milliM": "mM",
                "mmol": "mM",
                "MM": "mM",
                # percentage variants -> %
                "percent": "%",
                "pct": "%",
                # ratio variants -> ratio
                "ratios": "ratio",
            }
            mask = df["standard_units"].notna()
            if mask.any():
                series = df.loc[mask, "standard_units"].astype(str).str.strip()
                for old_unit, new_unit in unit_mapping.items():
                    series = series.str.replace(old_unit, new_unit, regex=False, case=False)
                df.loc[mask, "standard_units"] = series
                normalized_count += int(mask.sum())

        if "relation" in df.columns:
            unicode_to_ascii = {
                "≤": "<=",
                "≥": ">=",
                "≠": "~",
            }
            mask = df["relation"].notna()
            if mask.any():
                series = df.loc[mask, "relation"].astype(str).str.strip()
                for unicode_char, ascii_repl in unicode_to_ascii.items():
                    series = series.str.replace(unicode_char, ascii_repl, regex=False)
                df.loc[mask, "relation"] = series
                invalid_mask = mask & ~df["relation"].isin(RELATIONS)
                if invalid_mask.any():
                    log.warning("invalid_relation", count=int(invalid_mask.sum()))
                    df.loc[invalid_mask, "relation"] = None
                normalized_count += int(mask.sum())

        if normalized_count > 0:
            log.debug("measurements_normalized", normalized_count=normalized_count)

        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields: trim, empty string to null, title-case for organism."""

        df = df.copy()

        string_fields: dict[str, dict[str, Any]] = {
            "canonical_smiles": {"trim": True, "empty_to_null": True},
            "bao_label": {"trim": True, "empty_to_null": True, "max_length": 128},
            "target_organism": {"trim": True, "empty_to_null": True, "title_case": True},
            "data_validity_comment": {"trim": True, "empty_to_null": True},
            "activity_comment": {"trim": True, "empty_to_null": True},
            "standard_text_value": {"trim": True, "empty_to_null": True},
            "type": {"trim": True, "empty_to_null": True},
            "units": {"trim": True, "empty_to_null": True},
        }

        for field, options in string_fields.items():
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                series = df.loc[mask, field].astype(str)
                if options.get("trim"):
                    series = series.str.strip()
                if options.get("title_case"):
                    series = series.str.title()
                if options.get("max_length"):
                    max_len = options.get("max_length")
                    if isinstance(max_len, int):
                        series = series.str[:max_len]
                if options.get("empty_to_null"):
                    series = series.replace("", None)
                df.loc[mask, field] = series

        return df

    def _normalize_nested_structures(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Serialize nested structures (ligand_efficiency, activity_properties) to JSON strings."""

        df = df.copy()

        nested_fields = ["ligand_efficiency", "activity_properties"]

        for field in nested_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                serialized: list[Any] = []
                for idx, value in df.loc[mask, field].items():
                    if field == "activity_properties":
                        serialized_value = self._serialize_activity_properties(value, log)
                        serialized.append(serialized_value)
                        continue

                    if isinstance(value, (Mapping, list)):
                        try:
                            serialized.append(json.dumps(value, ensure_ascii=False, sort_keys=True))
                        except (TypeError, ValueError) as exc:
                            log.warning(
                                "nested_serialization_failed",
                                field=field,
                                index=idx,
                                error=str(exc),
                            )
                            serialized.append(None)
                    elif isinstance(value, str):
                        try:
                            json.loads(value)
                            serialized.append(value)
                        except (TypeError, ValueError):
                            serialized.append(None)
                    else:
                        serialized.append(None)
                df.loc[mask, field] = pd.Series(
                    serialized, dtype="object", index=df.loc[mask, field].index
                )

        return df

    def _serialize_activity_properties(self, value: Any, log: Any | None = None) -> str | None:
        """Return normalized JSON for activity_properties or None if not serializable."""

        normalized_items = self._normalize_activity_properties_items(value, log)
        if normalized_items is None:
            return None
        try:
            return json.dumps(normalized_items, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError) as exc:
            if log is not None:
                log.warning("activity_properties_serialization_failed", error=str(exc))
            return None

    def _normalize_activity_properties_items(
        self, value: Any, log: Any | None = None
    ) -> list[dict[str, Any]] | None:
        """Coerce activity_properties payloads into a list of constrained dictionaries."""

        if value is None:
            return None

        raw_value = value

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            try:
                parsed = json.loads(stripped)
            except (TypeError, ValueError):
                fallback_base: dict[str, Any | None] = dict.fromkeys(ACTIVITY_PROPERTY_KEYS, None)
                fallback_base["text_value"] = stripped
                return [fallback_base]
            else:
                value = parsed

        if isinstance(value, Mapping):
            items: list[Any] = [value]
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            items = list(value)
        else:
            if log is not None:
                log.warning(
                    "activity_properties_unhandled_type",
                    value_type=type(raw_value).__name__,
                )
            return None

        normalized: list[dict[str, Any]] = []
        for item in items:
            if item is None:
                continue
            if isinstance(item, Mapping):
                normalized_item: dict[str, Any | None] = {key: item.get(key) for key in ACTIVITY_PROPERTY_KEYS}
                result_flag_value = normalized_item.get("result_flag")
                if isinstance(result_flag_value, int) and result_flag_value in (0, 1):
                    normalized_item["result_flag"] = bool(result_flag_value)
                normalized.append(normalized_item)
            elif isinstance(item, str):
                str_base: dict[str, Any | None] = dict.fromkeys(ACTIVITY_PROPERTY_KEYS, None)
                str_base["text_value"] = item
                normalized.append(str_base)
            else:
                if log is not None:
                    log.warning(
                        "activity_properties_item_unhandled",
                        item_type=type(item).__name__,
                    )

        return normalized

    def _order_schema_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return DataFrame with schema columns ordered ahead of extras."""

        extras = [column for column in df.columns if column not in COLUMN_ORDER]
        if self.config.validation.strict:
            return df[list(COLUMN_ORDER)]
        return df[[*COLUMN_ORDER, *extras]]

    def _normalize_data_types(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Convert data types according to the Pandera schema."""

        df = df.copy()

        # Non-nullable integer fields
        non_nullable_int_fields = {
            "activity_id": "int64",
        }

        # Nullable integer fields
        nullable_int_fields = {
            "target_tax_id": "int64",
        }

        # Float fields
        float_fields = {
            "standard_value": "float64",
            "pchembl_value": "float64",
            "upper_value": "float64",
            "lower_value": "float64",
        }

        bool_fields = [
            "potential_duplicate",
        ]

        binary_flag_fields = [
            "standard_flag",
        ]

        # Handle non-nullable integers
        for field in non_nullable_int_fields:
            if field not in df.columns:
                continue
            try:
                numeric_series_int: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[field], errors="coerce"
                )
                df[field] = numeric_series_int.astype("Int64")
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=field, error=str(exc))

        # Handle nullable integers - preserve NA values
        for field in nullable_int_fields:
            if field not in df.columns:
                continue
            try:
                # Convert to numeric, preserving NA values
                nullable_numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[field], errors="coerce"
                )
                # Use Int64 (nullable integer) to preserve NA values
                df[field] = nullable_numeric_series.astype("Int64")
                # For nullable fields, ensure values >= 1 if not NA
                mask_valid = df[field].notna()
                if mask_valid.any():
                    invalid_mask = mask_valid & (df[field] < 1)
                    if invalid_mask.any():
                        log.warning(
                            "invalid_positive_integer",
                            field=field,
                            count=int(invalid_mask.sum()),
                        )
                        df.loc[invalid_mask, field] = pd.NA
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=field, error=str(exc))

        # Handle float fields
        for field in float_fields:
            if field not in df.columns:
                continue
            try:
                numeric_series_float: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[field], errors="coerce"
                )
                df[field] = numeric_series_float.astype("float64")
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=field, error=str(exc))

        for field in bool_fields:
            if field not in df.columns:
                continue
            try:
                bool_numeric_series: pd.Series[Any] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[field], errors="coerce"
                )
                filled_series: pd.Series[Any] = bool_numeric_series.fillna(False)  # pyright: ignore[reportUnknownMemberType]
                df[field] = filled_series.astype(bool)
            except (ValueError, TypeError) as exc:
                log.warning("bool_conversion_failed", field=field, error=str(exc))

        for field in binary_flag_fields:
            if field not in df.columns:
                continue
            try:
                numeric_series_flag: pd.Series[Any] = pd.to_numeric(df[field], errors="coerce")
                df[field] = numeric_series_flag.astype("Int64")
                mask_valid = df[field].notna()
                if mask_valid.any():
                    valid_values = df.loc[mask_valid, field]
                    invalid_valid_mask = ~valid_values.isin([0, 1])
                    if invalid_valid_mask.any():
                        invalid_index = valid_values.index[invalid_valid_mask]
                        log.warning(
                            "invalid_standard_flag",
                            field=field,
                            count=int(invalid_valid_mask.sum()),
                        )
                        df.loc[invalid_index, field] = pd.NA
            except (ValueError, TypeError) as exc:
                log.warning("type_conversion_failed", field=field, error=str(exc))

        return df

    def _validate_foreign_keys(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Validate foreign key integrity and format of ChEMBL IDs."""

        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")
        chembl_fields = [
            "assay_chembl_id",
            "testitem_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id",
            "document_chembl_id",
        ]

        warnings: list[str] = []

        for field in chembl_fields:
            if field not in df.columns:
                continue
            mask = df[field].notna()
            if mask.any():
                invalid_mask = mask & ~df[field].astype(str).str.match(
                    chembl_id_pattern.pattern, na=False
                )
                if invalid_mask.any():
                    warning_msg: str = f"{field}: {int(invalid_mask.sum())} invalid format(s)"
                    warnings.append(warning_msg)

        if warnings:
            log.warning("foreign_key_validation", warnings=warnings)

        return df

    def _check_activity_id_uniqueness(self, df: pd.DataFrame, log: Any) -> None:
        """Check uniqueness of activity_id before validation."""

        if "activity_id" not in df.columns:
            log.warning("activity_id_uniqueness_check_skipped", reason="column_not_found")
            return

        duplicates = df[df["activity_id"].duplicated(keep=False)]
        if not duplicates.empty:
            duplicate_count = len(duplicates)
            duplicate_ids = duplicates["activity_id"].unique().tolist()
            log.error(
                "activity_id_duplicates_found",
                duplicate_count=duplicate_count,
                duplicate_ids=duplicate_ids[:10],  # Log first 10 duplicates
                total_duplicate_ids=len(duplicate_ids),
            )
            msg = (
                f"Found {duplicate_count} duplicate activity_id value(s): "
                f"{duplicate_ids[:5]}{'...' if len(duplicate_ids) > 5 else ''}"
            )
            raise ValueError(msg)

        log.debug("activity_id_uniqueness_verified", unique_count=df["activity_id"].nunique())

    def _check_foreign_key_integrity(self, df: pd.DataFrame, log: Any) -> None:
        """Check foreign key integrity for ChEMBL IDs (format validation for non-null values)."""

        reference_fields = [
            "assay_chembl_id",
            "testitem_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id",
            "document_chembl_id",
        ]
        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")
        errors: list[str] = []

        for field in reference_fields:
            if field not in df.columns:
                log.debug(
                    "foreign_key_integrity_check_skipped", field=field, reason="column_not_found"
                )
                continue

            mask = df[field].notna()
            if not mask.any():
                log.debug("foreign_key_integrity_check_skipped", field=field, reason="all_null")
                continue

            invalid_mask = mask & ~df[field].astype(str).str.match(
                chembl_id_pattern.pattern, na=False
            )
            if invalid_mask.any():
                invalid_count = int(invalid_mask.sum())
                invalid_samples = df.loc[invalid_mask, field].unique().tolist()[:5]
                errors.append(
                    f"{field}: {invalid_count} invalid format(s), samples: {invalid_samples}"
                )
                log.warning(
                    "foreign_key_integrity_invalid",
                    field=field,
                    invalid_count=invalid_count,
                    samples=invalid_samples,
                )

        if errors:
            log.error("foreign_key_integrity_check_failed", errors=errors)
            msg = f"Foreign key integrity check failed: {'; '.join(errors)}"
            raise ValueError(msg)

        log.debug("foreign_key_integrity_verified")

    @staticmethod
    def _extract_validation_errors(exc: pandera.errors.SchemaErrors) -> dict[str, Any]:
        """Extract structured error information from SchemaErrors."""

        summary: dict[str, Any] = {
            "error_types": [],
            "affected_columns": [],
            "affected_rows": 0,
        }

        if hasattr(exc, "failure_cases") and not cast(pd.DataFrame, exc.failure_cases).empty:
            failure_cases = cast(pd.DataFrame, exc.failure_cases)
            summary["affected_rows"] = int(failure_cases["index"].nunique())

            if "schema_context" in failure_cases.columns:
                error_counts_series: pd.Series[Any] = failure_cases["schema_context"].value_counts()
                error_types_raw = error_counts_series.to_dict()  # pyright: ignore[reportUnknownMemberType]
                error_types_result: dict[Any, int] = cast(dict[Any, int], error_types_raw)
                summary["error_types"] = dict(error_types_result)

            if "column" in failure_cases.columns:
                affected_columns = failure_cases["column"].dropna().unique().tolist()
                summary["affected_columns"] = affected_columns

        if hasattr(exc, "error_counts"):
            error_counts = getattr(exc, "error_counts", None)
            if isinstance(error_counts, Mapping):
                summary["error_counts"] = dict(cast(dict[str, Any], error_counts))

        return summary

    @staticmethod
    def _format_failure_cases(failure_cases: pd.DataFrame) -> dict[str, Any]:
        """Format failure_cases DataFrame for logging."""

        if failure_cases.empty:
            return {}

        formatted: dict[str, Any] = {
            "total_failures": len(failure_cases),
            "unique_rows": int(failure_cases["index"].nunique())
            if "index" in failure_cases.columns
            else 0,
        }

        # Group by error type if schema_context is available
        if "schema_context" in failure_cases.columns:
            error_counts_series: pd.Series[Any] = failure_cases["schema_context"].value_counts().head(10)
            error_types_raw = error_counts_series.to_dict()  # pyright: ignore[reportUnknownMemberType]
            error_types: dict[Any, int] = cast(dict[Any, int], error_types_raw)
            formatted["error_types"] = dict(error_types)

        # Group by column if available
        if "column" in failure_cases.columns:
            column_counts_series: pd.Series[Any] = failure_cases["column"].value_counts().head(10)
            column_errors_raw = column_counts_series.to_dict()  # pyright: ignore[reportUnknownMemberType]
            column_errors: dict[Any, int] = cast(dict[Any, int], column_errors_raw)
            formatted["column_errors"] = dict(column_errors)

        # Sample of failure cases (first 5)
        if len(failure_cases) > 0:
            sample = failure_cases.head(5)
            sample_raw = sample.to_dict("records")  # pyright: ignore[reportUnknownMemberType]
            formatted["sample"] = cast(list[dict[str, Any]], sample_raw)

        return formatted

    def _log_detailed_validation_errors(
        self,
        failure_cases: pd.DataFrame,
        payload: pd.DataFrame,
        log: Any,
    ) -> None:
        """Log individual validation errors with row index and activity_id."""

        if failure_cases.empty or payload.empty:
            return

        # Get activity_id column if available
        activity_id_col = "activity_id" if "activity_id" in payload.columns else None
        index_col = "index" if "index" in failure_cases.columns else None

        if index_col is None:
            return

        # Limit to first 20 errors to avoid log spam
        max_errors = 20
        errors_to_log = failure_cases.head(max_errors)

        for _, error_row in errors_to_log.iterrows():
            row_index = error_row.get(index_col)
            if row_index is None:
                continue

            error_details: dict[str, Any] = {
                "row_index": int(row_index)
                if isinstance(row_index, (int, float))
                else str(row_index),
            }

            # Add activity_id if available
            if activity_id_col:
                try:
                    # Cast row_index to int for proper indexing
                    idx = int(row_index) if isinstance(row_index, (int, float)) else row_index
                    activity_id_value: Any = payload.at[cast(int, idx), activity_id_col]
                    activity_id = activity_id_value
                except (KeyError, IndexError):
                    activity_id = None
                if activity_id is not None and pd.notna(activity_id):
                    error_details["activity_id"] = (
                        int(activity_id)
                        if isinstance(activity_id, (int, float))
                        else str(activity_id)
                    )

            # Add column name if available
            if "column" in error_row and pd.notna(error_row["column"]):
                error_details["column"] = str(error_row["column"])

            # Add schema context if available
            if "schema_context" in error_row and pd.notna(error_row["schema_context"]):
                error_details["schema_context"] = str(error_row["schema_context"])

            # Add error message if available
            if "failure_case" in error_row and pd.notna(error_row["failure_case"]):
                error_details["failure_case"] = str(error_row["failure_case"])

            log.error("validation_error_detail", **error_details)

        if len(failure_cases) > max_errors:
            log.warning(
                "validation_errors_truncated",
                total_errors=len(failure_cases),
                logged_errors=max_errors,
            )

    def build_quality_report(self, df: pd.DataFrame) -> pd.DataFrame | dict[str, object] | None:
        """Return QC report with activity-specific metrics including distributions."""

        # Build base quality report with activity_id as business key for duplicate checking
        business_key = ["activity_id"] if "activity_id" in df.columns else None
        from bioetl.qc.report import build_quality_report as build_default_quality_report

        base_report = build_default_quality_report(df, business_key_fields=business_key)
        # build_default_quality_report always returns pd.DataFrame by contract

        rows: list[dict[str, Any]] = []
        if not base_report.empty:
            records_raw = base_report.to_dict("records")  # pyright: ignore[reportUnknownMemberType]
            records: list[dict[str, Any]] = cast(list[dict[str, Any]], records_raw)
            for record in records:
                rows.append({str(k): v for k, v in record.items()})

        # Add foreign key integrity metrics
        chembl_id_pattern = re.compile(r"^CHEMBL\d+$")
        foreign_key_fields = [
            "assay_chembl_id",
            "molecule_chembl_id",
            "target_chembl_id",
            "document_chembl_id",
        ]
        for field in foreign_key_fields:
            if field in df.columns:
                mask = df[field].notna()
                if mask.any():
                    string_series = df[field].astype(str)
                    valid_mask = mask & string_series.str.match(chembl_id_pattern.pattern, na=False)
                    invalid_count = int((mask & ~valid_mask).astype(int).sum())
                    valid_count = int(valid_mask.astype(int).sum())
                    total_count = int(mask.astype(int).sum())
                    integrity_ratio = float(valid_count / total_count) if total_count > 0 else 0.0
                    rows.append(
                        {
                            "section": "foreign_key",
                            "metric": "integrity_ratio",
                            "column": field,
                            "value": float(integrity_ratio),
                            "valid_count": int(valid_count),
                            "invalid_count": int(invalid_count),
                            "total_count": int(total_count),
                        }
                    )

        # Add measurement type distribution
        if "standard_type" in df.columns:
            type_counts_series: pd.Series[Any] = df["standard_type"].value_counts()
            type_dist_raw = type_counts_series.to_dict()  # pyright: ignore[reportUnknownMemberType]
            type_dist: dict[Any, int] = cast(dict[Any, int], type_dist_raw)
            for type_value, count in type_dist.items():
                rows.append(
                    {
                        "section": "distribution",
                        "metric": "standard_type_count",
                        "column": "standard_type",
                        "value": str(type_value) if type_value is not None else "null",
                        "count": int(count),
                    }
                )

        # Add unit distribution
        if "standard_units" in df.columns:
            unit_counts_series: pd.Series[Any] = df["standard_units"].value_counts()
            unit_dist_raw = unit_counts_series.to_dict()  # pyright: ignore[reportUnknownMemberType]
            unit_dist: dict[Any, int] = cast(dict[Any, int], unit_dist_raw)
            for unit_value, count in unit_dist.items():
                rows.append(
                    {
                        "section": "distribution",
                        "metric": "standard_units_count",
                        "column": "standard_units",
                        "value": str(unit_value) if unit_value is not None else "null",
                        "count": int(count),
                    }
                )

        return pd.DataFrame(rows)

    def write(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = False,
    ) -> RunResult:
        """Override write() to bind actor and ensure deterministic sorting.

        Parameters
        ----------
        df:
            The DataFrame to write.
        output_path:
            The base output path for all artifacts.
        extended:
            Whether to include extended QC artifacts.

        Returns
        -------
        RunResult:
            All artifacts generated by the write operation.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.write")

        # Bind actor to logs for all write operations
        UnifiedLogger.bind(actor=self.actor)

        # Ensure sort configuration is set for activity pipeline
        # Use canonical identifier columns from schema: assay_chembl_id, testitem_chembl_id, activity_id
        sort_keys = ["assay_chembl_id", "testitem_chembl_id", "activity_id"]

        # Check if all sort keys exist in the DataFrame
        # If DataFrame is empty or missing columns, fall back to original sort config
        if df.empty or not all(key in df.columns for key in sort_keys):
            # Use original sort config if DataFrame is empty or missing required columns
            return super().write(df, output_path, extended=extended)

        # Temporarily override sort config if not already set
        original_sort_by = self.config.determinism.sort.by
        if not original_sort_by or original_sort_by != sort_keys:
            # Create a modified config with the correct sort keys
            from copy import deepcopy

            from bioetl.config.models import DeterminismSortingConfig

            modified_config = deepcopy(self.config)
            modified_config.determinism.sort = DeterminismSortingConfig(
                by=sort_keys,
                ascending=[True, True, True],
                na_position="last",
            )

            log.debug(
                "write_sort_config_set",
                sort_keys=sort_keys,
                original_sort_keys=list(original_sort_by) if original_sort_by else [],
            )

            # Temporarily replace config
            original_config = self.config
            self.config = modified_config

            try:
                result = super().write(df, output_path, extended=extended)
            finally:
                # Restore original config
                self.config = original_config

            return result

        # If sort config already matches, proceed normally
        return super().write(df, output_path, extended=extended)
