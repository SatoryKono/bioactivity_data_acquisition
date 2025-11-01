"""HTTP client wrapper for the ChEMBL activity pipeline."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry
from bioetl.pipelines.base import PipelineBase
from bioetl.utils.chembl import SupportsRequestJson
from bioetl.utils.dtypes import coerce_nullable_int
from bioetl.sources.chembl.activity.parser.activity_parser import ActivityParser
from bioetl.sources.chembl.activity.request.activity_request import ActivityRequestBuilder

from ..core.deprecation import warn_legacy_client

__all__ = ["ActivityChEMBLClient"]

warn_legacy_client(__name__, replacement="bioetl.clients.chembl_activity")

logger = UnifiedLogger.get(__name__)

_NON_NEGATIVE_CACHE_COLUMNS: tuple[str, ...] = ("published_value", "standard_value")


class ActivityChEMBLClient:
    """Encapsulate API interactions and caching for ChEMBL activities."""

    def __init__(
        self,
        pipeline: PipelineBase,
        *,
        parser: ActivityParser,
        request_builder: ActivityRequestBuilder | None = None,
    ) -> None:
        self._pipeline = pipeline
        self._parser = parser
        chembl_context = pipeline._init_chembl_client(
            defaults={
                "enabled": True,
                "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                "batch_size": 25,
            },
            batch_size_cap=25,
        )
        self.api_client: SupportsRequestJson = chembl_context.client
        self.batch_size = chembl_context.batch_size
        self.max_url_length = chembl_context.max_url_length
        self._chembl_release: str | None = None
        self._fallback_factory: Callable[[int, str, Exception | None], dict[str, Any]] | None = None
        self._request_builder = request_builder or ActivityRequestBuilder(
            base_url=chembl_context.base_url,
            batch_size=self.batch_size,
            max_url_length=self.max_url_length,
        )
        pipeline.register_client(self.api_client)

    def set_release(self, release: str | None) -> None:
        """Update release metadata for cache scoping and parsing."""

        self._chembl_release = release
        self._parser.set_chembl_release(release)

    def set_fallback_factory(
        self, factory: Callable[[int, str, Exception | None], dict[str, Any]]
    ) -> None:
        """Provide a callback used to build fallback records."""

        self._fallback_factory = factory

    def extract(
        self,
        activity_ids: Sequence[int],
        *,
        expected_columns: Sequence[str] | None = None,
        integer_columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Extract activity data for the provided identifiers."""

        if not activity_ids:
            return pd.DataFrame()

        success_count = 0
        fallback_count = 0
        error_count = 0
        api_calls = 0
        cache_hits = 0
        results: list[dict[str, Any]] = []

        base_url = str(self.api_client.config.base_url)
        self._request_builder.base_url = base_url.rstrip("/")

        for batch_number, batch_ids in enumerate(
            self._request_builder.iter_batches(activity_ids), start=1
        ):
            logger.info("fetching_batch", batch=batch_number, size=len(batch_ids))

            cached_records = self._load_batch_from_cache(batch_ids)
            if cached_records is not None:
                logger.info("cache_batch_hit", batch=batch_number, size=len(batch_ids))
                cache_hits += len(batch_ids)
                results.extend(cached_records)
                success_count += sum(
                    1
                    for record in cached_records
                    if record.get("source_system") != "ChEMBL_FALLBACK"
                )
                fallback_count += sum(
                    1
                    for record in cached_records
                    if record.get("source_system") == "ChEMBL_FALLBACK"
                )
                continue

            try:
                batch_records, batch_metrics = self._fetch_batch(batch_ids)
                api_calls += 1
                success_count += batch_metrics["success"]
                fallback_count += batch_metrics["fallback"]
                error_count += batch_metrics["error"]
                results.extend(batch_records)
                self._store_batch_in_cache(batch_ids, batch_records)
            except CircuitBreakerOpenError as error:
                logger.warning("circuit_breaker_open", batch=batch_number, error=str(error))
                fallback_records = [
                    self._build_fallback(activity_id, "circuit_breaker_open", error)
                    for activity_id in batch_ids
                ]
                results.extend(fallback_records)
                fallback_count += len(fallback_records)
            except Exception as error:  # noqa: BLE001 - surfaced for metrics
                error_count += len(batch_ids)
                logger.error("batch_fetch_failed", error=str(error), batch_ids=batch_ids)
                fallback_records = [
                    self._build_fallback(activity_id, "exception", error)
                    for activity_id in batch_ids
                ]
                results.extend(fallback_records)
                fallback_count += len(fallback_records)

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        results_sorted = sorted(
            results, key=lambda row: (row.get("activity_id") or 0, row.get("source_system", ""))
        )

        logger.info(
            "chembl_activity_metrics",
            total_activities=len(activity_ids),
            success_count=success_count,
            fallback_count=fallback_count,
            error_count=error_count,
            success_rate=(success_count + fallback_count) / len(activity_ids)
            if activity_ids
            else 0.0,
            api_calls=api_calls,
            cache_hits=cache_hits,
        )

        df = pd.DataFrame(results_sorted)
        if not df.empty and expected_columns:
            extra_columns = [column for column in df.columns if column not in expected_columns]
            for column in expected_columns:
                if column not in df.columns:
                    if integer_columns and column in integer_columns:
                        df[column] = pd.Series(pd.NA, index=df.index, dtype="Int64")
                    else:
                        df[column] = pd.Series(pd.NA, index=df.index)
            ordered_columns = [column for column in expected_columns if column in df.columns]
            df = df[ordered_columns + extra_columns]

        if integer_columns:
            coerce_nullable_int(df, integer_columns)

        return df

    def _fetch_batch(self, batch_ids: Iterable[int]) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Fetch and parse a batch of activities from the ChEMBL API."""

        if self._fallback_factory is None:
            raise RuntimeError("Fallback factory must be configured before fetching batches")

        batch_ids = tuple(batch_ids)
        ids_str = ",".join(map(str, batch_ids))
        # ``limit`` defaults to 20 in the ChEMBL API which is lower than our batch size;
        # explicitly request ``len(batch_ids)`` (capped to the documented maximum of 1000)
        # so that all activities for the current batch are returned in a single response.
        limit = min(len(batch_ids), 1000)
        response = self.api_client.request_json(
            "/activity.json",
            params={"activity_id__in": ids_str, "limit": limit},
        )

        activities = response.get("activities", [])
        metrics = {"success": 0, "fallback": 0, "error": 0}

        records: dict[int, dict[str, Any]] = {}
        for activity in activities:
            activity_id = activity.get("activity_id")
            parsed = self._parser.parse(activity)
            parsed_id = parsed.get("activity_id")
            if parsed_id is None:
                continue
            records[int(parsed_id)] = parsed
            if activity_id and int(activity_id) != int(parsed_id):
                logger.debug(
                    "activity_id_normalized",
                    original=activity_id,
                    normalized=parsed_id,
                )

        missing_ids = [activity_id for activity_id in batch_ids if activity_id not in records]
        if missing_ids:
            metrics["error"] += len(missing_ids)
            for missing_id in missing_ids:
                records[missing_id] = self._build_fallback(missing_id, "not_in_response", None)

        metrics["success"] = sum(
            1 for record in records.values() if record.get("source_system") != "ChEMBL_FALLBACK"
        )
        metrics["fallback"] = sum(
            1 for record in records.values() if record.get("source_system") == "ChEMBL_FALLBACK"
        )

        ordered_records = [records[activity_id] for activity_id in sorted(records)]
        return ordered_records, metrics

    def _build_fallback(
        self, activity_id: int, reason: str, error: Exception | None
    ) -> dict[str, Any]:
        if self._fallback_factory is None:
            raise RuntimeError("Fallback factory must be configured before building fallbacks")
        return self._fallback_factory(activity_id, reason, error)

    def _cache_key(self, batch_ids: Iterable[int]) -> str:
        normalized = ",".join(map(str, sorted(batch_ids)))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _cache_base_dir(self) -> Path:
        base_dir = Path(self._pipeline.config.cache.directory)
        if not base_dir.is_absolute():
            if self._pipeline.config.paths.cache_root:
                base_dir = Path(self._pipeline.config.paths.cache_root) / base_dir
            else:
                base_dir = Path(base_dir)

        entity_dir = base_dir / self._pipeline.config.pipeline.entity
        if self._pipeline.config.cache.release_scoped:
            release = self._chembl_release or "unknown"
            return entity_dir / release
        return entity_dir

    def _cache_path(self, batch_ids: Iterable[int]) -> Path:
        cache_dir = self._cache_base_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"{self._cache_key(batch_ids)}.json"

    def _load_batch_from_cache(self, batch_ids: Iterable[int]) -> list[dict[str, Any]] | None:
        if not self._pipeline.config.cache.enabled:
            return None

        cache_path = self._cache_path(batch_ids)
        if not cache_path.exists():
            return None

        try:
            with cache_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError:
            logger.warning("cache_corrupted", path=str(cache_path))
            cache_path.unlink(missing_ok=True)
            return None

        if not isinstance(data, list):
            logger.warning("cache_payload_unexpected", path=str(cache_path))
            return None

        ordered_records: list[dict[str, Any]] = []
        for raw_record in data:
            if not isinstance(raw_record, dict):
                logger.warning("cache_record_invalid", path=str(cache_path))
                return None
            ordered_records.append(dict(raw_record))

        return self._sanitize_cached_records(ordered_records)

    def _sanitize_cached_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not records:
            return records

        sanitized_records: list[dict[str, Any]] = []
        for record in records:
            sanitized = dict(record)
            activity_id = sanitized.get("activity_id")

            for column in _NON_NEGATIVE_CACHE_COLUMNS:
                if column in sanitized:
                    sanitized[column] = self._sanitize_cached_non_negative(
                        sanitized.get(column),
                        column=column,
                        activity_id=activity_id,
                    )

            sanitized_records.append(sanitized)

        return sanitized_records

    def _sanitize_cached_non_negative(
        self,
        value: Any,
        *,
        column: str,
        activity_id: Any,
    ) -> float | None:
        result = registry.normalize("numeric", value)
        if result is None:
            return None

        if not isinstance(result, (int, float)):
            logger.warning(
                "cached_non_negative_sanitized",
                column=column,
                original_value=result,
                sanitized_value=None,
                activity_id=activity_id,
            )
            return None

        numeric_value = float(result)
        if numeric_value < 0:
            logger.warning(
                "cached_non_negative_sanitized",
                column=column,
                original_value=numeric_value,
                sanitized_value=None,
                activity_id=activity_id,
            )
            return None

        return numeric_value

    def _store_batch_in_cache(self, batch_ids: Iterable[int], records: list[dict[str, Any]]) -> None:
        if not self._pipeline.config.cache.enabled:
            return

        cache_path = self._cache_path(batch_ids)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        serializable = [
            dict(record)
            for record in sorted(
                records,
                key=lambda row: (row.get("activity_id") or 0, row.get("source_system", "")),
            )
        ]
        with cache_path.open("w", encoding="utf-8") as handle:
            json.dump(serializable, handle, ensure_ascii=False)
