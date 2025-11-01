"""Client wrapper for the ChEMBL assay pipeline."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Callable

import requests

from bioetl.core.api_client import CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.core.deprecation import warn_legacy_client
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.sources.chembl.assay.request.assay_request import AssayRequestBuilder

logger = UnifiedLogger.get(__name__)

warn_legacy_client(__name__, replacement="bioetl.clients.chembl_assay")


FallbackFactory = Callable[[str, str, Exception | None], dict[str, Any]]
RecordTransformer = Callable[[dict[str, Any]], dict[str, Any]]


class AssayChEMBLClient:
    """Handle ChEMBL assay requests with shared caching and metrics."""

    def __init__(
        self,
        pipeline: PipelineBase,
        *,
        defaults: Mapping[str, Any] | None = None,
        batch_size_cap: int | None = None,
    ) -> None:
        """Instantiate the client wrapper and underlying API client."""

        context = pipeline._init_chembl_client(
            defaults=defaults,
            batch_size_cap=batch_size_cap,
        )

        self.api_client: UnifiedAPIClient = context.client
        self.batch_size: int = context.batch_size
        resolved_max_url = context.max_url_length or (defaults or {}).get("max_url_length")
        if resolved_max_url is None:
            resolved_max_url = 2000
        self.max_url_length: int = max(1, int(resolved_max_url))
        self.base_url: str = context.base_url

        self._cache_enabled = bool(pipeline.config.cache.enabled)
        self._cache: dict[str, dict[str, Any]] = {}

        self._stats: dict[str, Any] = {
            "requested": 0,
            "success_count": 0,
            "cache_hits": 0,
            "cache_fallback_hits": 0,
            "fallback_counts": Counter(),
        }

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------
    def _cache_key(self, assay_id: str, release: str | None) -> str:
        version = release or "unknown"
        return f"assay:{version}:{assay_id}"

    def _cache_get(self, assay_id: str, release: str | None) -> dict[str, Any] | None:
        if not self._cache_enabled:
            return None
        cached = self._cache.get(self._cache_key(assay_id, release))
        if cached is not None:
            logger.debug("assay_cache_hit", assay_id=assay_id)
            return cached.copy()
        return None

    def _cache_set(
        self,
        assay_id: str,
        release: str | None,
        payload: dict[str, Any],
    ) -> None:
        if not self._cache_enabled:
            return
        key = self._cache_key(assay_id, release)
        self._cache[key] = payload.copy()
        logger.debug("assay_cache_store", key=key)

    # ------------------------------------------------------------------
    # Metrics helpers
    # ------------------------------------------------------------------
    @property
    def stats(self) -> dict[str, Any]:
        """Return a shallow copy of the current fetch statistics."""

        payload = self._stats.copy()
        payload["fallback_counts"] = Counter(self._stats["fallback_counts"])
        return payload

    def _register_fallback(
        self,
        assay_id: str,
        release: str | None,
        reason: str,
        factory: FallbackFactory,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        self._stats["fallback_counts"][reason] += 1
        record = factory(assay_id, reason, error)
        self._cache_set(assay_id, release, record)
        return record

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------
    def fetch_assays(
        self,
        assay_ids: Sequence[str],
        *,
        release: str | None,
        request_builder: AssayRequestBuilder,
        transform: RecordTransformer,
        fallback_factory: FallbackFactory,
    ) -> list[dict[str, Any]]:
        """Fetch assay payloads, applying caching and fallbacks as needed."""

        if not assay_ids:
            return []

        results: list[dict[str, Any]] = []
        pending: list[str] = []
        seen: set[str] = set()

        for assay_id in assay_ids:
            if not assay_id or assay_id in seen:
                continue
            seen.add(assay_id)
            self._stats["requested"] += 1
            cached = self._cache_get(assay_id, release)
            if cached is not None:
                self._stats["cache_hits"] += 1
                if str(cached.get("source_system")) == "ChEMBL_FALLBACK":
                    self._stats["cache_fallback_hits"] += 1
                    self._stats["fallback_counts"]["cache_hit"] += 1
                else:
                    self._stats["success_count"] += 1
                results.append(cached)
            else:
                pending.append(assay_id)

        if not pending:
            return results

        for batch_index, batch_ids in enumerate(
            request_builder.iter_assay_batches(pending),
            start=1,
        ):
            if not batch_ids:
                continue

            logger.info(
                "assay_fetch_batch", batch=batch_index, size=len(batch_ids)
            )

            try:
                payloads = self._fetch_assay_batch(batch_ids)
            except CircuitBreakerOpenError as exc:
                logger.error("assay_fetch_circuit_open", error=str(exc), batch_ids=batch_ids)
                for assay_id in batch_ids:
                    record = self._register_fallback(
                        assay_id,
                        release,
                        "circuit_open",
                        fallback_factory,
                        error=exc,
                    )
                    results.append(record)
                continue
            except requests.exceptions.RequestException as exc:
                logger.error("assay_fetch_request_exception", error=str(exc), batch_ids=batch_ids)
                for assay_id in batch_ids:
                    record = self._register_fallback(
                        assay_id,
                        release,
                        "request_exception",
                        fallback_factory,
                        error=exc,
                    )
                    results.append(record)
                continue
            except Exception as exc:  # noqa: BLE001 - defensive capture
                logger.error("assay_fetch_failed", error=str(exc), batch_ids=batch_ids)
                for assay_id in batch_ids:
                    record = self._register_fallback(
                        assay_id,
                        release,
                        "unexpected_error",
                        fallback_factory,
                        error=exc,
                    )
                    results.append(record)
                continue

            for assay_id in batch_ids:
                payload = payloads.get(assay_id)
                if payload is None:
                    logger.warning("assay_missing_from_response", assay_id=assay_id)
                    record = self._register_fallback(
                        assay_id,
                        release,
                        "missing_from_response",
                        fallback_factory,
                    )
                    results.append(record)
                    continue

                record = transform(payload)
                self._cache_set(assay_id, release, record)
                self._stats["success_count"] += 1
                results.append(record)

        return results

    def _fetch_assay_batch(
        self,
        batch_ids: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        """Fetch a batch of assays and map them by identifier."""

        response = self.api_client.request_json(
            "/assay.json",
            params={"assay_chembl_id__in": ",".join(batch_ids)},
        )

        payloads: list[dict[str, Any]] = []
        if isinstance(response, dict):
            payloads = response.get("assays", []) or []
        elif isinstance(response, list):
            payloads = [item for item in response if isinstance(item, dict)]

        assays_by_id: dict[str, dict[str, Any]] = {}
        for assay in payloads:
            assay_id = assay.get("assay_chembl_id")
            if assay_id:
                assays_by_id[str(assay_id)] = assay

        return assays_by_id

    def clear_cache(self) -> None:
        """Clear any cached assay payloads."""

        self._cache.clear()

    def snapshot_metrics(self) -> dict[str, Any]:
        """Return a calculated metrics payload for observability."""

        stats = self.stats
        fallback_counts: Counter[str] = stats["fallback_counts"]
        fallback_total = int(sum(fallback_counts.values()))
        requested = int(stats.get("requested", 0))
        success_count = int(stats.get("success_count", 0))
        cache_hits = int(stats.get("cache_hits", 0))
        cache_fallback_hits = int(stats.get("cache_fallback_hits", 0))

        success_rate = 0.0
        if requested:
            success_rate = (success_count + fallback_total) / requested

        return {
            "requested": requested,
            "success_count": success_count,
            "fallback_total": fallback_total,
            "fallback_by_reason": dict(fallback_counts),
            "cache_hits": cache_hits,
            "cache_fallback_hits": cache_fallback_hits,
            "success_rate": success_rate,
        }
