"""ChEMBL API client for TestItem pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import requests  # type: ignore[import-untyped]

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.deprecation import warn_legacy_client
from bioetl.core.logger import UnifiedLogger
from bioetl.sources.chembl.testitem.parser import TestItemParser
from bioetl.sources.chembl.testitem.request import TestItemRequestBuilder
from bioetl.utils.fallback import FallbackRecordBuilder, build_fallback_payload

logger = UnifiedLogger.get(__name__)

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.testitem")


class TestItemChEMBLClient:
    """Client for fetching molecule data from ChEMBL API."""

    def __init__(
        self,
        api_client: UnifiedAPIClient,
        batch_size: int,
        chembl_release: str | None,
        molecule_cache: dict[str, dict[str, Any]],
        request_builder: TestItemRequestBuilder,
        parser: TestItemParser,
        fallback_builder: FallbackRecordBuilder,
    ) -> None:
        self.api_client = api_client
        self.batch_size = batch_size
        self.chembl_release = chembl_release
        self._molecule_cache = molecule_cache
        self.request_builder = request_builder
        self.parser = parser
        self._fallback_builder = fallback_builder

    def fetch_molecules(
        self, molecule_ids: Sequence[str]
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Fetch molecules in batches, honoring cache and request limits."""

        results: list[dict[str, Any]] = []
        cache_hits = 0
        ids_to_fetch: list[str] = []

        for molecule_id in molecule_ids:
            cached = self._get_from_cache(molecule_id)
            if cached is not None:
                results.append(cached)
                cache_hits += 1
            else:
                ids_to_fetch.append(molecule_id)

        stats = {
            "cache_hits": cache_hits,
            "api_success_count": 0,
            "fallback_count": 0,
        }

        if not ids_to_fetch:
            return results, stats

        batches = self.request_builder.iter_batches(ids_to_fetch)
        for batch_index, batch_ids in enumerate(batches, start=1):
            logger.info("fetching_batch", batch=batch_index, size=len(batch_ids))
            batch_records, batch_stats = self._fetch_molecule_batch(batch_ids)
            results.extend(batch_records)
            stats["api_success_count"] += batch_stats["api_success_count"]
            stats["fallback_count"] += batch_stats["fallback_count"]

        return results, stats

    def fetch_single_molecule(self, molecule_id: str, attempt: int) -> dict[str, Any]:
        """Fetch a single molecule by ID with fallback handling."""

        try:
            response = self.api_client.request_json(f"/molecule/{molecule_id}.json")
        except requests.exceptions.HTTPError as exc:
            record = self._create_fallback_record(
                molecule_id,
                attempt=attempt,
                error=exc,
                reason="http_error",
                retry_after=self._extract_retry_after(exc),
            )
            fallback_message = record.get("fallback_error_message")
            logger.warning(
                "molecule_fallback_http_error",
                molecule_chembl_id=molecule_id,
                http_status=record.get("fallback_http_status"),
                attempt=attempt,
                message=fallback_message,
            )
            self._store_in_cache(record)
            return record
        except Exception as exc:  # noqa: BLE001
            record = self._create_fallback_record(
                molecule_id,
                attempt=attempt,
                error=exc,
                reason="unexpected_error",
            )
            logger.error(
                "molecule_fallback_unexpected_error",
                molecule_chembl_id=molecule_id,
                attempt=attempt,
                error=str(exc),
            )
            self._store_in_cache(record)
            return record

        if not isinstance(response, dict) or "molecule_chembl_id" not in response:
            record = self._create_fallback_record(
                molecule_id,
                attempt=attempt,
                reason="missing_from_response",
                message="Missing molecule in response",
            )
            logger.warning(
                "molecule_missing_in_response", molecule_chembl_id=molecule_id, attempt=attempt
            )
            self._store_in_cache(record)
            return record

        record = self.parser.parse(response)
        self._store_in_cache(record)
        return record

    def _fetch_molecule_batch(
        self, batch_ids: Sequence[str]
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        api_success_count = 0
        fallback_count = 0
        records: list[dict[str, Any]] = []

        params = self.request_builder.build_filter_params(batch_ids)

        try:
            response = self.api_client.request_json("/molecule.json", params=params)
        except Exception as exc:  # noqa: BLE001
            logger.error("batch_fetch_failed", error=str(exc), batch_ids=list(batch_ids))
            for missing_id in batch_ids:
                record = self._create_fallback_record(
                    missing_id,
                    attempt=1,
                    error=exc,
                    reason="batch_exception",
                    message="Batch request failed",
                )
                self._store_in_cache(record)
                records.append(record)
                fallback_count += 1
            return records, {"api_success_count": api_success_count, "fallback_count": fallback_count}

        molecules = response.get("molecules", []) if isinstance(response, dict) else []
        returned_ids = {
            m.get("molecule_chembl_id") for m in molecules if isinstance(m, dict)
        }
        missing_ids = [mol_id for mol_id in batch_ids if mol_id not in returned_ids]

        for payload in molecules:
            if not isinstance(payload, dict):
                continue
            record = self.parser.parse(payload)
            self._store_in_cache(record)
            records.append(record)
            if self._is_fallback_record(record):
                fallback_count += 1
            else:
                api_success_count += 1

        if missing_ids:
            logger.warning(
                "incomplete_batch_response",
                requested=len(batch_ids),
                returned=len(molecules),
                missing=missing_ids,
            )
            for missing_id in missing_ids:
                record = self.fetch_single_molecule(missing_id, attempt=2)
                records.append(record)
                if self._is_fallback_record(record):
                    fallback_count += 1
                else:
                    api_success_count += 1

        logger.info("batch_fetched", count=len(molecules))
        return records, {"api_success_count": api_success_count, "fallback_count": fallback_count}

    def _cache_key(self, molecule_id: str) -> str:
        release = self.chembl_release or "unversioned"
        return f"{release}:{molecule_id}"

    def _get_from_cache(self, molecule_id: str) -> dict[str, Any] | None:
        cache_key = self._cache_key(molecule_id)
        cached = self._molecule_cache.get(cache_key)
        if cached is None:
            return None
        return cached.copy()

    def _store_in_cache(self, record: dict[str, Any]) -> None:
        molecule_id = record.get("molecule_chembl_id")
        if not molecule_id:
            return
        cache_key = self._cache_key(str(molecule_id))
        self._molecule_cache[cache_key] = record.copy()

    def _create_fallback_record(
        self,
        molecule_id: str,
        *,
        attempt: int,
        error: Exception | None = None,
        retry_after: float | None = None,
        message: str | None = None,
        reason: str = "exception",
    ) -> dict[str, Any]:
        fallback_record = self._fallback_builder.record({"molecule_chembl_id": molecule_id})

        metadata = build_fallback_payload(
            entity="testitem",
            reason=reason,
            error=error,
            source="TESTITEM_FALLBACK",
            attempt=attempt,
            message=message,
            context=self._fallback_builder.context_with({"molecule_chembl_id": molecule_id}),
        )

        if retry_after is not None:
            metadata["fallback_retry_after_sec"] = retry_after

        if reason:
            metadata["fallback_error_code"] = reason
        else:
            metadata.setdefault("fallback_error_code", None)

        fallback_record.update(metadata)
        return fallback_record

    def _is_fallback_record(self, record: dict[str, Any]) -> bool:
        return bool(record.get("fallback_attempt")) or bool(record.get("fallback_error_code"))

    @staticmethod
    def _extract_retry_after(error: requests.exceptions.HTTPError) -> float | None:  # type: ignore[valid-type]
        if not hasattr(error, "response") or error.response is None:  # type: ignore[attr-defined]
            return None
        retry_after = error.response.headers.get("Retry-After")  # type: ignore[attr-defined]
        if retry_after is None:
            return None
        try:
            return float(retry_after)
        except (TypeError, ValueError):
            return None
