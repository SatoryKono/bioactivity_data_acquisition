"""ChEMBL API client for TestItem pipeline."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests  # type: ignore[import-untyped]

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


class TestItemChEMBLClient:
    """Client for fetching molecule data from ChEMBL API."""

    def __init__(
        self,
        api_client: UnifiedAPIClient,
        batch_size: int,
        chembl_release: str | None,
        molecule_cache: dict[str, dict[str, Any]],
    ):
        """Initialize TestItem ChEMBL client.

        Args:
            api_client: Unified API client instance
            batch_size: Batch size for API requests
            chembl_release: ChEMBL release version
            molecule_cache: In-memory cache for molecule records
        """
        self.api_client = api_client
        self.batch_size = batch_size
        self.chembl_release = chembl_release
        self._molecule_cache = molecule_cache

    def _cache_key(self, molecule_id: str) -> str:
        """Create cache key for molecule ID."""
        release = self.chembl_release or "unversioned"
        return f"{release}:{molecule_id}"

    def _store_in_cache(self, record: dict[str, Any]) -> None:
        """Store molecule record in cache."""
        molecule_id = record.get("molecule_chembl_id")
        if not molecule_id:
            return
        cache_key = self._cache_key(str(molecule_id))
        self._molecule_cache[cache_key] = record.copy()

    def fetch_single_molecule(
        self,
        molecule_id: str,
        attempt: int,
        create_fallback_record: Any,  # Callable type
    ) -> dict[str, Any]:
        """Fetch a single molecule by ID with fallback handling.

        Args:
            molecule_id: ChEMBL molecule ID
            attempt: Attempt number for fallback tracking
            create_fallback_record: Function to create fallback records

        Returns:
            Molecule record or fallback record
        """
        try:
            response = self.api_client.request_json(f"/molecule/{molecule_id}.json")
        except requests.exceptions.HTTPError as exc:
            record = create_fallback_record(
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
            record = create_fallback_record(
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
            record = create_fallback_record(
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

        return response

    @staticmethod
    def _extract_retry_after(error: requests.exceptions.HTTPError) -> float | None:  # type: ignore[valid-type]
        """Extract Retry-After header value from HTTP error."""
        if not hasattr(error, "response") or error.response is None:  # type: ignore[attr-defined]
            return None
        retry_after = error.response.headers.get("Retry-After")  # type: ignore[attr-defined]
        if retry_after is None:
            return None
        try:
            return float(retry_after)
        except (TypeError, ValueError):
            return None

