"""Base class for external API adapters."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


@dataclass
class AdapterConfig:
    """Configuration for external adapter."""

    enabled: bool = True
    batch_size: int = 50
    workers: int = 1
    tool: str = "bioactivity_etl"
    email: str = ""
    api_key: str = ""
    mailto: str = ""


class ExternalAdapter(ABC):
    """Base class for external API adapters.

    Subclasses are expected to implement :meth:`_fetch_batch` for fetching a
    batch of identifiers. The :meth:`_fetch_in_batches` helper coordinates
    validation, batching, and logging for the common ``fetch_by_ids`` workflow.
    """

    DEFAULT_BATCH_SIZE: int | None = 50

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize adapter with API client."""
        self.api_config = api_config
        self.adapter_config = adapter_config
        self.api_client = UnifiedAPIClient(api_config)
        self.logger = UnifiedLogger.get(self.__class__.__name__)

    def fetch_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch records by identifiers (DOI, PMID, etc)."""

        return self._fetch_in_batches(ids)

    @abstractmethod
    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize a single record from the external source."""

    @abstractmethod
    def _fetch_batch(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch a batch of identifiers from the upstream API."""

    def _fetch_in_batches(
        self,
        ids: list[str],
        *,
        batch_size: int | None = None,
        log_event: str = "batch_fetch_failed",
    ) -> list[dict[str, Any]]:
        """Fetch identifiers in batches using :meth:`_fetch_batch`.

        Args:
            ids: Identifiers to fetch from the external source.
            batch_size: Explicit batch size override. When ``None`` the helper
                falls back to ``adapter_config.batch_size`` and then the
                adapter's ``DEFAULT_BATCH_SIZE`` before defaulting to the total
                number of identifiers.
            log_event: Structured logging event name for batch failures.

        Returns:
            Aggregated list of records fetched by :meth:`_fetch_batch`.
        """

        if not ids:
            return []

        sanitized_ids = [identifier for identifier in ids if isinstance(identifier, str) and identifier]
        filtered = len(ids) - len(sanitized_ids)

        if filtered:
            self.logger.warning("invalid_ids_filtered", dropped=filtered)

        if not sanitized_ids:
            return []

        effective_batch_size = self._resolve_batch_size(
            requested=batch_size,
            total=len(sanitized_ids),
        )

        all_records: list[dict[str, Any]] = []
        for index, start in enumerate(range(0, len(sanitized_ids), effective_batch_size)):
            batch_ids = sanitized_ids[start : start + effective_batch_size]
            try:
                batch_records = self._fetch_batch(batch_ids)
            except Exception as exc:
                self.logger.error(
                    log_event,
                    batch=start,
                    batch_index=index,
                    error=str(exc),
                )
                continue

            if batch_records:
                all_records.extend(batch_records)

        return all_records

    def to_dataframe(self, records: list[dict[str, Any]]) -> pd.DataFrame:
        """Convert list of normalized records to DataFrame."""
        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        return df

    def process(self, ids: list[str]) -> pd.DataFrame:
        """Process list of IDs: fetch, normalize, convert to DataFrame."""

        return self._process_collection(ids, self.fetch_by_ids)

    def _process_collection(
        self,
        items: Sequence[str],
        fetch_fn: Callable[[list[str]], list[dict[str, Any]]],
        *,
        start_event: str = "starting_fetch",
        no_items_event: str = "no_ids_provided",
        empty_event: str = "no_records_fetched",
    ) -> pd.DataFrame:
        """Shared workflow for fetching, normalizing, and tabulating records."""

        items_list = list(items) if items is not None else []

        if not items_list:
            self.logger.info(no_items_event, adapter=self.__class__.__name__)
            return pd.DataFrame()

        self.logger.info(start_event, adapter=self.__class__.__name__, count=len(items_list))

        raw_records = fetch_fn(items_list)

        if not raw_records:
            self.logger.warning(empty_event, adapter=self.__class__.__name__)
            return pd.DataFrame()

        normalized_records: list[dict[str, Any]] = []
        for raw_record in raw_records:
            try:
                normalized = self.normalize_record(raw_record)
            except Exception as exc:  # pragma: no cover - defensive logging
                if isinstance(raw_record, dict):
                    record_id = raw_record.get("id", "unknown")
                else:
                    record_id = getattr(raw_record, "id", "unknown")
                self.logger.error(
                    "normalization_failed",
                    adapter=self.__class__.__name__,
                    error=str(exc),
                    record_id=record_id,
                )
                continue

            if normalized:
                normalized_records.append(normalized)

        self.logger.info(
            "fetch_completed",
            adapter=self.__class__.__name__,
            fetched=len(raw_records),
            normalized=len(normalized_records),
        )

        return self.to_dataframe(normalized_records)

    def close(self) -> None:
        """Close adapter and cleanup resources."""
        self.api_client.close()

    def _resolve_batch_size(self, *, requested: int | None, total: int) -> int:
        """Determine effective batch size for :meth:`_fetch_in_batches`.

        The resolution order is ``requested`` > ``adapter_config.batch_size`` >
        ``DEFAULT_BATCH_SIZE`` > ``total``. Non-positive values are ignored.
        """

        def _valid(candidate: int | None) -> int | None:
            if candidate is None:
                return None
            if isinstance(candidate, bool):  # Guard against bool-as-int
                return None
            return candidate if candidate > 0 else None

        for candidate in (
            _valid(requested),
            _valid(getattr(self.adapter_config, "batch_size", None)),
            _valid(getattr(self, "DEFAULT_BATCH_SIZE", None)),
        ):
            if candidate:
                return candidate

        return max(total, 1)

