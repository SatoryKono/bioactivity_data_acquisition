"""Base class for external API adapters."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import structlog
    from structlog import stdlib

import pandas as pd

from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


class AdapterFetchError(RuntimeError):
    """Exception raised when an adapter cannot retrieve requested records."""

    def __init__(
        self,
        message: str,
        *,
        failed_ids: Sequence[str] | None = None,
        partial_records: Sequence[dict[str, Any]] | None = None,
        errors: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.failed_ids = list(failed_ids or [])
        self.partial_records = list(partial_records or [])
        self.errors = dict(errors or {})

    def __str__(self) -> str:  # pragma: no cover - formatting helper
        base_message = super().__str__()
        if not self.failed_ids:
            return base_message
        preview = ", ".join(self.failed_ids[:3])
        suffix = "" if len(self.failed_ids) <= 3 else ", ..."
        return f"{base_message} (failed ids: {preview}{suffix})"


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
    logger: structlog.stdlib.BoundLogger

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize adapter with API client."""
        self.api_config = api_config
        self.adapter_config = adapter_config
        self.api_client = UnifiedAPIClient(api_config)
        self.logger = cast(stdlib.BoundLogger, UnifiedLogger.get(self.__class__.__name__))

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

        sanitized_ids = [identifier for identifier in ids if identifier]
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
        successful_batches = 0
        partial_records_collected = False
        aggregated_failed_ids: list[str] = []
        aggregated_errors: dict[str, str] = {}
        last_error: Exception | None = None
        for index, start in enumerate(range(0, len(sanitized_ids), effective_batch_size)):
            batch_ids = sanitized_ids[start : start + effective_batch_size]
            try:
                batch_records = self._fetch_batch(batch_ids)
            except AdapterFetchError as exc:
                last_error = exc
                if exc.partial_records:
                    all_records.extend(exc.partial_records)
                    partial_records_collected = True
                if exc.failed_ids:
                    aggregated_failed_ids.extend(exc.failed_ids)
                if exc.errors:
                    aggregated_errors.update(exc.errors)
                self.logger.error(
                    log_event,
                    batch=start,
                    batch_index=index,
                    error=str(exc),
                    failed_ids=exc.failed_ids or None,
                )
                continue
            except Exception as exc:
                last_error = exc
                aggregated_failed_ids.extend(batch_ids)
                aggregated_errors.update({identifier: str(exc) for identifier in batch_ids})
                self.logger.error(
                    log_event,
                    batch=start,
                    batch_index=index,
                    error=str(exc),
                )
                continue

            if batch_records:
                all_records.extend(batch_records)
            successful_batches += 1

        if successful_batches == 0 and not partial_records_collected:
            failed_ids = aggregated_failed_ids or sanitized_ids
            message = (
                str(last_error)
                if last_error is not None and str(last_error)
                else f"{self.__class__.__name__} failed to fetch any records"
            )
            raise AdapterFetchError(
                message,
                failed_ids=failed_ids,
                errors=aggregated_errors or None,
            )

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

        items_list = list(items)

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
                record_id = raw_record.get("id", "unknown")
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

