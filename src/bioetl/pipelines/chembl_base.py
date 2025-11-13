"""Shared utilities and base class for ChEMBL pipelines.

This module provides common functionality for all ChEMBL-based pipelines,
including configuration resolution, API client management, pagination handling,
and data extraction utilities.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol, cast
from urllib.parse import urlencode, urlparse

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.config.models.models import SourceConfig
from bioetl.config.pipeline_source import ChemblPipelineSourceConfig
from bioetl.core import APIClientFactory
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.core.mapping_utils import stringify_mapping

from .base import PipelineBase
from .common.release_tracker import ChemblHandshakeResult, ChemblReleaseMixin

# ChemblClient is dynamically loaded in __init__.py at runtime
# Type checking uses Any for client parameters to avoid circular dependencies


class ProcessItemFn(Protocol):
    """Signature for per-item processing callbacks used in pagination."""

    def __call__(self, __item: dict[str, Any]) -> dict[str, Any]:
        ...


class ChemblPipelineBase(ChemblReleaseMixin, PipelineBase):
    """Base class for ChEMBL-based ETL pipelines.

    This class provides common functionality for all ChEMBL pipelines,
    including configuration resolution, API client management, pagination,
    and data extraction utilities.
    """

    def __init__(self, config: Any, run_id: str) -> None:
        """Initialize the ChEMBL pipeline base.

        Parameters
        ----------
        config
            Pipeline configuration object.
        run_id
            Unique identifier for this pipeline run.
        """
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)

    # ------------------------------------------------------------------
    # Configuration resolution methods
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str) -> SourceConfig:
        """Resolve source configuration by name.

        Parameters
        ----------
        name
            Name of the source configuration to resolve.

        Returns
        -------
        SourceConfig
            The resolved source configuration.

        Raises
        ------
        KeyError
            If the source is not configured for this pipeline.
        """
        try:
            return self.config.sources[name]
        except KeyError as exc:
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _normalize_parameters(parameters: Any) -> dict[str, Any]:
        """Return parameters as a plain mapping.

        Parameters
        ----------
        parameters:
            Source configuration parameters represented as Mapping, Pydantic
            model, or arbitrary object with attributes.

        Returns
        -------
        dict[str, Any]
            Normalised mapping with stringified keys preserving deterministic
            ordering semantics for later processing.
        """

        if isinstance(parameters, Mapping):
            mapping = cast(Mapping[object, Any], parameters)
            return stringify_mapping(mapping)

        model_dump = getattr(parameters, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, Mapping):
                mapping = cast(Mapping[object, Any], dumped)
                return stringify_mapping(mapping)

        as_dict = getattr(parameters, "dict", None)
        if callable(as_dict):
            dumped = as_dict()
            if isinstance(dumped, Mapping):
                mapping = cast(Mapping[object, Any], dumped)
                return stringify_mapping(mapping)

        attrs = getattr(parameters, "__dict__", None)
        if isinstance(attrs, dict):
            attr_mapping = cast(dict[str, Any], attrs)
            return {key: value for key, value in attr_mapping.items() if not key.startswith("_")}

        return {}

    @staticmethod
    def _resolve_base_url(parameters: Any) -> str:
        """Resolve base URL from source configuration parameters.

        Parameters
        ----------
        parameters
            Source configuration parameters mapping.

        Returns
        -------
        str
            The resolved base URL, normalized (trailing slash removed).

        Raises
        ------
        ValueError
            If base_url is not a non-empty string.
        """
        params = ChemblPipelineBase._normalize_parameters(parameters)
        base_url = params.get("base_url") or "https://www.ebi.ac.uk/chembl/api/data"
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url.rstrip("/")

    @staticmethod
    def _resolve_page_size(batch_size: int, limit: int | None, *, hard_cap: int = 25) -> int:
        """Return deterministic page size respecting limit and API hard cap."""

        effective = min(max(int(batch_size), 1), hard_cap)
        if limit is not None:
            effective = min(effective, max(int(limit), 0))
        return max(effective, 1)

    @staticmethod
    def _resolve_batch_size(source_config: SourceConfig) -> int:
        """Resolve batch size from source configuration.

        Parameters
        ----------
        source_config
            Source configuration object.

        Returns
        -------
        int
            The resolved batch size (default: 25).
        """
        batch_size: int | None = getattr(source_config, "batch_size", None)
        if batch_size is None:
            parameters = getattr(source_config, "parameters", {})
            if isinstance(parameters, Mapping):
                parameters_mapping = cast(Mapping[str, Any], parameters)
                candidate: Any = parameters_mapping.get("batch_size")
                if isinstance(candidate, int) and candidate > 0:
                    batch_size = candidate
        if batch_size is None or batch_size <= 0:
            batch_size = 25
        return batch_size

    def _resolve_select_fields(
        self,
        source_config: SourceConfig,
        default_fields: Sequence[str] | None = None,
    ) -> list[str]:
        """Resolve select_fields from config or use default.

        Parameters
        ----------
        source_config
            Source configuration object.
        default_fields
            Optional default field list to use if not configured.

        Returns
        -------
        list[str]
            List of field names to select from the API.
        """
        parameters_raw = getattr(source_config, "parameters", {})
        if isinstance(parameters_raw, Mapping):
            parameters = cast(Mapping[str, Any], parameters_raw)
            select_fields_raw = parameters.get("select_fields")
            if (
                select_fields_raw is not None
                and isinstance(select_fields_raw, Sequence)
                and not isinstance(select_fields_raw, (str, bytes))
            ):
                select_fields = cast(Sequence[Any], select_fields_raw)
                return [str(field) for field in select_fields]
        if default_fields:
            return list(default_fields)
        return []

    # ------------------------------------------------------------------
    # API client management
    # ------------------------------------------------------------------

    def prepare_chembl_client(
        self,
        source_name: str = "chembl",
        *,
        base_url: str | None = None,
        client_name: str | None = None,
    ) -> tuple[UnifiedAPIClient, str]:
        """Prepare and register a ChEMBL API client.

        Parameters
        ----------
        source_name
            Name of the source configuration (default: "chembl").
        base_url
            Optional base URL override. If not provided, resolved from config.
        client_name
            Optional client registration name. If not provided, uses default.

        Returns
        -------
        tuple[UnifiedAPIClient, str]
            The prepared API client and resolved base URL.
        """
        source_config = self._resolve_source_config(source_name)
        parameters = getattr(source_config, "parameters", {})
        resolved_base_url = base_url or self._resolve_base_url(parameters)
        client = self._client_factory.for_source(source_name, base_url=resolved_base_url)
        if client_name:
            self.register_client(client_name, client)
        return client, resolved_base_url

    # ------------------------------------------------------------------
    # ChEMBL release fetching
    # ------------------------------------------------------------------

    def fetch_chembl_release(
        self,
        client: UnifiedAPIClient | Any,  # pyright: ignore[reportAny]
        log: BoundLogger | None = None,
    ) -> str | None:
        """Fetch ChEMBL release version from status endpoint.

        Supports both UnifiedAPIClient (direct HTTP) and ChemblClient
        (wrapped with handshake) interfaces.

        Parameters
        ----------
        client
            API client (UnifiedAPIClient or ChemblClient).
        log
            Optional logger instance. If not provided, creates one.

        Returns
        -------
        str | None
            The ChEMBL release version, or None if unavailable.
        """
        if log is None:
            log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")

        release_value: str | None = None
        requested_at = datetime.now(timezone.utc)

        # Check if client is ChemblClient by checking for handshake method
        handshake_candidate = getattr(client, "handshake", None)
        if callable(handshake_candidate):
            try:
                handshake_result: ChemblHandshakeResult = self.perform_chembl_handshake(
                    client,
                    log=log,
                    event=f"{self.pipeline_code}.status",
                    endpoint="/status",
                    enabled=True,
                )
                release_value = handshake_result.release
                requested_at = handshake_result.requested_at_utc
            except Exception as exc:
                log.warning(f"{self.pipeline_code}.status_failed", error=str(exc))
            finally:
                self._update_release(release_value)
                self.record_extract_metadata(
                    chembl_release=release_value,
                    requested_at_utc=requested_at,
                )
            return release_value

        # Use direct HTTP for UnifiedAPIClient
        get_candidate = getattr(client, "get", None)
        if callable(get_candidate):
            client_get = cast(Callable[..., Any], get_candidate)
            requested_at = datetime.now(timezone.utc)
            try:
                response = client_get("/status.json")
                json_candidate = getattr(response, "json", None)
                if callable(json_candidate):
                    status_payload_raw = json_candidate()
                    status_payload = self._coerce_mapping(status_payload_raw)
                    release_value = self._extract_chembl_release(status_payload)
                    log.info(f"{self.pipeline_code}.status", chembl_release=release_value)
            except Exception as exc:
                log.warning(f"{self.pipeline_code}.status_failed", error=str(exc))
            finally:
                self._update_release(release_value)
                self.record_extract_metadata(
                    chembl_release=release_value,
                    requested_at_utc=requested_at,
                )
            return release_value
        self._update_release(None)
        self.record_extract_metadata(requested_at_utc=datetime.now(timezone.utc))
        return None

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient | Any,  # pyright: ignore[reportAny]
        log: BoundLogger | None = None,
    ) -> str | None:
        """Backward compatible wrapper for tests expecting private method."""

        return self.fetch_chembl_release(client, log)

    def perform_source_handshake(
        self,
        handshake_target: Any,
        *,
        source_config: ChemblPipelineSourceConfig[Any],
        log: BoundLogger,
        event: str,
    ) -> ChemblHandshakeResult:
        """Выполнить handshake для переданного клиента и обновить release."""

        handshake_result: ChemblHandshakeResult = self.perform_chembl_handshake(
            handshake_target,
            log=log,
            event=event,
            endpoint=source_config.handshake_endpoint,
            enabled=source_config.handshake_enabled,
        )
        return handshake_result

    @staticmethod
    def _extract_page_items(
        payload: Mapping[str, Any],
        items_keys: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Extract items from paginated response payload.

        Parameters
        ----------
        payload
            Paginated response payload mapping.
        items_keys
            Optional sequence of keys to check for items. If not provided,
            uses common defaults: ("data", "items", "results").

        Returns
        -------
        list[dict[str, Any]]
            List of extracted item dictionaries.
        """
        if items_keys is None:
            items_keys = ("data", "items", "results")

        for key in items_keys:
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                candidates: list[dict[str, Any]] = []
                sequence_items = cast(Sequence[object], value)
                for item in sequence_items:
                    if isinstance(item, Mapping):
                        mapping = cast(Mapping[object, Any], item)
                        candidates.append(stringify_mapping(mapping))
                if candidates:
                    return candidates

        # Fallback: iterate all keys except page_meta
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                candidates = []
                sequence_items = cast(Sequence[object], value)
                for item in sequence_items:
                    if isinstance(item, Mapping):
                        mapping = cast(Mapping[object, Any], item)
                        candidates.append(stringify_mapping(mapping))
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any], base_url: str) -> str | None:
        """Extract next page link from paginated response.

        Parameters
        ----------
        payload
            Paginated response payload mapping.
        base_url
            Base URL for the API (used to normalize full URLs to relative paths).

        Returns
        -------
        str | None
            Relative path for the next page, or None if no next page.
        """
        page_meta = payload.get("page_meta")
        if not isinstance(page_meta, Mapping):
            return None

        page_meta_mapping = cast(Mapping[str, Any], page_meta)
        next_link_raw = page_meta_mapping.get("next")
        if not isinstance(next_link_raw, str):
            return None

        next_link = next_link_raw.strip()
        if not next_link:
            return None

        # If next_link is a full URL, extract only the relative path
        if next_link.startswith("http://") or next_link.startswith("https://"):
            parsed = urlparse(next_link)
            base_parsed = urlparse(base_url)

            # Normalize paths: remove trailing slashes for comparison
            path = parsed.path.rstrip("/")
            base_path = base_parsed.path.rstrip("/")

            # If paths match, return just the path with query
            if path == base_path or path.startswith(f"{base_path}/"):
                relative_path = path[len(base_path) :] if path.startswith(base_path) else path
                if parsed.query:
                    return f"{relative_path}?{parsed.query}"
                return relative_path

            # If base paths don't match, return full URL path + query
            if parsed.query:
                return f"{parsed.path}?{parsed.query}"
            return parsed.path

        # Already a relative path
        return next_link

    # ------------------------------------------------------------------
    # Batch extraction utilities
    # ------------------------------------------------------------------

    def extract_ids_paginated(
        self,
        ids: Sequence[str],
        endpoint: str,
        id_column: str,
        id_param_name: str,
        client: UnifiedAPIClient,
        *,
        batch_size: int | None = None,
        limit: int | None = None,
        select_fields: Sequence[str] | None = None,
        items_keys: Sequence[str] | None = None,
        page_size: int | None = None,
        max_url_length: int | None = None,
        process_item: ProcessItemFn | None = None,
    ) -> pd.DataFrame:
        """Extract records by batching ID values with pagination support.

        Parameters
        ----------
        ids
            Sequence of ID values to extract.
        endpoint
            API endpoint path (e.g., "/activity.json", "/document.json").
        id_column
            Name of the ID column in the resulting DataFrame.
        id_param_name
            API parameter name for ID filtering (e.g., "activity_id__in", "document_chembl_id__in").
        client
            Unified API client instance.
        batch_size
            Optional batch size override. If not provided, resolved from config.
        limit
            Optional limit on total number of records to extract.
        select_fields
            Optional list of fields to select from the API.
        items_keys
            Optional keys to check for items in response (passed to _extract_page_items).
        page_size
            Optional explicit page size for batched requests. Defaults to config.
        max_url_length
            Optional explicit maximum URL length used to split batches.
        process_item
            Optional callable to process each item before adding to results.

        Returns
        -------
        pd.DataFrame
            DataFrame containing extracted records, sorted by ID column.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        method_start = time.perf_counter()

        source_config_raw = self._resolve_source_config("chembl")
        chembl_source_config = cast(
            ChemblPipelineSourceConfig[Any],
            ChemblPipelineSourceConfig.from_source_config(source_config_raw),
        )

        defaults = ChemblPipelineSourceConfig.defaults
        config_page_size = int(chembl_source_config.page_size)
        effective_page_size = page_size if page_size is not None else config_page_size
        base_batch_size = batch_size if batch_size is not None else effective_page_size
        effective_size = self._resolve_page_size(
            base_batch_size,
            limit,
            hard_cap=defaults.page_size_cap,
        )
        max_url_length_candidate: Any = getattr(chembl_source_config, "max_url_length", None)
        config_max_url_length = (
            int(max_url_length_candidate)
            if isinstance(max_url_length_candidate, int)
            else None
        )
        effective_max_url_length = (
            max_url_length if max_url_length is not None else config_max_url_length
        )

        # Extract unique IDs, filter out NaN, sort for determinism
        unique_ids = sorted({str(id_val) for id_val in ids if id_val and str(id_val).strip()})

        if not unique_ids:
            log.debug("extract_ids_paginated.no_valid_ids")
            return pd.DataFrame({id_column: pd.Series(dtype="string")})

        # Process in batches
        all_records: list[dict[str, Any]] = []
        batches = 0
        api_calls = 0

        def _should_flush(candidate: Sequence[str]) -> bool:
            if len(candidate) > effective_size:
                return True
            if effective_max_url_length is None:
                return False
            query_params: dict[str, str] = {id_param_name: ",".join(candidate)}
            if select_fields:
                query_params["only"] = ",".join(select_fields)
            encoded: str = urlencode(query_params)
            estimated_length = len(endpoint) + 1 + len(encoded)
            return estimated_length > effective_max_url_length

        current_batch: list[str] = []
        for identifier in unique_ids:
            candidate_batch = [*current_batch, identifier]
            if current_batch and _should_flush(candidate_batch):
                batches += 1
                request_params: dict[str, Any] = {
                    id_param_name: ",".join(current_batch),
                    "limit": len(current_batch),
                }
                if select_fields:
                    request_params["only"] = ",".join(select_fields)

                limit_reached = False
                try:
                    response = client.get(endpoint, params=request_params)
                    api_calls += 1
                    payload = self._coerce_mapping(response.json())
                    page_items = self._extract_page_items(payload, items_keys=items_keys)

                    for item in page_items:
                        processed_item = process_item(dict(item)) if process_item else dict(item)
                        all_records.append(processed_item)

                    if limit is not None and len(all_records) >= limit:
                        all_records = all_records[:limit]
                        limit_reached = True
                        break

                except Exception as exc:
                    log.warning(
                        "extract_ids_paginated.batch_error",
                        batch_ids=current_batch,
                        error=str(exc),
                        exc_info=True,
                    )
                if limit_reached:
                    break
                current_batch = [identifier]
                continue

            current_batch = candidate_batch

        if current_batch:
            batches += 1

            final_request_params: dict[str, Any] = {
                id_param_name: ",".join(current_batch),
                "limit": len(current_batch),
            }
            if select_fields:
                final_request_params["only"] = ",".join(select_fields)

            try:
                response = client.get(endpoint, params=final_request_params)
                api_calls += 1
                payload = self._coerce_mapping(response.json())
                page_items = self._extract_page_items(payload, items_keys=items_keys)

                for item in page_items:
                    processed_item = process_item(dict(item)) if process_item else dict(item)
                    all_records.append(processed_item)

                    if limit is not None and len(all_records) >= limit:
                        all_records = all_records[:limit]
                        break

            except Exception as exc:
                log.warning(
                    "extract_ids_paginated.batch_error",
                    batch_ids=current_batch,
                    error=str(exc),
                    exc_info=True,
                )
        dataframe = pd.DataFrame.from_records(all_records)  # pyright: ignore[reportUnknownMemberType]
        if dataframe.empty:
            dataframe = pd.DataFrame({id_column: pd.Series(dtype="string")})
        elif id_column in dataframe.columns:
            dataframe = dataframe.sort_values(id_column).reset_index(drop=True)

        duration_ms = (time.perf_counter() - method_start) * 1000.0
        summary: dict[str, int | float] = {
            "rows": int(dataframe.shape[0]),
            "requested": len(unique_ids),
            "batches": batches,
            "api_calls": api_calls,
            "duration_ms": duration_ms,
        }
        log.info("extract_ids_paginated.summary", **summary)

        if hasattr(self, "_last_batch_extract_stats"):
            self._last_batch_extract_stats = summary  # pyright: ignore[reportAttributeAccessIssue]

        return dataframe
