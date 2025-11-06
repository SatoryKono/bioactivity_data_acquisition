"""Shared utilities and base class for ChEMBL pipelines.

This module provides common functionality for all ChEMBL-based pipelines,
including configuration resolution, API client management, pagination handling,
and data extraction utilities.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.config.models.source import SourceConfig
from bioetl.core import APIClientFactory
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

from ..base import PipelineBase

# ChemblClient is dynamically loaded in __init__.py at runtime
# Type checking uses Any for client parameters to avoid circular dependencies


class ChemblPipelineBase(PipelineBase):
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
        self._chembl_release: str | None = None

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release captured during extraction."""
        return self._chembl_release

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
    def _resolve_base_url(parameters: Mapping[str, Any]) -> str:
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
        base_url = parameters.get("base_url") or "https://www.ebi.ac.uk/chembl/api/data"
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url.rstrip("/")

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
                candidate: Any = parameters.get("batch_size")  # pyright: ignore[reportAssignmentType]
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
        if not isinstance(parameters, Mapping):
            parameters = {}
        resolved_base_url = base_url or self._resolve_base_url(cast(Mapping[str, Any], parameters))
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

        # Check if client is ChemblClient by checking for handshake method
        if hasattr(client, "handshake") and callable(getattr(client, "handshake", None)):  # pyright: ignore[reportArgumentType]
            # Use handshake method for ChemblClient
            try:
                status = client.handshake("/status")  # pyright: ignore[reportAttributeAccessIssue, reportAny]
                if isinstance(status, Mapping):
                    release_value = status.get("chembl_db_version") or status.get("chembl_release")  # pyright: ignore[reportAssignmentType]
                    if isinstance(release_value, str):
                        log.info(f"{self.pipeline_code}.status", chembl_release=release_value)
                        return release_value
            except Exception as exc:
                log.warning(f"{self.pipeline_code}.status_failed", error=str(exc))
            return None

        # Use direct HTTP for UnifiedAPIClient
        if hasattr(client, "get") and callable(getattr(client, "get", None)):  # pyright: ignore[reportArgumentType]
            try:
                response = client.get("/status.json")  # pyright: ignore[reportAttributeAccessIssue, reportAny]
                if hasattr(response, "json"):  # pyright: ignore[reportArgumentType]
                    status_payload = self._coerce_mapping(response.json())  # pyright: ignore[reportAttributeAccessIssue, reportAny]
                    release_value = self._extract_chembl_release(status_payload)
                    log.info(f"{self.pipeline_code}.status", chembl_release=release_value)
                    return release_value
            except Exception as exc:
                log.warning(f"{self.pipeline_code}.status_failed", error=str(exc))
        return None

    @staticmethod
    def _extract_chembl_release(payload: Mapping[str, Any]) -> str | None:
        """Extract ChEMBL release version from status payload.

        Parameters
        ----------
        payload
            Status response payload mapping.

        Returns
        -------
        str | None
            The release version string, or None if not found.
        """
        for key in ("chembl_release", "chembl_db_version", "release", "version"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return None

    # ------------------------------------------------------------------
    # Response processing utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        """Coerce payload to dictionary mapping.

        Parameters
        ----------
        payload
            Response payload (may be dict, Mapping, or other).

        Returns
        -------
        dict[str, Any]
            Dictionary representation of the payload.
        """
        if isinstance(payload, Mapping):
            return cast(dict[str, Any], payload)
        return {}

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

        candidates: list[dict[str, Any]] = []
        for key in items_keys:
            value: Any = payload.get(key)  # pyright: ignore[reportAssignmentType]
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)  # pyright: ignore[reportAny, reportArgumentType, reportUnknownArgumentType]
                    for item in value  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates

        # Fallback: iterate all keys except page_meta
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)  # pyright: ignore[reportAny, reportArgumentType, reportUnknownArgumentType]
                    for item in value  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(item, Mapping)
                ]
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
        page_meta: Any = payload.get("page_meta")  # pyright: ignore[reportAssignmentType]
        if not isinstance(page_meta, Mapping):
            return None

        next_link_raw: Any = page_meta.get("next")  # pyright: ignore[reportAssignmentType]
        next_link: str | None = cast(str | None, next_link_raw) if next_link_raw is not None else None
        if not isinstance(next_link, str) or not next_link:
            return None

        # If next_link is a full URL, extract only the relative path
        if next_link.startswith("http://") or next_link.startswith("https://"):
            parsed = urlparse(next_link)  # pyright: ignore[reportArgumentType]
            base_parsed = urlparse(base_url)  # pyright: ignore[reportArgumentType]

            # Normalize paths: remove trailing slashes for comparison
            path = parsed.path.rstrip("/")  # pyright: ignore[reportArgumentType]
            base_path = base_parsed.path.rstrip("/")  # pyright: ignore[reportArgumentType]

            # If paths match, return just the path with query
            if path == base_path or path.startswith(base_path + "/"):  # pyright: ignore[reportArgumentType]
                relative_path = path[len(base_path) :] if path.startswith(base_path) else path  # pyright: ignore[reportArgumentType]
                if parsed.query:
                    return f"{relative_path}?{parsed.query}"
                return relative_path  # pyright: ignore[reportReturnType]

            # If base paths don't match, return full URL path + query
            if parsed.query:
                return f"{parsed.path}?{parsed.query}"  # pyright: ignore[reportReturnType]
            return parsed.path  # pyright: ignore[reportReturnType]

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
        process_item: Any | None = None,
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
        process_item
            Optional callable to process each item before adding to results.

        Returns
        -------
        pd.DataFrame
            DataFrame containing extracted records, sorted by ID column.
        """
        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        method_start = time.perf_counter()

        if batch_size is None:
            source_config = self._resolve_source_config("chembl")
            batch_size = self._resolve_batch_size(source_config)

        # Ensure batch_size does not exceed ChEMBL API limit
        batch_size = min(batch_size, 25)
        batch_size = max(batch_size, 1)

        # Extract unique IDs, filter out NaN, sort for determinism
        unique_ids = sorted({str(id_val) for id_val in ids if id_val and str(id_val).strip()})

        if not unique_ids:
            log.debug("extract_ids_paginated.no_valid_ids")
            return pd.DataFrame({id_column: pd.Series(dtype="string")})

        # Process in batches
        all_records: list[dict[str, Any]] = []
        batches = 0
        api_calls = 0

        for i in range(0, len(unique_ids), batch_size):
            batch_ids = unique_ids[i : i + batch_size]
            batches += 1

            params: dict[str, Any] = {
                id_param_name: ",".join(batch_ids),
                "limit": batch_size,
            }
            if select_fields:
                params["only"] = ",".join(select_fields)

            try:
                response = client.get(endpoint, params=params)
                api_calls += 1
                payload = self._coerce_mapping(response.json())
                page_items = self._extract_page_items(payload, items_keys=items_keys)

                for item in page_items:
                    if process_item:
                        processed_item = process_item(dict(item))
                    else:
                        processed_item = dict(item)
                    all_records.append(processed_item)

                if limit is not None and len(all_records) >= limit:
                    all_records = all_records[:limit]
                    break

            except Exception as exc:
                log.warning(
                    "extract_ids_paginated.batch_error",
                    batch_ids=batch_ids,
                    error=str(exc),
                    exc_info=True,
                )

        dataframe = pd.DataFrame.from_records(all_records)  # pyright: ignore[reportUnknownMemberType]
        if dataframe.empty:
            dataframe = pd.DataFrame({id_column: pd.Series(dtype="string")})
        elif id_column in dataframe.columns:
            dataframe = dataframe.sort_values(id_column).reset_index(drop=True)

        duration_ms = (time.perf_counter() - method_start) * 1000.0
        log.info(
            "extract_ids_paginated.summary",
            rows=int(dataframe.shape[0]),
            requested=len(unique_ids),
            batches=batches,
            api_calls=api_calls,
            duration_ms=duration_ms,
        )

        return dataframe

