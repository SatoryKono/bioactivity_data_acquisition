"""Shared utilities and base class for ChEMBL pipelines.

This module provides common functionality for all ChEMBL-based pipelines,
including configuration resolution, API client management, pagination handling,
and data extraction utilities.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.config.models.source import SourceConfig
from bioetl.core import APIClientFactory
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

from .base import PipelineBase


@dataclass(slots=True)
class ChemblExtractionContext:
    """Holds runtime state for a descriptor-driven extraction run."""

    source_config: Any
    iterator: Any
    chembl_client: Any | None = None
    select_fields: Sequence[str] | None = None
    page_size: int | None = None
    chembl_release: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    extra_filters: dict[str, Any] = field(default_factory=dict)
    iterate_all_kwargs: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ChemblExtractionDescriptor:
    """Descriptor describing how to execute a ``run_extract_all`` operation."""

    name: str
    source_name: str
    source_config_factory: Callable[[SourceConfig], Any]
    build_context: Callable[["ChemblPipelineBase", Any, BoundLogger], ChemblExtractionContext]
    id_column: str
    summary_event: str
    must_have_fields: Sequence[str] = ()
    default_select_fields: Sequence[str] | None = None
    record_transform: Callable[["ChemblPipelineBase", Mapping[str, Any], ChemblExtractionContext], Mapping[str, Any]] | None = None
    post_processors: Sequence[
        Callable[["ChemblPipelineBase", pd.DataFrame, ChemblExtractionContext, BoundLogger], pd.DataFrame]
    ] = ()
    sort_by: Sequence[str] | str | None = None
    empty_frame_factory: Callable[["ChemblPipelineBase", ChemblExtractionContext], pd.DataFrame] | None = None
    dry_run_handler: Callable[["ChemblPipelineBase", ChemblExtractionContext, BoundLogger, float], pd.DataFrame] | None = None
    hard_page_size_cap: int | None = 25
    summary_extra: Callable[["ChemblPipelineBase", pd.DataFrame, ChemblExtractionContext], Mapping[str, Any]] | None = None

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
    def _stringify_mapping(mapping: Mapping[object, Any]) -> dict[str, Any]:
        """Return mapping with stringified keys preserving values."""

        return {str(key): value for key, value in mapping.items()}

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
            return ChemblPipelineBase._stringify_mapping(mapping)

        model_dump = getattr(parameters, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, Mapping):
                mapping = cast(Mapping[object, Any], dumped)
                return ChemblPipelineBase._stringify_mapping(mapping)

        as_dict = getattr(parameters, "dict", None)
        if callable(as_dict):
            dumped = as_dict()
            if isinstance(dumped, Mapping):
                mapping = cast(Mapping[object, Any], dumped)
                return ChemblPipelineBase._stringify_mapping(mapping)

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

    def _extract_from_input_file(
        self,
        log: BoundLogger,
        *,
        event_name: str,
    ) -> pd.DataFrame | None:
        """Extract records using IDs loaded from the configured input file."""

        if not self.config.cli.input_file:
            return None

        id_column_name = self._get_id_column_name()
        ids = self._read_input_ids(
            id_column_name=id_column_name,
            limit=self.config.cli.limit,
            sample=self.config.cli.sample,
        )
        if not ids:
            return None

        log.info(event_name, mode="batch", ids_count=len(ids))
        return self.extract_by_ids(ids)

    def run_extract_all(self, descriptor: ChemblExtractionDescriptor) -> pd.DataFrame:
        """Execute a descriptor-driven extraction loop with uniform metadata."""

        log = UnifiedLogger.get(__name__).bind(component=f"{self.pipeline_code}.extract")
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config(descriptor.source_name)
        source_config = descriptor.source_config_factory(source_raw)

        context = descriptor.build_context(self, source_config, log)
        context.source_config = source_config

        limit = self.config.cli.limit

        select_fields_list: list[str] | None = None
        if context.select_fields is not None:
            select_fields_list = list(context.select_fields)
        elif descriptor.default_select_fields is not None:
            select_fields_list = list(descriptor.default_select_fields)

        if descriptor.must_have_fields:
            must_fields = list(descriptor.must_have_fields)
            if select_fields_list is None:
                select_fields_list = must_fields
            else:
                select_fields_list = list(dict.fromkeys([*select_fields_list, *must_fields]))

        context.select_fields = select_fields_list

        batch_size_candidate: int | None = getattr(source_config, "batch_size", None)
        if batch_size_candidate is None:
            batch_size_candidate = getattr(source_config, "page_size", None)
        if batch_size_candidate is None:
            batch_size_candidate = self._resolve_batch_size(source_raw)

        hard_cap = descriptor.hard_page_size_cap
        if hard_cap is None and batch_size_candidate is not None:
            hard_cap = max(int(batch_size_candidate), 1)

        page_size = context.page_size
        if page_size is None:
            base_size = batch_size_candidate if batch_size_candidate is not None else 25
            cap = hard_cap if hard_cap is not None else base_size
            page_size = self._resolve_page_size(base_size, limit, hard_cap=cap)
        context.page_size = page_size

        parameters = getattr(source_config, "parameters", {})
        normalised_parameters = self._normalize_parameters(parameters)

        filters_payload: dict[str, Any] = {
            "mode": "all",
            "limit": int(limit) if limit is not None else None,
            "page_size": page_size,
            "select_fields": list(select_fields_list) if select_fields_list else None,
        }
        if context.extra_filters:
            filters_payload.update(context.extra_filters)
        if normalised_parameters:
            filters_payload["parameters"] = normalised_parameters

        compact_filters = {key: value for key, value in filters_payload.items() if value is not None}

        metadata_kwargs = dict(context.metadata)
        self.record_extract_metadata(
            chembl_release=context.chembl_release,
            filters=compact_filters,
            requested_at_utc=datetime.now(timezone.utc),
            **metadata_kwargs,
        )

        if self.config.cli.dry_run:
            if descriptor.dry_run_handler is not None:
                return descriptor.dry_run_handler(self, context, log, stage_start)

            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            if descriptor.empty_frame_factory is not None:
                dataframe = descriptor.empty_frame_factory(self, context)
            else:
                dataframe = pd.DataFrame()

            summary_payload: dict[str, Any] = {
                "rows": int(dataframe.shape[0]),
                "duration_ms": duration_ms,
                "dry_run": True,
            }
            if context.chembl_release is not None:
                summary_payload["chembl_release"] = context.chembl_release
            if descriptor.summary_extra is not None:
                summary_payload.update(descriptor.summary_extra(self, dataframe, context))
            if context.stats:
                summary_payload.update(context.stats)
            log.info(descriptor.summary_event, **summary_payload)
            return dataframe

        iterator_kwargs = dict(context.iterate_all_kwargs)
        records: list[dict[str, Any]] = []

        for payload in context.iterator.iterate_all(
            limit=limit,
            page_size=page_size,
            select_fields=select_fields_list,
            **iterator_kwargs,
        ):
            if descriptor.record_transform is not None:
                record_mapping = descriptor.record_transform(self, payload, context)
            else:
                record_mapping = self._coerce_mapping(payload)
            records.append(dict(record_mapping))

        if records:
            dataframe = pd.DataFrame.from_records(records)
        elif descriptor.empty_frame_factory is not None:
            dataframe = descriptor.empty_frame_factory(self, context)
        else:
            dataframe = pd.DataFrame({descriptor.id_column: pd.Series(dtype="object")})

        if descriptor.sort_by and not dataframe.empty:
            sort_columns = (
                list(descriptor.sort_by)
                if isinstance(descriptor.sort_by, Sequence) and not isinstance(descriptor.sort_by, (str, bytes))
                else [cast(str, descriptor.sort_by)]
            )
            dataframe = dataframe.sort_values(sort_columns).reset_index(drop=True)

        for processor in descriptor.post_processors:
            dataframe = processor(self, dataframe, context, log)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0

        summary_payload: dict[str, Any] = {
            "rows": int(dataframe.shape[0]),
            "duration_ms": duration_ms,
        }
        if context.chembl_release is not None:
            summary_payload["chembl_release"] = context.chembl_release
        if context.stats:
            summary_payload.update(context.stats)
        if descriptor.summary_extra is not None:
            summary_payload.update(descriptor.summary_extra(self, dataframe, context))

        log.info(descriptor.summary_event, **summary_payload)
        return dataframe

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

        # Check if client is ChemblClient by checking for handshake method
        handshake_candidate = getattr(client, "handshake", None)
        if callable(handshake_candidate):
            handshake = cast(Callable[[str], Any], handshake_candidate)
            request_timestamp = datetime.now(timezone.utc)
            try:
                status = handshake("/status")
                if isinstance(status, Mapping):
                    status_mapping = cast(Mapping[str, Any], status)
                    candidate = status_mapping.get("chembl_db_version") or status_mapping.get("chembl_release")
                    if isinstance(candidate, str):
                        release_value = candidate
                        log.info(f"{self.pipeline_code}.status", chembl_release=release_value)
            except Exception as exc:
                log.warning(f"{self.pipeline_code}.status_failed", error=str(exc))
            finally:
                self.record_extract_metadata(
                    chembl_release=release_value,
                    requested_at_utc=request_timestamp,
                )
            return release_value

        # Use direct HTTP for UnifiedAPIClient
        get_candidate = getattr(client, "get", None)
        if callable(get_candidate):
            client_get = cast(Callable[..., Any], get_candidate)
            request_timestamp = datetime.now(timezone.utc)
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
                self.record_extract_metadata(
                    chembl_release=release_value,
                    requested_at_utc=request_timestamp,
                )
            return release_value
        self.record_extract_metadata(requested_at_utc=datetime.now(timezone.utc))
        return None

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient | Any,  # pyright: ignore[reportAny]
        log: BoundLogger | None = None,
    ) -> str | None:
        """Backward compatible wrapper for tests expecting private method."""

        return self.fetch_chembl_release(client, log)

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
            mapping = cast(Mapping[object, Any], payload)
            return ChemblPipelineBase._stringify_mapping(mapping)
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

        for key in items_keys:
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                candidates: list[dict[str, Any]] = []
                sequence_items = cast(Sequence[object], value)
                for item in sequence_items:
                    if isinstance(item, Mapping):
                        mapping = cast(Mapping[object, Any], item)
                        candidates.append(ChemblPipelineBase._stringify_mapping(mapping))
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
                        candidates.append(ChemblPipelineBase._stringify_mapping(mapping))
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
