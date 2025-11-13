"""Base classes for ChEMBL entity fetching."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Protocol

import pandas as pd

from bioetl.clients.chembl_config import EntityConfig
from bioetl.core.logging import UnifiedLogger

__all__ = [
    "ChemblClientProtocol",
    "ChemblEntityClientProtocol",
    "ChemblEntityFetcherBase",
    "EntityConfig",
]


class ChemblClientProtocol(Protocol):
    """Minimal contract for a ChEMBL client consumed by entity fetchers."""

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Iterate over paginated responses from the ChEMBL API."""
        ...


class ChemblEntityClientProtocol(Protocol):
    """Common protocol implemented by thin ChEMBL entity clients."""

    def fetch_by_ids(
        self,
        ids: Sequence[str],
        fields: Sequence[str] | None = None,
        *,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        """Fetch entity records by identifiers."""
        ...

    def fetch_all(
        self,
        *,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> pd.DataFrame:
        """Fetch all records with optional limit and projection."""
        ...

    def iterate_records(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Iterate over raw entity payloads."""
        ...


class ChemblEntityFetcherBase(ChemblEntityClientProtocol):
    """Base helper for fetching ChEMBL entities returning DataFrame payloads."""

    _DEFAULT_PAGE_SIZE = 1000

    def __init__(self, chembl_client: ChemblClientProtocol, config: EntityConfig) -> None:
        """Initialize the fetcher for a ChEMBL entity."""
        self._chembl_client: ChemblClientProtocol = chembl_client
        self._config = config
        self._log = UnifiedLogger.get(__name__).bind(
            component="chembl_entity",
            entity=config.log_prefix,
        )

    def fetch_by_ids(
        self,
        ids: Sequence[str],
        fields: Sequence[str] | None = None,
        *,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        identifiers = self._validate_identifiers(ids)
        if not identifiers:
            self._log.debug(f"{self._config.log_prefix}.fetch_by_ids.no_ids")
            return self._empty_frame(fields)

        page_size = self._resolve_page_size(page_limit, None)
        records: list[Mapping[str, Any]] = []
        for chunk in self._iter_chunks(identifiers):
            params = self._build_chunk_params(chunk, fields=fields)
            chunk_records = list(
                self.iterate_records(
                    params=params,
                    fields=fields,
                    page_size=page_size,
                )
            )
            records.extend(chunk_records)

        frame = self._records_to_frame(records, fields)
        self._log.info(
            f"{self._config.log_prefix}.fetch_by_ids.complete",
            ids_requested=len(identifiers),
            rows=len(frame),
        )
        return frame

    def fetch_all(
        self,
        *,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> pd.DataFrame:
        effective_page_size = self._resolve_page_size(page_size, limit)
        records = list(
            self.iterate_records(
                limit=limit,
                fields=fields,
                page_size=effective_page_size,
            )
        )
        frame = self._records_to_frame(records, fields)
        self._log.info(
            f"{self._config.log_prefix}.fetch_all.complete",
            rows=len(frame),
            limit=limit,
        )
        return frame

    def iterate_records(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        resolved_page_size = self._resolve_page_size(page_size, limit)
        request_params = self._compose_params(
            params=params,
            fields=fields,
            page_size=resolved_page_size,
        )
        yielded = 0
        for record in self._chembl_client.paginate(
            self._config.endpoint,
            params=request_params,
            page_size=resolved_page_size,
            items_key=self._config.items_key,
        ):
            yield dict(record)
            yielded += 1
            if limit is not None and yielded >= limit:
                break

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _validate_identifiers(self, ids: Sequence[str]) -> list[str]:
        if not isinstance(ids, Sequence):
            msg = "ids must be a sequence of strings"
            raise TypeError(msg)
        identifiers: list[str] = []
        for idx, identifier in enumerate(ids):
            if not isinstance(identifier, str):
                msg = f"identifier at position {idx} must be str, got {type(identifier)!r}"
                raise TypeError(msg)
            if identifier:
                identifiers.append(identifier)
        return identifiers

    def _iter_chunks(self, identifiers: Sequence[str]) -> Iterator[Sequence[str]]:
        chunk_size = self._config.chunk_size
        for offset in range(0, len(identifiers), chunk_size):
            yield identifiers[offset : offset + chunk_size]

    def _compose_params(
        self,
        *,
        params: Mapping[str, Any] | None,
        fields: Sequence[str] | None,
        page_size: int,
    ) -> dict[str, Any]:
        request_params: dict[str, Any] = dict(self._config.filters)
        if params:
            request_params.update(params)
        if fields:
            request_params["only"] = ",".join(fields)
        request_params.setdefault("limit", page_size)
        return request_params

    def _resolve_page_size(
        self,
        requested: int | None,
        limit: int | None,
    ) -> int:
        candidate = requested or self._config.max_page_size or self._DEFAULT_PAGE_SIZE
        if candidate <= 0:
            candidate = self._DEFAULT_PAGE_SIZE
        if limit is not None:
            candidate = min(candidate, max(limit, 1))
        max_page_size = self._config.max_page_size
        if max_page_size is not None:
            candidate = min(candidate, max_page_size)
        return candidate

    def _records_to_frame(
        self,
        records: Sequence[Mapping[str, Any]],
        fields: Sequence[str] | None,
    ) -> pd.DataFrame:
        if not records:
            return self._empty_frame(fields)

        frame = pd.DataFrame.from_records(records)
        ordered_columns = self._resolve_column_order(frame.columns.tolist(), fields)
        frame = frame.reindex(columns=ordered_columns)
        if self._config.ordering:
            sort_columns = [
                column for column in self._config.ordering if column in frame.columns
            ]
            if sort_columns:
                frame = frame.sort_values(by=sort_columns, kind="mergesort")
        return frame.reset_index(drop=True)

    def _resolve_column_order(
        self,
        columns: Sequence[str],
        fields: Sequence[str] | None,
    ) -> list[str]:
        ordered: list[str] = []

        def _extend(values: Sequence[str]) -> None:
            for value in values:
                if value not in ordered:
                    ordered.append(value)

        if fields:
            _extend(fields)
        elif self._config.default_fields:
            _extend(self._config.default_fields)

        if self._config.ordering:
            _extend(self._config.ordering)

        _extend(columns)
        return ordered

    def _empty_frame(self, fields: Sequence[str] | None) -> pd.DataFrame:
        column_order = list(fields) if fields else list(self._config.default_fields)
        if not column_order:
            column_order = list(self._config.ordering)
        return pd.DataFrame(columns=column_order)

    def _build_chunk_params(
        self,
        chunk: Sequence[str],
        *,
        fields: Sequence[str] | None,
    ) -> dict[str, Any]:
        return {self._config.filter_param: ",".join(chunk)}

