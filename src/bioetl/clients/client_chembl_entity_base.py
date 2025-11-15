"Base classes for ChEMBL entity fetching."

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, ClassVar, Protocol
from urllib.parse import urlencode

import pandas as pd

from bioetl.clients.base import normalize_select_fields
from bioetl.clients.chembl_config import EntityConfig
from bioetl.core.common import ChemblReleaseMixin
from bioetl.core.logging import UnifiedLogger

__all__ = [
    "ChemblClientProtocol",
    "ChemblEntityClientProtocol",
    "ChemblEntityConfigMixin",
    "ChemblEntityFetcherBase",
    "EntityConfig",
]


class ChemblClientProtocol(Protocol):
    """Минимальный контракт HTTP-клиента, обслуживающего ChEMBL entity-уровень."""

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        ...

    def handshake(self, endpoint: str | None = None) -> Mapping[str, Any]:
        ...


class ChemblEntityClientProtocol(Protocol):
    """Контракт тонких клиентов ChEMBL-сущностей."""

    def fetch_by_ids(
        self,
        ids: Sequence[str],
        fields: Sequence[str] | None = None,
        *,
        page_limit: int | None = None,
    ) -> pd.DataFrame:
        ...

    def fetch_all(
        self,
        *,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> pd.DataFrame:
        ...

    def iterate_records(
        self,
        *,
        params: Mapping[str, Any] | None = None,
        limit: int | None = None,
        fields: Sequence[str] | None = None,
        page_size: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        ...


class ChemblEntityConfigMixin:
    """Mixin, предоставляющий единообразную инициализацию для ChEMBL-клиентов."""

    ENTITY_CONFIG: ClassVar[EntityConfig | None] = None
    DEFAULT_BATCH_SIZE: ClassVar[int | None] = None
    DEFAULT_MAX_URL_LENGTH: ClassVar[int | None] = None
    REQUIRE_MAX_URL_LENGTH: ClassVar[bool] = False

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
        *,
        entity_config: EntityConfig | None = None,
        batch_size: int | None = None,
        max_url_length: int | None = None,
    ) -> None:
        config = entity_config or self.ENTITY_CONFIG
        if config is None:
            msg = (
                "entity_config должен быть передан явным параметром "
                "или определён как ENTITY_CONFIG в классе"
            )
            raise ValueError(msg)

        resolved_batch_size = batch_size
        if resolved_batch_size is None:
            resolved_batch_size = self.DEFAULT_BATCH_SIZE
        resolved_batch_size = self._normalize_batch_size(resolved_batch_size)

        resolved_max_url_length = max_url_length
        if resolved_max_url_length is None:
            resolved_max_url_length = self.DEFAULT_MAX_URL_LENGTH
        resolved_max_url_length = self._normalize_max_url_length(
            resolved_max_url_length
        )

        if self.REQUIRE_MAX_URL_LENGTH and resolved_max_url_length is None:
            msg = "max_url_length обязателен для данного клиента"
            raise ValueError(msg)

        ChemblEntityFetcherBase.__init__(  # avoids mypy complaints in mixin
            self,
            chembl_client=chembl_client,
            config=config,
            batch_size=resolved_batch_size,
            max_url_length=resolved_max_url_length,
        )

    def _normalize_batch_size(self, batch_size: int | None) -> int | None:
        return batch_size

    def _normalize_max_url_length(self, max_url_length: int | None) -> int | None:
        return max_url_length


class ChemblEntityFetcherBase(ChemblReleaseMixin, ChemblEntityClientProtocol):
    """Универсальный DataFrame-клиент ChEMBL с единым поведением."""

    _DEFAULT_PAGE_SIZE = 1000

    @classmethod
    def _init_from_entity_config(
        cls,
        instance: "ChemblEntityFetcherBase",
        chembl_client: ChemblClientProtocol,
        *,
        entity_config: EntityConfig,
        batch_size: int | None = None,
        max_url_length: int | None = None,
    ) -> None:
        if not isinstance(instance, ChemblEntityFetcherBase):
            msg = "instance must inherit from ChemblEntityFetcherBase"
            raise TypeError(msg)
        ChemblEntityFetcherBase.__init__(
            instance,
            chembl_client=chembl_client,
            config=entity_config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
        config: EntityConfig,
        *,
        batch_size: int | None = None,
        max_url_length: int | None = None,
    ) -> None:
        super().__init__()
        if batch_size is not None and batch_size <= 0:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)
        if max_url_length is not None and max_url_length <= 0:
            msg = "max_url_length must be a positive integer"
            raise ValueError(msg)

        self._chembl_client: ChemblClientProtocol = chembl_client
        self._config = config
        self._batch_size = batch_size or config.chunk_size
        self._chunk_limit = max(1, min(config.chunk_size, self._batch_size))
        self._max_url_length = max_url_length
        self._log = UnifiedLogger.get(__name__).bind(
            component="chembl_entity",
            entity=config.log_prefix,
        )

    # ------------------------------------------------------------------ #
    # Публичный API                                                     #
    # ------------------------------------------------------------------ #

    @property
    def chembl_client(self) -> ChemblClientProtocol:
        return self._chembl_client

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def max_url_length(self) -> int | None:
        return self._max_url_length

    def handshake(
        self,
        *,
        endpoint: str | None = None,
        enabled: bool = True,
    ) -> Mapping[str, Any]:
        if not enabled:
            self._log.info(
                f"{self._config.log_prefix}.handshake.skipped",
                handshake_endpoint=endpoint,
                handshake_enabled=enabled,
            )
            return {}

        payload = self._chembl_client.handshake(endpoint)
        release = payload.get("chembl_db_version") or payload.get("chembl_release")
        if isinstance(release, str):
            self._set_chembl_release(release)
        self._log.info(
            f"{self._config.log_prefix}.handshake",
            handshake_endpoint=endpoint,
            handshake_enabled=enabled,
            chembl_release=self.chembl_release,
        )
        return payload

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

        projection = normalize_select_fields(fields)
        page_size = self._resolve_page_size(page_limit, None)
        records: list[Mapping[str, Any]] = []
        for chunk in self._chunk_identifiers(identifiers, select_fields=projection):
            params = self._build_chunk_params(chunk, fields=projection)
            chunk_records = list(
                self.iterate_records(
                    params=params,
                    fields=projection,
                    page_size=page_size,
                )
            )
            records.extend(chunk_records)

        frame = self._records_to_frame(records, projection)
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
        projection = normalize_select_fields(fields)
        effective_page_size = self._resolve_page_size(page_size, limit)
        records = list(
            self.iterate_records(
                limit=limit,
                fields=projection,
                page_size=effective_page_size,
            )
        )
        frame = self._records_to_frame(records, projection)
        self._log.info(
            f"{self._config.log_prefix}.fetch_all.complete",
            rows=len(frame),
            limit=limit,
        )
        return frame

    def iterate_all(
        self,
        *,
        limit: int | None = None,
        page_size: int | None = None,
        select_fields: Sequence[str] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        projection = normalize_select_fields(select_fields)
        effective_page_size = self._resolve_page_size(page_size, limit)
        yielded = 0
        for record in self.iterate_records(
            limit=limit,
            fields=projection,
            page_size=effective_page_size,
        ):
            yield record
            yielded += 1
            if limit is not None and yielded >= limit:
                break

    def iterate_by_ids(
        self,
        ids: Sequence[str],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        identifiers = self._validate_identifiers(ids)
        if not identifiers:
            return

        projection = normalize_select_fields(select_fields)
        page_size = self._resolve_page_size(None, None)
        for chunk in self._chunk_identifiers(identifiers, select_fields=projection):
            params = self._build_chunk_params(chunk, fields=projection)
            yield from self.iterate_records(
                params=params,
                fields=projection,
                page_size=page_size,
            )

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
    # Вспомогательные методы                                            #
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
            normalized = identifier.strip()
            if normalized:
                identifiers.append(normalized)
        return identifiers

    def _chunk_identifiers(
        self,
        ids: Sequence[str],
        *,
        select_fields: Sequence[str] | None,
    ) -> Iterable[Sequence[str]]:
        chunk: deque[str] = deque()
        for identifier in ids:
            candidate_size = len(chunk) + 1
            if self._should_flush_chunk(
                chunk,
                candidate_size=candidate_size,
                next_identifier=identifier,
                select_fields=select_fields,
            ):
                yield tuple(chunk)
                chunk.clear()
            chunk.append(identifier)
            if len(chunk) >= self._chunk_limit:
                yield tuple(chunk)
                chunk.clear()
        if chunk:
            yield tuple(chunk)

    def _should_flush_chunk(
        self,
        chunk: deque[str],
        *,
        candidate_size: int,
        next_identifier: str,
        select_fields: Sequence[str] | None,
    ) -> bool:
        if not chunk:
            return False
        if candidate_size > self._chunk_limit:
            return True
        if not self._config.enable_url_length_check:
            return False
        if self._max_url_length is None:
            return False

        encoded_length = self._encode_in_query(
            tuple(list(chunk) + [next_identifier]),
            select_fields=select_fields,
        )
        return encoded_length > self._max_url_length

    def _encode_in_query(
        self,
        identifiers: Sequence[str],
        *,
        select_fields: Sequence[str] | None,
    ) -> int:
        params_dict: dict[str, str] = {self._config.filter_param: ",".join(identifiers)}
        if select_fields:
            params_dict["only"] = ",".join(sorted(select_fields))
        encoded = urlencode(params_dict)
        base_length = self._config.base_endpoint_length or len(self._config.endpoint)
        return base_length + 1 + len(encoded)

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
        candidate = requested or self._config.max_page_size or self._batch_size or self._DEFAULT_PAGE_SIZE
        if candidate <= 0:
            candidate = self._DEFAULT_PAGE_SIZE
        if limit is not None:
            candidate = min(candidate, max(limit, 1))
        candidate = min(candidate, self._batch_size)
        max_page_size = self._config.max_page_size
        if max_page_size is not None:
            candidate = min(candidate, max_page_size)
        return max(candidate, 1)

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
            return ordered

        if self._config.default_fields:
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
        payload = {self._config.filter_param: ",".join(chunk)}
        if fields:
            payload["only"] = ",".join(fields)
        return payload
 
