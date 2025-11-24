"""Общий базовый клиент для сущностей."""

from __future__ import annotations

from collections.abc import Generator, Iterable, Mapping, Sequence
from typing import Any, Callable
import logging

import pandas as pd
import requests

from bioetl.clients.helpers import retry_backoff, safe_cast

__all__ = ["BaseEntityFetcher"]


class BaseEntityFetcher:
    """Унифицированный клиент с поддержкой чанковой выборки и сборки DataFrame."""

    def __init__(
        self,
        session: requests.Session | None = None,
        *,
        default_chunk_size: int | None = 200,
        logger: logging.Logger | None = None,
    ) -> None:
        self._session = session or requests.Session()
        self._default_chunk_size = default_chunk_size
        self._log = logger or logging.getLogger(self.__class__.__name__)

    @retry_backoff((requests.RequestException,))
    def fetch_page(
        self, url: str, params: Mapping[str, Any] | None = None
    ) -> Any:
        """Выполнить запрос страницы и вернуть JSON-пayload."""

        response = self._session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _extract_records(self, payload: Any) -> list[Mapping[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, Sequence) and not isinstance(
            payload, (str, bytes, bytearray)
        ):
            return [
                dict(item) for item in payload if isinstance(item, Mapping)
            ]
        if isinstance(payload, Mapping):
            for key in ("items", "results", "data", "records"):
                raw = payload.get(key)
                if isinstance(raw, Sequence) and not isinstance(
                    raw, (str, bytes, bytearray)
                ):
                    return [
                        dict(item) for item in raw if isinstance(item, Mapping)
                    ]
        return []

    def chunked_fetch(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        chunk_size: int | None = None,
    ) -> Generator[Mapping[str, Any], None, None]:
        """Итерироваться по записям, выгружая их странично."""

        effective_chunk = (
            chunk_size if chunk_size is not None else self._default_chunk_size
        )
        offset = 0
        while True:
            page_params: dict[str, Any] = dict(params or {})
            if effective_chunk:
                page_params.setdefault("limit", effective_chunk)
                page_params.setdefault("offset", offset)
            payload = self.fetch_page(url, page_params)
            records = self._extract_records(payload)
            if not records:
                break
            for record in records:
                yield record
            if effective_chunk is None or len(records) < effective_chunk:
                break
            offset += effective_chunk

    def records_to_dataframe(
        self,
        records: Iterable[Mapping[str, Any]],
        field_mapping: Mapping[str, str],
        dtype_map: Mapping[str, Callable[[Any], Any] | type] | None = None,
    ) -> pd.DataFrame:
        """Собрать DataFrame по отображению полей и опциональным приведениям типов."""

        normalized_records = list(records)
        if not normalized_records:
            return pd.DataFrame(columns=list(field_mapping.keys()))

        rows: list[dict[str, Any]] = []
        for record in normalized_records:
            row: dict[str, Any] = {}
            for column, source_key in field_mapping.items():
                row[column] = record.get(source_key)
            rows.append(row)

        frame = pd.DataFrame.from_records(rows)
        if dtype_map:
            for column, caster in dtype_map.items():
                if column in frame.columns:
                    frame[column] = frame[column].apply(
                        lambda value: safe_cast(value, caster)
                    )
        return frame
