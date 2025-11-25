"""Пример тонкого клиента активности ChEMBL на базе BaseEntityFetcher."""

from __future__ import annotations

from collections.abc import Generator, Mapping
from typing import Any
import logging

from bioetl.clients.common import BaseEntityFetcher

__all__ = ["ChemblActivityClient"]


class ChemblActivityClient(BaseEntityFetcher):
    """Лёгкий клиент, сохраняющий прежний публичный API."""

    def __init__(
        self,
        base_url: str,
        *,
        session=None,
        default_chunk_size: int | None = 200,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(
            session=session,
            default_chunk_size=default_chunk_size,
            logger=logger,
        )
        self._base_url = base_url.rstrip("/")

    def _build_url(self, *parts: str) -> str:
        normalized_parts = [
            self._base_url,
            *[part.strip("/") for part in parts],
        ]
        return "/".join(normalized_parts)

    def list_activities(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        chunk_size: int | None = None,
    ) -> Generator[Mapping[str, Any], None, None]:
        """Вернуть генератор активностей с чанковой пагинацией."""

        return self.chunked_fetch(
            self._build_url("activity"), params=params, chunk_size=chunk_size
        )

    def get_by_id(
        self, activity_id: str, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Получить единичную запись по идентификатору."""

        payload = self.fetch_page(
            self._build_url("activity", str(activity_id)), params or {}
        )
        if isinstance(payload, Mapping):
            return payload
        return {"data": payload}

    def fetch_all_dataframe(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        field_mapping: Mapping[str, str] | None = None,
        dtype_map: Mapping[str, Any] | None = None,
        chunk_size: int | None = None,
    ):
        """Сохранить совместимость с прежним методом DataFrame-выгрузки."""

        records = list(
            self.list_activities(params=params, chunk_size=chunk_size)
        )
        return self._records_to_frame(records, field_mapping, dtype_map)

    def _records_to_frame(
        self,
        records: list[Mapping[str, Any]],
        field_mapping: Mapping[str, str] | None = None,
        dtype_map: Mapping[str, Any] | None = None,
    ):
        mapping = field_mapping
        if mapping is None:
            mapping = (
                {key: key for key in records[0].keys()} if records else {}
            )
        return self.records_to_dataframe(records, mapping, dtype_map=dtype_map)
