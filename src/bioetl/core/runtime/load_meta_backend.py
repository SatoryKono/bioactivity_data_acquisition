from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from bioetl.core.runtime.load_meta_store import LoadMetaStore


class LoadMetaBackend(Protocol):
    def begin_record(
        self,
        source_system: str,
        request_base_url: str,
        request_params: Mapping[str, Any] | str,
        *,
        source_release: str | None = None,
        source_api_version: str | None = None,
        job_id: str | None = None,
        operator: str | None = None,
        notes: str | None = None,
    ) -> str: ...

    def update_pagination(
        self,
        load_meta_id: str,
        pagination_payload: Mapping[str, Any]
        | Iterable[Mapping[str, Any]],
        *,
        records_fetched_delta: int | None = None,
    ) -> None: ...

    def finish_record(
        self,
        load_meta_id: str,
        *,
        status: str,
        records_fetched: int,
        error_message: str | None = None,
        retry_count_delta: int = 0,
        notes: str | None = None,
    ) -> None: ...


__all__ = ["LoadMetaBackend", "LoadMetaStore"]
