"""Storage facade for persisting chembl_metadata_schema lineage events."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from infrastructure.logging import LogEvents, UnifiedLogger
from infrastructure.runtime.load_meta_builder import LoadMetaBuilder, _ActiveRecord
from infrastructure.runtime.load_meta_writer import LoadMetaWriter

__all__ = ["LoadMetaStore"]


class LoadMetaStore:
    """Manage lifecycle of chembl_metadata_schema entries with deterministic persistence."""

    def __init__(
        self,
        base_path: str | Path,
        *,
        dataset_format: str = "parquet",
        builder: LoadMetaBuilder | None = None,
        writer: LoadMetaWriter | None = None,
    ) -> None:
        self._base_path = Path(base_path).resolve()
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._meta_dir = self._base_path / "load_meta"
        self._meta_dir.mkdir(parents=True, exist_ok=True)
        self._logger = UnifiedLogger.get(__name__).bind(component="load_meta_store")
        self._builder = builder or LoadMetaBuilder()
        self._writer = writer or LoadMetaWriter(
            dataset_format=dataset_format, logger=self._logger
        )
        self._active: dict[str, _ActiveRecord] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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
    ) -> str:
        """Create a new active chembl_metadata_schema record and return its identifier."""

        load_meta_id = str(uuid4())
        record = self._builder.begin_record(
            load_meta_id,
            source_system=source_system,
            request_base_url=request_base_url,
            request_params=request_params,
            source_release=source_release,
            source_api_version=source_api_version,
            job_id=job_id,
            operator=operator,
            notes=notes,
        )
        self._active[load_meta_id] = record
        self._logger.info(
            LogEvents.LOAD_META_BEGIN,
            load_meta_id=load_meta_id,
            source_system=record.source_system,
            request_base_url=record.request_base_url,
        )
        return load_meta_id

    def update_pagination(
        self,
        load_meta_id: str,
        pagination_payload: Mapping[str, Any] | Iterable[Mapping[str, Any]],
        *,
        records_fetched_delta: int | None = None,
    ) -> None:
        """Append pagination metadata for ``load_meta_id``."""

        record = self._require_active(load_meta_id)
        pages = self._builder.update_pagination(
            record, pagination_payload, records_fetched_delta=records_fetched_delta
        )
        self._logger.info(
            LogEvents.LOAD_META_PAGE,
            load_meta_id=load_meta_id,
            pages=pages,
        )

    def finish_record(
        self,
        load_meta_id: str,
        *,
        status: str,
        records_fetched: int,
        error_message: str | None = None,
        retry_count_delta: int = 0,
        notes: str | None = None,
        request_finished_at: datetime | None = None,
        ingested_at: datetime | None = None,
    ) -> None:
        """Finalize ``load_meta_id`` and persist it to storage."""

        record = self._require_active(load_meta_id)
        frame = self._builder.finish_record(
            record,
            status=status,
            records_fetched=records_fetched,
            error_message=error_message,
            retry_count_delta=retry_count_delta,
            notes=notes,
            request_finished_at=request_finished_at,
            ingested_at=ingested_at,
        )
        self._writer.write(frame, self._meta_dir / f"{load_meta_id}.parquet")
        self._logger.info(
            LogEvents.LOAD_META_FINISH,
            load_meta_id=load_meta_id,
            status=status,
            records_fetched=records_fetched,
        )
        del self._active[load_meta_id]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_active(self, load_meta_id: str) -> _ActiveRecord:
        try:
            return self._active[load_meta_id]
        except KeyError as exc:  # pragma: no cover - defensive guard
            msg = f"Load meta id '{load_meta_id}' is not active"
            raise KeyError(msg) from exc
