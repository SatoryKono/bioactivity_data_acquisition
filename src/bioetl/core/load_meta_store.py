"""Storage helper for persisting chembl_metadata_schema lineage events."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pandas as pd
import pandera as pa

from bioetl.core.hashing import hash_from_mapping
from bioetl.core.log_events import LogEvents
from bioetl.schemas.chembl_metadata_schema import (
    BUSINESS_KEY_FIELDS,
    COLUMN_ORDER,
    ROW_HASH_FIELDS,
    LoadMetaSchema,
)

from .logger import UnifiedLogger

__all__ = ["LoadMetaStore"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_json(payload: Any) -> str:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )


def _normalize_base_url(value: Any) -> str:
    text = str(value)
    if text.startswith("http://") or text.startswith("https://"):
        return text
    stripped = text.strip().strip("<>")
    if not stripped:
        stripped = "unknown"
    return f"https://mock.invalid/{stripped}"


@dataclass(slots=True)
class _ActiveRecord:
    load_meta_id: str
    source_system: str
    request_base_url: str
    request_params_json: str
    request_started_at: datetime
    request_finished_at: datetime
    ingested_at: datetime
    source_release: str | None = None
    source_api_version: str | None = None
    records_fetched: int = 0
    status: str = "success"
    error_message_opt: str | None = None
    retry_count: int = 0
    job_id: str | None = None
    operator: str | None = None
    notes: str | None = None
    pagination_events: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        pagination_meta = None
        if self.pagination_events:
            pagination_meta = _canonical_json(self.pagination_events)
        return {
            "load_meta_id": self.load_meta_id,
            "source_system": self.source_system,
            "source_release": self.source_release,
            "source_api_version": self.source_api_version,
            "request_base_url": self.request_base_url,
            "request_params_json": self.request_params_json,
            "pagination_meta": pagination_meta,
            "request_started_at": self.request_started_at,
            "request_finished_at": self.request_finished_at,
            "ingested_at": self.ingested_at,
            "records_fetched": self.records_fetched,
            "status": self.status,
            "error_message_opt": self.error_message_opt,
            "retry_count": self.retry_count,
            "job_id": self.job_id,
            "operator": self.operator,
            "notes": self.notes,
        }


class LoadMetaStore:
    """Manage lifecycle of chembl_metadata_schema entries with deterministic persistence."""

    def __init__(self, base_path: str | Path, *, dataset_format: str = "parquet") -> None:
        if dataset_format not in {"parquet", "delta"}:
            msg = f"Unsupported dataset format: {dataset_format}"
            raise ValueError(msg)
        self._base_path = Path(base_path).resolve()
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._meta_dir = self._base_path / "load_meta"
        self._meta_dir.mkdir(parents=True, exist_ok=True)
        self._dataset_format = dataset_format
        self._logger = UnifiedLogger.get(__name__).bind(component="load_meta_store")
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
        base_url = _normalize_base_url(request_base_url)
        if isinstance(request_params, str):
            params_json = request_params
        else:
            params_json = _canonical_json(request_params)
        now = _utcnow()
        record = _ActiveRecord(
            load_meta_id=load_meta_id,
            source_system=source_system,
            request_base_url=base_url,
            request_params_json=params_json,
            request_started_at=now,
            request_finished_at=now,
            ingested_at=now,
            source_release=source_release,
            source_api_version=source_api_version,
            job_id=job_id,
            operator=operator,
            notes=notes,
        )
        self._active[load_meta_id] = record
        self._logger.info(LogEvents.LOAD_META_BEGIN,
            load_meta_id=load_meta_id,
            source_system=source_system,
            request_base_url=base_url,
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
        events: list[dict[str, Any]]
        if isinstance(pagination_payload, Mapping):
            mapped = cast(Mapping[str, Any], pagination_payload)
            events = [dict(mapped.items())]
        else:
            events = [dict(payload.items()) for payload in pagination_payload]
        record.pagination_events.extend(events)
        if records_fetched_delta is not None:
            record.records_fetched += records_fetched_delta
        record.request_finished_at = _utcnow()
        self._logger.info(LogEvents.LOAD_META_PAGE,
            load_meta_id=load_meta_id,
            pages=len(events),
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
        record.status = status
        record.records_fetched = records_fetched
        record.error_message_opt = error_message
        record.retry_count += max(retry_count_delta, 0)
        record.request_finished_at = request_finished_at or _utcnow()
        record.ingested_at = ingested_at or _utcnow()
        if notes:
            record.notes = notes if record.notes is None else f"{record.notes}; {notes}"

        payload = record.to_payload()
        payload["hash_business_key"] = hash_from_mapping(payload, BUSINESS_KEY_FIELDS)
        payload["hash_row"] = hash_from_mapping(payload, ROW_HASH_FIELDS)

        df = pd.DataFrame([payload], columns=COLUMN_ORDER)
        LoadMetaSchema.validate(df, lazy=True)
        self._write_dataframe(df, self._meta_dir / f"{load_meta_id}.parquet")
        self._logger.info(LogEvents.LOAD_META_FINISH,
            load_meta_id=load_meta_id,
            status=status,
            records_fetched=records_fetched,
        )
        del self._active[load_meta_id]

    def write_dataframe(
        self,
        frame: Any,
        path: str | Path,
        *,
        schema: pa.DataFrameSchema | None = None,
    ) -> None:
        """Validate (optionally) and persist ``frame`` atomically."""

        destination = Path(path)
        if not destination.is_absolute():
            destination = self._base_path / destination
        if schema is not None and isinstance(frame, pd.DataFrame):
            schema.validate(frame, lazy=True)
        self._write_dataframe(frame, destination)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_active(self, load_meta_id: str) -> _ActiveRecord:
        try:
            return self._active[load_meta_id]
        except KeyError as exc:  # pragma: no cover - defensive guard
            msg = f"Load meta id '{load_meta_id}' is not active"
            raise KeyError(msg) from exc

    def _write_dataframe(self, frame: Any, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(frame, pd.DataFrame):
            if self._dataset_format != "parquet":
                msg = "Only parquet format is supported for pandas DataFrames"
                raise RuntimeError(msg)
            suffix = path.suffix or ".parquet"
            with tempfile.NamedTemporaryFile(
                "wb", suffix=suffix, delete=False, dir=path.parent
            ) as handle:
                temp_path = Path(handle.name)
            try:
                frame.to_parquet(temp_path, index=False)
                os.replace(temp_path, path)
            except Exception:  # pragma: no cover - cleanup branch
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
                raise
            return
        _write_spark_dataframe(frame, path, fmt=self._dataset_format)


# Optional Spark support helpers -------------------------------------------------

if TYPE_CHECKING:  # pragma: no cover - typing aid
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore[import-not-found]
else:  # pragma: no cover - optional dependency
    try:
        from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore[import-not-found]
    except Exception:
        SparkDataFrame = None  # type: ignore[assignment]


def _write_spark_dataframe(frame: Any, path: Path, *, fmt: str) -> None:
    if SparkDataFrame is None or not isinstance(frame, SparkDataFrame):
        msg = "Spark DataFrame support is unavailable"
        raise RuntimeError(msg)
    temp_dir = Path(tempfile.mkdtemp(prefix="load_meta_", dir=str(path.parent)))
    try:
        writer = frame.write.mode("overwrite")
        if fmt == "delta":
            writer.format("delta").save(str(temp_dir))
        else:
            writer.parquet(str(temp_dir))
        if path.exists():
            shutil.rmtree(path)
        os.replace(temp_dir, path)
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
