"""Domain builder for chembl_metadata_schema payloads without IO concerns."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, cast

import pandas as pd

from infrastructure.io import hash_from_mapping
from infrastructure.schemas.chembl_metadata_schema import (
    BUSINESS_KEY_FIELDS,
    COLUMN_ORDER,
    ROW_HASH_FIELDS,
    LoadMetaSchema,
)

__all__ = ["LoadMetaBuilder"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _normalise_base_url(value: Any) -> str:
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


class LoadMetaBuilder:
    """Build validated chembl_metadata_schema payloads."""

    def __init__(self, *, now_factory: Callable[[], datetime] | None = None) -> None:
        self._now_factory = now_factory or _utcnow

    # ------------------------------------------------------------------
    # API invoked by LoadMetaStore facade
    # ------------------------------------------------------------------
    def begin_record(
        self,
        load_meta_id: str,
        *,
        source_system: str,
        request_base_url: str,
        request_params: Mapping[str, Any] | str,
        source_release: str | None = None,
        source_api_version: str | None = None,
        job_id: str | None = None,
        operator: str | None = None,
        notes: str | None = None,
    ) -> _ActiveRecord:
        base_url = _normalise_base_url(request_base_url)
        if isinstance(request_params, str):
            params_json = request_params
        else:
            params_json = _canonical_json(request_params)
        now = self._now_factory()
        return _ActiveRecord(
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

    def update_pagination(
        self,
        record: _ActiveRecord,
        pagination_payload: Mapping[str, Any] | Iterable[Mapping[str, Any]],
        *,
        records_fetched_delta: int | None = None,
    ) -> int:
        events: list[dict[str, Any]]
        if isinstance(pagination_payload, Mapping):
            mapped = cast(Mapping[str, Any], pagination_payload)
            events = [dict(mapped.items())]
        else:
            events = [dict(payload.items()) for payload in pagination_payload]
        record.pagination_events.extend(events)
        if records_fetched_delta is not None:
            record.records_fetched += records_fetched_delta
        record.request_finished_at = self._now_factory()
        return len(events)

    def finish_record(
        self,
        record: _ActiveRecord,
        *,
        status: str,
        records_fetched: int,
        error_message: str | None = None,
        retry_count_delta: int = 0,
        notes: str | None = None,
        request_finished_at: datetime | None = None,
        ingested_at: datetime | None = None,
    ) -> pd.DataFrame:
        record.status = status
        record.records_fetched = records_fetched
        record.error_message_opt = error_message
        record.retry_count += max(retry_count_delta, 0)
        record.request_finished_at = request_finished_at or self._now_factory()
        record.ingested_at = ingested_at or self._now_factory()
        if notes:
            record.notes = notes if record.notes is None else f"{record.notes}; {notes}"

        payload = record.to_payload()
        payload["hash_business_key"] = hash_from_mapping(payload, BUSINESS_KEY_FIELDS)
        payload["hash_row"] = hash_from_mapping(payload, ROW_HASH_FIELDS)

        df = pd.DataFrame([payload], columns=COLUMN_ORDER)
        LoadMetaSchema.validate(df, lazy=True)
        return df
