"""Pandera schema for the load_meta lineage table."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import pandas as pd
import pandera as pa
from pandera import Check, Column

from bioetl.schemas.base import create_schema
from bioetl.schemas.vocab import required_vocab_ids

SCHEMA_VERSION = "1.0.0"

COLUMN_ORDER = (
    "load_meta_id",
    "source_system",
    "source_release",
    "source_api_version",
    "request_base_url",
    "request_params_json",
    "pagination_meta",
    "request_started_at",
    "request_finished_at",
    "ingested_at",
    "records_fetched",
    "status",
    "error_message_opt",
    "retry_count",
    "job_id",
    "operator",
    "notes",
)

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

ALLOWED_SOURCE_SYSTEMS: tuple[str, ...] = tuple(sorted(required_vocab_ids("source_system")))
ALLOWED_STATUS_VALUES: tuple[str, ...] = tuple(sorted(required_vocab_ids("status")))


def _is_uuid_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return bool(UUID_PATTERN.match(value))


def _is_valid_json_string(value: Any) -> bool:
    if value is None or value is pd.NA:
        return False
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        json.loads(value)
    except (TypeError, ValueError):
        return False
    return True


def _validate_json_series(series: pd.Series) -> bool:
    return bool(series.map(_is_valid_json_string).all())


def _validate_optional_json_series(series: pd.Series) -> bool:
    non_null = series.dropna()
    if non_null.empty:
        return True
    return bool(non_null.map(_is_valid_json_string).all())


def _is_timezone_aware(series: pd.Series) -> bool:
    return series.dt.tz is not None


def _time_window_consistent(row: pd.Series) -> bool:
    start = row["request_started_at"]
    finish = row["request_finished_at"]
    ingested = row["ingested_at"]
    if not (isinstance(start, datetime) and isinstance(finish, datetime) and isinstance(ingested, datetime)):
        return False
    return start <= finish <= ingested


def _uuid_series(series: pd.Series) -> bool:
    return bool(series.map(_is_uuid_string).all())


columns: dict[str, Column] = {
    "load_meta_id": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check(_uuid_series, element_wise=False)],
        nullable=False,
        unique=True,
    ),  # type: ignore[assignment]
    "source_system": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check.isin(ALLOWED_SOURCE_SYSTEMS)],
        nullable=False,
    ),  # type: ignore[assignment]
    "source_release": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "source_api_version": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "request_base_url": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check.str_matches(r"^https?://")],
        nullable=False,
    ),  # type: ignore[assignment]
    "request_params_json": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check(_validate_json_series, element_wise=False)],
        nullable=False,
    ),  # type: ignore[assignment]
    "pagination_meta": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check(_validate_optional_json_series, element_wise=False)],
        nullable=True,
    ),  # type: ignore[assignment]
    "request_started_at": Column(
        pa.DateTime,  # type: ignore[arg-type]
        checks=[Check(_is_timezone_aware, element_wise=False)],
        nullable=False,
    ),  # type: ignore[assignment]
    "request_finished_at": Column(
        pa.DateTime,  # type: ignore[arg-type]
        checks=[Check(_is_timezone_aware, element_wise=False)],
        nullable=False,
    ),  # type: ignore[assignment]
    "ingested_at": Column(
        pa.DateTime,  # type: ignore[arg-type]
        checks=[Check(_is_timezone_aware, element_wise=False)],
        nullable=False,
    ),  # type: ignore[assignment]
    "records_fetched": Column(pa.Int64, checks=[Check.ge(0)], nullable=False),  # type: ignore[arg-type,assignment]
    "status": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check.isin(ALLOWED_STATUS_VALUES)],
        nullable=False,
    ),  # type: ignore[assignment]
    "error_message_opt": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "retry_count": Column(pa.Int64, checks=[Check.ge(0)], nullable=False),  # type: ignore[arg-type,assignment]
    "job_id": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "operator": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "notes": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
}

_BASE_SCHEMA = create_schema(
    columns=columns,
    version=SCHEMA_VERSION,
    name="LoadMetaSchema",
    strict=True,
    checks=[Check(_time_window_consistent, axis=1, name="time_window_consistency")],
)
LoadMetaSchema = _BASE_SCHEMA

__all__ = [
    "ALLOWED_SOURCE_SYSTEMS",
    "ALLOWED_STATUS_VALUES",
    "COLUMN_ORDER",
    "LoadMetaSchema",
    "SCHEMA_VERSION",
]

