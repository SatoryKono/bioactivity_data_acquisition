"""Pandera schema for the load_meta lineage table."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pandera as pa
from pandas import DatetimeTZDtype
from pandera import Check, Column

from bioetl.schemas.base import create_schema
from bioetl.schemas.common import (
    coerce_optional_timestamp,
    ensure_json_text,
    is_null_like,
    sort_normalized,
    string_column_with_check,
    uuid_column,
)
from bioetl.schemas.vocab import required_vocab_ids

SCHEMA_VERSION = "1.0.0"

BASE_COLUMNS = (
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

BUSINESS_KEY_FIELDS = (
    "source_system",
    "request_base_url",
    "request_params_json",
    "source_release",
    "source_api_version",
    "job_id",
    "operator",
)

ROW_HASH_FIELDS = BASE_COLUMNS

COLUMN_ORDER = (*BASE_COLUMNS, "hash_business_key", "hash_row")

ALLOWED_SOURCE_SYSTEMS: tuple[str, ...] = tuple(
    sort_normalized(str(item) for item in required_vocab_ids("source_system"))
)
ALLOWED_STATUS_VALUES: tuple[str, ...] = tuple(
    sort_normalized(str(item) for item in required_vocab_ids("status"))
)


def _time_window_consistent(row: pd.Series[Any], **_: Any) -> bool:
    start = coerce_optional_timestamp(row.get("request_started_at"))
    finish = coerce_optional_timestamp(row.get("request_finished_at"))
    ingested = coerce_optional_timestamp(row.get("ingested_at"))

    if not (start and finish and ingested):
        return False

    return start <= finish <= ingested


def _validate_json_series(series: pd.Series[Any]) -> bool:
    try:
        series.map(ensure_json_text)
    except (TypeError, ValueError):
        return False
    return True


def _validate_optional_json_series(series: pd.Series[Any]) -> bool:
    mask = series.map(is_null_like)
    non_null = series[~mask]
    if non_null.empty:
        return True
    return _validate_json_series(non_null)


columns: dict[str, Column] = {
    "load_meta_id": uuid_column(nullable=False, unique=True),
    "source_system": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check.isin(ALLOWED_SOURCE_SYSTEMS)],  # type: ignore[arg-type]
        nullable=False,
    ),  # type: ignore[assignment]
    "source_release": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "source_api_version": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "request_base_url": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check.str_matches(r"^https?://")],  # type: ignore[arg-type]
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
        DatetimeTZDtype(tz="UTC"),
        nullable=False,
    ),  # type: ignore[assignment]
    "request_finished_at": Column(
        DatetimeTZDtype(tz="UTC"),
        nullable=False,
    ),  # type: ignore[assignment]
    "ingested_at": Column(
        DatetimeTZDtype(tz="UTC"),
        nullable=False,
    ),  # type: ignore[assignment]
    "records_fetched": Column(pa.Int64, checks=[Check.ge(0)], nullable=False),  # type: ignore[arg-type,assignment]
    "status": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check.isin(ALLOWED_STATUS_VALUES)],  # type: ignore[arg-type]
        nullable=False,
    ),  # type: ignore[assignment]
    "error_message_opt": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "retry_count": Column(pa.Int64, checks=[Check.ge(0)], nullable=False),  # type: ignore[arg-type,assignment]
    "job_id": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "operator": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "notes": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "hash_business_key": string_column_with_check(str_length=(64, 64), nullable=False),
    "hash_row": string_column_with_check(str_length=(64, 64), nullable=False),
}

_BASE_SCHEMA = create_schema(
    columns=columns,
    version=SCHEMA_VERSION,
    name="LoadMetaSchema",
    strict=True,
    checks=[Check(_time_window_consistent, axis=1, name="time_window_consistency", element_wise=False)],
)
LoadMetaSchema = _BASE_SCHEMA

__all__ = [
    "ALLOWED_SOURCE_SYSTEMS",
    "ALLOWED_STATUS_VALUES",
    "COLUMN_ORDER",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "BASE_COLUMNS",
    "LoadMetaSchema",
    "SCHEMA_VERSION",
]
