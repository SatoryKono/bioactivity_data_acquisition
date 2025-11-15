"""Pandera schema for the chembl_metadata_schema lineage table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import pandera as pa
from functools import partial

from pandas import DatetimeTZDtype
from pandera import Check, Column

from bioetl.schemas._validators import validate_json_series
from bioetl.schemas.base_abstract_schema import create_schema
from bioetl.schemas.common_column_factory import SchemaColumnFactory
from bioetl.schemas.common_schema import HTTP_URL_PATTERN

SCHEMA_VERSION = "1.0.0"
STATUS_VALUES: tuple[str, ...] = ("success", "warning", "error")

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

COLUMN_ORDER: list[str] = [*BASE_COLUMNS, "hash_business_key", "hash_row"]

REQUIRED_FIELDS: list[str] = [
    "load_meta_id",
    "source_system",
    "request_base_url",
    "request_params_json",
    "request_started_at",
    "request_finished_at",
    "ingested_at",
    "records_fetched",
    "status",
    "retry_count",
    "hash_business_key",
    "hash_row",
]

BUSINESS_KEY_FIELDS: list[str] = [
    "source_system",
    "request_base_url",
    "request_params_json",
    "source_release",
    "source_api_version",
    "job_id",
    "operator",
]

ROW_HASH_FIELDS: list[str] = list(BASE_COLUMNS)

def _time_window_consistent(row: pd.Series, **_: Any) -> bool:
    start = row["request_started_at"]
    finish = row["request_finished_at"]
    ingested = row["ingested_at"]

    if isinstance(start, pd.Series):
        start = start.iloc[0]
    if isinstance(finish, pd.Series):
        finish = finish.iloc[0]
    if isinstance(ingested, pd.Series):
        ingested = ingested.iloc[0]

    if not (
        isinstance(start, datetime)
        and isinstance(finish, datetime)
        and isinstance(ingested, datetime)
    ):
        return False

    return start <= finish <= ingested


CF = SchemaColumnFactory

columns: dict[str, Column] = {
    "load_meta_id": CF.uuid(nullable=False, unique=True),
    "source_system": CF.string(nullable=False, vocabulary="source_system"),
    "source_release": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "source_api_version": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "request_base_url": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check.str_matches(HTTP_URL_PATTERN)],
        nullable=False,
    ),  # type: ignore[assignment]
    "request_params_json": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check(validate_json_series, element_wise=False)],
        nullable=False,
    ),  # type: ignore[assignment]
    "pagination_meta": Column(
        pa.String,  # type: ignore[arg-type]
        checks=[Check(partial(validate_json_series, optional=True), element_wise=False)],
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
    "status": CF.string(nullable=False, vocabulary="status", isin=STATUS_VALUES),
    "error_message_opt": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "retry_count": Column(pa.Int64, checks=[Check.ge(0)], nullable=False),  # type: ignore[arg-type,assignment]
    "job_id": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "operator": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "notes": Column(pa.String, nullable=True),  # type: ignore[arg-type,assignment]
    "hash_business_key": CF.string(length=(64, 64), nullable=False),
    "hash_row": CF.string(length=(64, 64), nullable=False),
}

_BASE_SCHEMA = create_schema(
    columns=columns,
    version=SCHEMA_VERSION,
    name="LoadMetaSchema",
    strict=True,
    column_order=COLUMN_ORDER,
    checks=[Check(_time_window_consistent, axis=1, name="time_window_consistency", element_wise=False)],
)
LoadMetaSchema = _BASE_SCHEMA

__all__ = [
    "COLUMN_ORDER",
    "REQUIRED_FIELDS",
    "BUSINESS_KEY_FIELDS",
    "ROW_HASH_FIELDS",
    "STATUS_VALUES",
    "BASE_COLUMNS",
    "LoadMetaSchema",
    "SCHEMA_VERSION",
]
