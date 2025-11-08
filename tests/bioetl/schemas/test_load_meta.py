"""Unit tests for the load_meta Pandera schema."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pandera.errors
import pytest

from bioetl.core.hashing import hash_from_mapping
from bioetl.schemas.load_meta import (
    BUSINESS_KEY_FIELDS,
    ROW_HASH_FIELDS,
    LoadMetaSchema,
)


def _utc(seconds: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _record(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "load_meta_id": "123e4567-e89b-12d3-a456-426614174000",
        "source_system": "chembl_rest",
        "source_release": "36",
        "source_api_version": "2.0",
        "request_base_url": "https://www.ebi.ac.uk/chembl/api/data/assay",
        "request_params_json": "{}",
        "pagination_meta": None,
        "request_started_at": _utc(0),
        "request_finished_at": _utc(1),
        "ingested_at": _utc(2),
        "records_fetched": 0,
        "status": "success",
        "error_message_opt": None,
        "retry_count": 0,
        "job_id": "job-1",
        "operator": "pipeline.activity",
        "notes": None,
    }
    payload.update(overrides)
    payload["hash_business_key"] = hash_from_mapping(payload, BUSINESS_KEY_FIELDS)
    payload["hash_row"] = hash_from_mapping(payload, ROW_HASH_FIELDS)
    return payload


def test_validate_accepts_well_formed_record() -> None:
    df = pd.DataFrame([_record()])
    LoadMetaSchema.validate(df, lazy=True)


def test_validate_rejects_invalid_status() -> None:
    df = pd.DataFrame([_record(status="invalid")])
    with pytest.raises(pandera.errors.SchemaErrors):
        LoadMetaSchema.validate(df, lazy=True)


def test_validate_enforces_time_order() -> None:
    df = pd.DataFrame(
        [
            _record(
                request_started_at=_utc(5),
                request_finished_at=_utc(1),
                ingested_at=_utc(2),
            )
        ]
    )
    with pytest.raises(pandera.errors.SchemaErrors):
        LoadMetaSchema.validate(df, lazy=True)
