"""Tests for the LoadMetaStore append-only persistence."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from bioetl.core.hashing import hash_from_mapping
from bioetl.core.load_meta_store import LoadMetaStore
from bioetl.schemas.load_meta import (
    BUSINESS_KEY_FIELDS,
    ROW_HASH_FIELDS,
    LoadMetaSchema,
)


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)  # pyright: ignore[reportUnknownMemberType]


def test_finish_record_persists_parquet(tmp_path: Path) -> None:
    store = LoadMetaStore(tmp_path)
    params = {"limit": 50, "filter": "chembl"}
    load_meta_id = store.begin_record(
        "chembl_rest",
        "https://www.ebi.ac.uk/chembl/api/data/assay",
        params,
        source_release="36",
        source_api_version="2.0",
        job_id="job-123",
        operator="pipeline.activity",
    )
    store.update_pagination(
        load_meta_id,
        {"page_index": 0, "endpoint": "assay.json", "result_count": 25, "status_code": 200},
        records_fetched_delta=25,
    )
    store.finish_record(load_meta_id, status="success", records_fetched=25)

    file_path = tmp_path / "load_meta" / f"{load_meta_id}.parquet"
    assert file_path.exists()
    frame = _read_parquet(file_path)
    LoadMetaSchema.validate(frame, lazy=True)
    assert frame.at[0, "status"] == "success"
    assert frame.at[0, "records_fetched"] == 25
    assert frame.at[0, "load_meta_id"] == load_meta_id
    row = frame.iloc[0]
    row_mapping: Mapping[str, Any] = {column: row[column] for column in frame.columns}
    assert len(row["hash_business_key"]) == 64
    assert len(row["hash_row"]) == 64
    assert row["hash_business_key"] == hash_from_mapping(row_mapping, BUSINESS_KEY_FIELDS)
    assert row["hash_row"] == hash_from_mapping(row_mapping, ROW_HASH_FIELDS)


def test_finish_record_records_error(tmp_path: Path) -> None:
    store = LoadMetaStore(tmp_path)
    load_meta_id = store.begin_record(
        "chembl_rest",
        "https://www.ebi.ac.uk/chembl/api/data/target",
        {"limit": 10},
    )
    store.finish_record(
        load_meta_id,
        status="error",
        records_fetched=0,
        error_message="timeout",
        retry_count_delta=1,
    )

    file_path = tmp_path / "load_meta" / f"{load_meta_id}.parquet"
    frame = _read_parquet(file_path)
    assert frame.at[0, "status"] == "error"
    assert frame.at[0, "error_message_opt"] == "timeout"
    assert frame.at[0, "retry_count"] == 1
    row = frame.iloc[0]
    row_mapping: Mapping[str, Any] = {column: row[column] for column in frame.columns}
    assert row["hash_business_key"] == hash_from_mapping(row_mapping, BUSINESS_KEY_FIELDS)
    assert row["hash_row"] == hash_from_mapping(row_mapping, ROW_HASH_FIELDS)


def test_update_pagination_requires_active_id(tmp_path: Path) -> None:
    store = LoadMetaStore(tmp_path)
    with pytest.raises(KeyError):
        store.update_pagination("missing", {"page": 1})
