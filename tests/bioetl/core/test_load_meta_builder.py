from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from bioetl.core import hash_from_mapping
from infrastructure.runtime.load_meta_builder import LoadMetaBuilder
from infrastructure.schemas.chembl_metadata_schema import (
    BUSINESS_KEY_FIELDS,
    ROW_HASH_FIELDS,
    LoadMetaSchema,
)


def test_finish_record_builds_validated_payload() -> None:
    fixed_time = datetime(2024, 2, 1, 12, 30, tzinfo=timezone.utc)
    builder = LoadMetaBuilder(now_factory=lambda: fixed_time)

    record = builder.begin_record(
        "123e4567-e89b-12d3-a456-426614174000",
        source_system="chembl_rest",
        request_base_url="ebi.ac.uk/chembl/api/data",
        request_params={"limit": 25},
        source_release="36",
        source_api_version="2.0",
        job_id="job-123",
        operator="pipeline.activity",
        notes="initial",
    )

    pages = builder.update_pagination(
        record,
        {"page_index": 0, "endpoint": "assay.json", "result_count": 25, "status_code": 200},
        records_fetched_delta=25,
    )
    assert pages == 1
    assert record.records_fetched == 25
    assert record.request_finished_at == fixed_time

    frame = builder.finish_record(
        record,
        status="success",
        records_fetched=25,
        notes="finalised",
    )

    LoadMetaSchema.validate(frame, lazy=True)
    assert frame.at[0, "status"] == "success"
    assert frame.at[0, "records_fetched"] == 25
    assert frame.at[0, "source_release"] == "36"
    assert frame.at[0, "request_base_url"].startswith("https://")
    assert frame.at[0, "pagination_meta"] is not None

    row = frame.iloc[0]
    mapping: Mapping[str, Any] = {column: row[column] for column in frame.columns}
    assert mapping["hash_business_key"] == hash_from_mapping(mapping, BUSINESS_KEY_FIELDS)
    assert mapping["hash_row"] == hash_from_mapping(mapping, ROW_HASH_FIELDS)
