"""Validation guardrails for nullable integer assay columns."""

from __future__ import annotations

import uuid

import pandas as pd
from pandas.api.types import is_integer_dtype

from bioetl.config.loader import load_config
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.pipelines.assay import AssayPipeline
from bioetl.schemas import AssaySchema


def _minimal_assay_row() -> dict[str, object]:
    """Create a minimal schema-compliant assay row for validation tests."""

    schema_columns = list(AssaySchema.to_schema().columns.keys())
    row: dict[str, object] = dict.fromkeys(schema_columns)
    row.update(
        {
            "assay_chembl_id": "CHEMBL123",
            "row_subtype": "assay",
            "row_index": 0,
            "assay_tax_id": 9606,
            "assay_class_id": 1,
            "confidence_score": 1,
            "src_id": 1,
            "variant_id": 1,
            "assay_param_value": 0.0,
            "assay_param_standard_value": 0.0,
            "pipeline_version": "1.0.0",
            "source_system": "chembl",
            "chembl_release": "ChEMBL_TEST",
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "hash_business_key": "b" * 64,
            "hash_row": "a" * 64,
            "index": 0,
        }
    )
    return row


def test_validate_allows_nullable_int_na(monkeypatch) -> None:
    """AssayPipeline.validate should accept <NA> in nullable integer fields."""

    config = load_config("configs/pipelines/assay.yaml")
    run_id = str(uuid.uuid4())[:8]

    monkeypatch.setattr(
        UnifiedAPIClient,
        "request_json",
        lambda self, url, params=None, method="GET", **kwargs: {"chembl_db_version": "ChEMBL_TEST"}
        if url == "/status.json"
        else {},
    )

    pipeline = AssayPipeline(config, run_id)

    df = pd.DataFrame([_minimal_assay_row()]).convert_dtypes()
    nullable_int_columns = [
        "assay_tax_id",
        "confidence_score",
        "src_id",
        "species_group_flag",
        "tax_id",
        "component_count",
        "assay_class_id",
        "variant_id",
    ]
    # Initialize optional integer columns that are not part of the minimal row
    if "component_count" in df.columns and df["component_count"].isna().all():
        df["component_count"] = 1
    if "species_group_flag" in df.columns and df["species_group_flag"].isna().all():
        df["species_group_flag"] = 1
    if "tax_id" in df.columns and df["tax_id"].isna().all():
        df["tax_id"] = 9606

    for column in nullable_int_columns:
        if column in df.columns:
            df[column] = pd.NA

    validated = pipeline.validate(df)

    assert pipeline.validation_issues == []
    for column in nullable_int_columns:
        if column in validated.columns:
            assert is_integer_dtype(validated[column]), f"{column} should remain Int64"
            assert validated[column].isna().all(), f"{column} should preserve <NA>"
