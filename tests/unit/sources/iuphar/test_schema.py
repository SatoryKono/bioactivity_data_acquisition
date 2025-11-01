"""Schema validation tests for IUPHAR outputs."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.iuphar.schema import (
    IupharClassificationSchema,
    IupharGoldSchema,
    IupharTargetSchema,
)


def test_iuphar_target_schema_validates_dataframe() -> None:
    """A minimal DataFrame satisfying the target schema should validate."""

    df = pd.DataFrame(
        {
            "iuphar_target_id": pd.Series([1], dtype="Int64"),
            "targetId": pd.Series([1], dtype="Int64"),
            "iuphar_name": ["Example"],
            "name": ["Example"],
            "abbreviation": ["EX"],
            "synonyms": ["Example"],
            "annotationStatus": ["CURATED"],
            "geneSymbol": ["EX"],
            "classification_path": ["GPCRs > Class A"],
            "classification_depth": pd.Series([2], dtype="Int64"),
            "iuphar_type": ["GPCRs"],
            "iuphar_class": ["Class A GPCRs"],
            "iuphar_subclass": ["Adenosine receptors"],
            "pipeline_version": ["1.0.0"],
            "run_id": ["test"],
            "source_system": ["iuphar"],
            "extracted_at": ["2024-01-01T00:00:00Z"],
        }
    )

    validated = IupharTargetSchema.validate(df)
    assert not validated.empty


def test_iuphar_classification_schema_accepts_optional_fields() -> None:
    """Classification schema should allow nullable entries."""

    df = pd.DataFrame(
        {
            "iuphar_target_id": pd.Series([1], dtype="Int64"),
            "iuphar_family_id": pd.Series([10], dtype="Int64"),
            "iuphar_family_name": ["GPCRs"],
            "classification_path": ["GPCRs > Class A"],
            "classification_depth": pd.Series([2], dtype="Int64"),
            "iuphar_type": ["GPCRs"],
            "iuphar_class": ["Class A"],
            "iuphar_subclass": ["Adenosine receptors"],
            "classification_source": ["iuphar"],
        }
    )

    validated = IupharClassificationSchema.validate(df)
    assert not validated.empty


def test_iuphar_gold_schema_accepts_metrics() -> None:
    """The gold schema should validate consolidated enrichment metrics."""

    df = pd.DataFrame(
        {
            "iuphar_target_id": pd.Series([1], dtype="Int64"),
            "iuphar_type": ["GPCRs"],
            "iuphar_class": ["Class A"],
            "iuphar_subclass": ["Adenosine receptors"],
            "classification_source": ["iuphar"],
        }
    )

    validated = IupharGoldSchema.validate(df)
    assert not validated.empty
