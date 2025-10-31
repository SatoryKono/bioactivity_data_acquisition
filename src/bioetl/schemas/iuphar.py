"""Pandera schemas for IUPHAR pipeline outputs."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import pa
from bioetl.pandera_typing import Series
from bioetl.schemas.base import BaseSchema


class IupharTargetSchema(BaseSchema):
    """Schema describing the primary IUPHAR target dataset."""

    iuphar_target_id: Series[pd.Int64Dtype] = pa.Field(nullable=False, ge=0)
    targetId: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    iuphar_name: Series[str] = pa.Field(nullable=True)
    name: Series[str] = pa.Field(nullable=True)
    abbreviation: Series[str] = pa.Field(nullable=True)
    synonyms: Series[str] = pa.Field(nullable=True)
    annotationStatus: Series[str] = pa.Field(nullable=True)
    geneSymbol: Series[str] = pa.Field(nullable=True)
    classification_path: Series[str] = pa.Field(nullable=True)
    classification_depth: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    iuphar_type: Series[str] = pa.Field(nullable=True)
    iuphar_class: Series[str] = pa.Field(nullable=True)
    iuphar_subclass: Series[str] = pa.Field(nullable=True)
    pipeline_version: Series[str] = pa.Field(nullable=True)
    run_id: Series[str] = pa.Field(nullable=True)
    source_system: Series[str] = pa.Field(nullable=True)
    extracted_at: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False

    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "run_id",
        "source_system",
        "extracted_at",
        "iuphar_target_id",
        "targetId",
        "iuphar_name",
        "name",
        "abbreviation",
        "synonyms",
        "annotationStatus",
        "geneSymbol",
        "classification_path",
        "classification_depth",
        "iuphar_type",
        "iuphar_class",
        "iuphar_subclass",
    ]


class IupharClassificationSchema(BaseSchema):
    """Schema describing per-family IUPHAR classifications."""

    iuphar_target_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    iuphar_family_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    iuphar_family_name: Series[str] = pa.Field(nullable=True)
    classification_path: Series[str] = pa.Field(nullable=True)
    classification_depth: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    iuphar_type: Series[str] = pa.Field(nullable=True)
    iuphar_class: Series[str] = pa.Field(nullable=True)
    iuphar_subclass: Series[str] = pa.Field(nullable=True)
    classification_source: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


class IupharGoldSchema(BaseSchema):
    """Schema describing consolidated IUPHAR enrichment output."""

    iuphar_target_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    iuphar_type: Series[str] = pa.Field(nullable=True)
    iuphar_class: Series[str] = pa.Field(nullable=True)
    iuphar_subclass: Series[str] = pa.Field(nullable=True)
    classification_source: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
        ordered = False


__all__ = [
    "IupharTargetSchema",
    "IupharClassificationSchema",
    "IupharGoldSchema",
]
