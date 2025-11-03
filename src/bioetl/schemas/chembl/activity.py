"""ChEMBL-only Activity schema with strict Pandera validation."""

from __future__ import annotations

from pandera import Check, Column, DataFrameSchema  # type: ignore[import-untyped]

# DataFrameSchema возвращает экземпляр класса DataFrameSchema
# Аннотация типа добавлена для явного указания типа переменной
ActivitySchema: DataFrameSchema = DataFrameSchema(  # type: ignore[assignment]
    {
        "activity_id": Column(int, nullable=False, unique=True, coerce=True),
        "assay_id": Column(str, nullable=False),
        "document_id": Column(str, nullable=True),
        "target_id": Column(str, nullable=True),
        "testitem_id": Column(str, nullable=False),
        "record_id": Column(int, nullable=True, coerce=True),
        "standard_type": Column(str, nullable=False, checks=Check.str_length(1, 64)),  # type: ignore[attr-defined]
        "standard_relation": Column(str, nullable=True, checks=Check.isin(["=", ">", "<", ">=", "<=", "~"])),  # type: ignore[attr-defined]
        "standard_value": Column(float, nullable=True),
        "standard_units": Column(str, nullable=True, checks=Check.str_length(0, 32)),  # type: ignore[attr-defined]
        "pchembl_value": Column(float, nullable=True, checks=Check.ge(0) & Check.le(20)),  # type: ignore[attr-defined]
        "data_validity_comment": Column(str, nullable=True),
    },
    strict=True,
    ordered=True,
)
