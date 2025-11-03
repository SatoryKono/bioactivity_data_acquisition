"""ChEMBL-only Target schema with strict Pandera validation."""

from pandera import Check, Column, DataFrameSchema


TargetSchema = DataFrameSchema(
    {
        "target_id": Column(str, nullable=False),
        "pref_name": Column(str, nullable=True),
        "target_type": Column(str, nullable=True),
        "organism": Column(str, nullable=True),
        "tax_id": Column(int, nullable=True, coerce=True),
    },
    strict=True,
    ordered=True,
)
