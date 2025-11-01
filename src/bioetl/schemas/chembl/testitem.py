"""ChEMBL-only TestItem schema with strict Pandera validation."""

from pandera import Check, Column, DataFrameSchema


TestItemSchema = DataFrameSchema(
    {
        "testitem_id": Column(str, nullable=False),
        "pref_name": Column(str, nullable=True),
        "molecule_type": Column(str, nullable=True),
        "max_phase": Column(int, nullable=True, checks=Check.ge(0) & Check.le(4)),
        "first_approval": Column(int, nullable=True, checks=Check.ge(1900) & Check.le(2100)),
        "first_in_class": Column(bool, nullable=True, coerce=True),
    },
    strict=True,
    ordered=True,
)

