"""ChEMBL-only Assay schema with strict Pandera validation."""

from pandera import Check, Column, DataFrameSchema


AssaySchema = DataFrameSchema(
    {
        "assay_id": Column(str, nullable=False),
        "description": Column(str, nullable=True),
        "assay_type": Column(str, nullable=True),
        "assay_category": Column(str, nullable=True),
        "assay_test_type": Column(str, nullable=True),
        "assay_strain": Column(str, nullable=True),
        "cell_chembl_id": Column(str, nullable=True),
        "target_id": Column(str, nullable=True),
        "confidence_score": Column(float, nullable=True, checks=Check.ge(0) & Check.le(10)),
        "organism": Column(str, nullable=True),
        "tax_id": Column(int, nullable=True, coerce=True),
    },
    strict=True,
    ordered=True,
)
