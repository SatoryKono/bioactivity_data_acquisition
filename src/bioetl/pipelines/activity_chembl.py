from __future__ import annotations

from bioetl.pipelines.base import FileBasedPipeline, PipelineSpec
from bioetl.pipelines.schemas import activity_schema

_ACTIVITY_SPEC = PipelineSpec(
    name="activity_chembl",
    business_key=("activity_id", "row_subtype", "row_index"),
    required_fields=(
        "assay_chembl_id",
        "testitem_chembl_id",
        "molecule_chembl_id",
        "load_meta_id",
    ),
    optional_fields=(
        "target_chembl_id",
        "document_chembl_id",
        "activity_properties",
        "assay_type",
        "assay_description",
        "assay_organism",
        "assay_tax_id",
        "target_pref_name",
        "target_organism",
        "target_tax_id",
    ),
)


class ActivityChemblPipeline(FileBasedPipeline):
    """Пайплайн загрузки фактов активности из ChEMBL."""

    def __init__(self, run_id: str, *, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_ACTIVITY_SPEC,
            schema=activity_schema(),
            strict_validation=strict_validation,
        )
