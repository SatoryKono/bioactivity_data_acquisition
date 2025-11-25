from __future__ import annotations

from bioetl.pipelines.base import FileBasedPipeline, PipelineSpec
from bioetl.pipelines.schemas import assays_schema

_ASSAYS_SPEC = PipelineSpec(
    name="assays_chembl",
    business_key=("assay_chembl_id", "row_subtype", "row_index"),
    required_fields=("load_meta_id",),
    optional_fields=(
        "target_chembl_id",
        "document_chembl_id",
        "assay_type",
        "assay_category",
        "assay_organism",
        "assay_tax_id",
        "confidence_score",
        "confidence_description",
    ),
)


class AssaysChemblPipeline(FileBasedPipeline):
    """Пайплайн размерности экспериментов ChEMBL."""

    def __init__(self, run_id: str, *, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_ASSAYS_SPEC,
            schema=assays_schema(),
            strict_validation=strict_validation,
        )
