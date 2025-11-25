from __future__ import annotations

from pipelines.base import FileBasedPipeline, PipelineSpec
from pipelines.schemas import assays_schema

_ASSAYS_SPEC = PipelineSpec(
    name="assays_chembl",
    business_key=("assay_chembl_id", "row_subtype", "row_index"),
    required_fields=("assay_chembl_id", "row_subtype", "row_index", "load_meta_id"),
    optional_fields=(
        "description",
        "assay_type",
        "assay_type_description",
        "assay_test_type",
        "assay_category",
        "assay_organism",
        "assay_tax_id",
        "assay_strain",
        "assay_tissue",
        "assay_cell_type",
        "assay_subcellular_fraction",
        "target_chembl_id",
        "document_chembl_id",
        "src_id",
        "src_assay_id",
        "cell_chembl_id",
        "tissue_chembl_id",
        "assay_group",
        "confidence_score",
        "confidence_description",
        "assay_classifications",
        "assay_parameters",
        "assay_class_id",
        "curation_level",
    ),
    require_hash_business_key=True,
)


class AssaysChemblPipeline(FileBasedPipeline):
    """Пайплайн загрузки размерности ассев из ChEMBL."""

    def __init__(self, run_id: str, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_ASSAYS_SPEC,
            schema=assays_schema(),
            strict_validation=strict_validation,
        )


__all__ = ["AssaysChemblPipeline"]
