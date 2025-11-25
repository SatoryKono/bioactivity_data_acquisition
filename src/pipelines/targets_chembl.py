from __future__ import annotations

from pipelines.base import FileBasedPipeline, PipelineSpec
from pipelines.schemas import targets_schema

_TARGETS_SPEC = PipelineSpec(
    name="targets_chembl",
    business_key=("target_chembl_id",),
    required_fields=("target_chembl_id", "load_meta_id"),
    optional_fields=(
        "pref_name",
        "target_type",
        "organism",
        "tax_id",
        "species_group_flag",
        "cross_references__flat",
        "target_components__flat",
        "target_component_synonyms__flat",
        "uniprot_accessions",
        "protein_class_desc",
        "protein_class_list",
        "protein_class_top",
        "component_count",
    ),
    require_hash_business_key=True,
)


class TargetsChemblPipeline(FileBasedPipeline):
    """Пайплайн загрузки размерности целей из ChEMBL."""

    def __init__(self, run_id: str, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_TARGETS_SPEC,
            schema=targets_schema(),
            strict_validation=strict_validation,
        )


__all__ = ["TargetsChemblPipeline"]
