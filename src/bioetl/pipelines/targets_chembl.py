from __future__ import annotations

from bioetl.pipelines.base import FileBasedPipeline, PipelineSpec
from bioetl.pipelines.schemas import targets_schema

_TARGETS_SPEC = PipelineSpec(
    name="targets_chembl",
    business_key=("target_chembl_id",),
    required_fields=("load_meta_id",),
)


class TargetsChemblPipeline(FileBasedPipeline):
    """Пайплайн размерности целей ChEMBL."""

    def __init__(self, run_id: str, *, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_TARGETS_SPEC,
            schema=targets_schema(),
            strict_validation=strict_validation,
        )
