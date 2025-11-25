from __future__ import annotations

from bioetl.pipelines.base import FileBasedPipeline, PipelineSpec
from bioetl.pipelines.schemas import documents_schema

_DOCUMENTS_SPEC = PipelineSpec(
    name="documents_chembl",
    business_key=("document_chembl_id",),
    required_fields=("load_meta_id",),
    require_hash_business_key=True,
    enforce_source_value=True,
)


class DocumentsChemblPipeline(FileBasedPipeline):
    """Пайплайн размерности документов ChEMBL."""

    def __init__(self, run_id: str, *, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_DOCUMENTS_SPEC,
            schema=documents_schema(),
            strict_validation=strict_validation,
        )
