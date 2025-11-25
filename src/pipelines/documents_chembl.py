from __future__ import annotations

from pipelines.base import FileBasedPipeline, PipelineSpec
from pipelines.schemas import documents_schema

_DOCUMENTS_SPEC = PipelineSpec(
    name="documents_chembl",
    business_key=("document_chembl_id",),
    required_fields=("document_chembl_id", "source", "load_meta_id"),
    optional_fields=(
        "doc_type",
        "journal",
        "journal_full_title",
        "doi",
        "doi_clean",
        "pubmed_id",
        "year",
        "volume",
        "issue",
        "first_page",
        "last_page",
        "authors",
        "authors_count",
        "src_id",
        "title",
        "abstract",
        "term",
        "weight",
    ),
    require_hash_business_key=True,
    enforce_source_value=True,
)


class DocumentsChemblPipeline(FileBasedPipeline):
    """Пайплайн загрузки размерности документов из ChEMBL."""

    def __init__(self, run_id: str, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_DOCUMENTS_SPEC,
            schema=documents_schema(),
            strict_validation=strict_validation,
        )


__all__ = ["DocumentsChemblPipeline"]
