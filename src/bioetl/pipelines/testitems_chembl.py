from __future__ import annotations

from bioetl.pipelines.base import FileBasedPipeline, PipelineSpec
from bioetl.pipelines.schemas import testitems_schema

_TESTITEMS_SPEC = PipelineSpec(
    name="testitems_chembl",
    business_key=("molecule_chembl_id",),
    required_fields=("_chembl_db_version", "_api_version", "load_meta_id"),
)


class TestItemsChemblPipeline(FileBasedPipeline):
    """Пайплайн размерности тестируемых соединений ChEMBL."""

    def __init__(self, run_id: str, *, config, strict_validation: bool = False) -> None:
        super().__init__(
            run_id,
            config=config,
            spec=_TESTITEMS_SPEC,
            schema=testitems_schema(),
            strict_validation=strict_validation,
        )
