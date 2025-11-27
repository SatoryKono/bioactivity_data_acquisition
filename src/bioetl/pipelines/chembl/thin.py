"""Тонкие реализации ChEMBL-пайплайнов для основных сущностей."""

from __future__ import annotations

from typing import Any, Mapping

from bioetl.pipelines.chembl.base import ChemblPipeline
from bioetl.core.schemas.activity_schema import ActivitySchema
from bioetl.core.schemas.assay_schema import AssaySchema
from bioetl.core.schemas.document_schema import DocumentSchema
from bioetl.core.schemas.target_schema import TargetSchema
from bioetl.core.schemas.testitem_schema import TestItemSchema


class ChemblActivityThinPipeline(ChemblPipeline):
    pipeline_code = "activity_chembl_thin"
    entity_name = "activity"
    required_sort_fields = ("activity_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(config, run_id=run_id, validator=ActivitySchema, **kwargs)


class ChemblAssayThinPipeline(ChemblPipeline):
    pipeline_code = "assay_chembl_thin"
    entity_name = "assay"
    required_sort_fields = ("assay_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(config, run_id=run_id, validator=AssaySchema, **kwargs)


class ChemblDocumentThinPipeline(ChemblPipeline):
    pipeline_code = "document_chembl_thin"
    entity_name = "document"
    required_sort_fields = ("document_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(config, run_id=run_id, validator=DocumentSchema, **kwargs)


class ChemblTargetThinPipeline(ChemblPipeline):
    pipeline_code = "target_chembl_thin"
    entity_name = "target"
    required_sort_fields = ("target_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(config, run_id=run_id, validator=TargetSchema, **kwargs)


class ChemblTestItemThinPipeline(ChemblPipeline):
    pipeline_code = "testitem_chembl_thin"
    entity_name = "testitem"
    required_sort_fields = ("test_item_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None, **kwargs: Any) -> None:
        super().__init__(config, run_id=run_id, validator=TestItemSchema, **kwargs)


__all__ = [
    "ChemblActivityThinPipeline",
    "ChemblAssayThinPipeline",
    "ChemblDocumentThinPipeline",
    "ChemblTargetThinPipeline",
    "ChemblTestItemThinPipeline",
]
