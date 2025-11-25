"""Concrete Chembl batch pipelines for common Chembl entities."""

from __future__ import annotations

from .base import BaseChemblPipeline, NormalizedBatch, RawBatch
from .config import PipelineConfig


class _ConfiguredPipeline(BaseChemblPipeline):
    """Mixin-like helper that injects defaults for table and id fields."""

    default_table: str = ""
    default_id_field: str = ""

    def __init__(
        self,
        *,
        db_client,
        normalizer,
        validator,
        config: PipelineConfig | None = None,
    ) -> None:
        effective_config = config or PipelineConfig(
            table_name=self.default_table,
            id_field=self.default_id_field,
        )
        super().__init__(
            db_client=db_client, normalizer=normalizer, validator=validator, config=effective_config
        )


class ActivityPipeline(_ConfiguredPipeline):
    """Activity table pipeline with deduplication logic."""

    default_table = "activity"
    default_id_field = "activity_id"

    def _normalize(self, raw: RawBatch) -> NormalizedBatch:
        normalized = super()._normalize(raw)
        deduped = self.normalizer.drop_duplicates(normalized.records, [self.config.id_field])
        return NormalizedBatch(
            requested_ids=normalized.requested_ids,
            records=deduped,
            missing_ids=normalized.missing_ids,
        )


class AssayPipeline(_ConfiguredPipeline):
    """Assay pipeline with table/id defaults."""

    default_table = "assay"
    default_id_field = "assay_id"

    def _normalize(self, raw: RawBatch) -> NormalizedBatch:
        normalized = super()._normalize(raw)
        return NormalizedBatch(
            requested_ids=normalized.requested_ids,
            records=self.normalizer.drop_duplicates(normalized.records, [self.config.id_field]),
            missing_ids=normalized.missing_ids,
        )


class DocumentPipeline(_ConfiguredPipeline):
    """Document pipeline."""

    default_table = "document"
    default_id_field = "document_id"


class TargetPipeline(_ConfiguredPipeline):
    """Target pipeline."""

    default_table = "target"
    default_id_field = "target_id"


class TestItemPipeline(_ConfiguredPipeline):
    """TestItem pipeline (note the capitalization)."""

    default_table = "testitem"
    default_id_field = "testitem_id"


__all__ = [
    "ActivityPipeline",
    "AssayPipeline",
    "DocumentPipeline",
    "TargetPipeline",
    "TestItemPipeline",
]
