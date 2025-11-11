"""Specification utilities for generic ChEMBL entity pipelines."""

from __future__ import annotations

"""Declarative specifications for ChEMBL entity extraction pipelines."""

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.config.models.source import SourceConfig

from .chembl_base import ChemblExtractionContext, ChemblExtractionDescriptor, ChemblPipelineBase


RecordTransform = Callable[[ChemblPipelineBase, Mapping[str, Any], ChemblExtractionContext], Mapping[str, Any]]
PostProcessor = Callable[[ChemblPipelineBase, pd.DataFrame, ChemblExtractionContext, BoundLogger], pd.DataFrame]
EmptyFrameFactory = Callable[[ChemblPipelineBase, ChemblExtractionContext], pd.DataFrame]
DryRunHandler = Callable[[ChemblPipelineBase, ChemblExtractionContext, BoundLogger, float], pd.DataFrame]
SummaryExtraFactory = Callable[[ChemblPipelineBase, pd.DataFrame, ChemblExtractionContext], Mapping[str, Any]]


@dataclass(slots=True)
class ChemblEntitySpecification:
    """Declarative specification describing a ChEMBL entity extraction."""

    name: str
    source_name: str
    source_config_factory: Callable[[SourceConfig], Any]
    build_context: Callable[[ChemblPipelineBase, Any, BoundLogger], ChemblExtractionContext]
    id_column: str
    summary_event: str
    default_select_fields: Sequence[str] | None = None
    must_have_fields: Sequence[str] = field(default_factory=tuple)
    record_transform: RecordTransform | None = None
    post_processors: Sequence[PostProcessor] = field(default_factory=tuple)
    sort_by: Sequence[str] | str | None = None
    empty_frame_factory: EmptyFrameFactory | None = None
    dry_run_handler: DryRunHandler | None = None
    hard_page_size_cap: int | None = 25
    summary_extra: SummaryExtraFactory | None = None
    required_enrichers: Sequence[str] = field(default_factory=tuple)

    def to_descriptor(self) -> ChemblExtractionDescriptor:
        """Convert the entity specification into an extraction descriptor."""

        default_fields: Sequence[str] | None = None
        if self.default_select_fields is not None:
            default_fields = tuple(self.default_select_fields)

        post_processors: tuple[PostProcessor, ...] = tuple(self.post_processors)

        must_have_fields: tuple[str, ...] = tuple(self.must_have_fields)

        return ChemblExtractionDescriptor(
            name=self.name,
            source_name=self.source_name,
            source_config_factory=self.source_config_factory,
            build_context=self.build_context,
            id_column=self.id_column,
            summary_event=self.summary_event,
            must_have_fields=must_have_fields,
            default_select_fields=default_fields,
            record_transform=self.record_transform,
            post_processors=post_processors,
            sort_by=self.sort_by,
            empty_frame_factory=self.empty_frame_factory,
            dry_run_handler=self.dry_run_handler,
            hard_page_size_cap=self.hard_page_size_cap,
            summary_extra=self.summary_extra,
        )
