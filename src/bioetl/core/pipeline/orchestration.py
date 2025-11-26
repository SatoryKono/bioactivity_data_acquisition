"""Orchestration primitives for unified pipeline lifecycle."""
from __future__ import annotations

import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.runtime import PipelineRuntimeBase
from bioetl.core.pipeline.stage_plan import StagePlanMetadata, build_default_stage_plan
from bioetl.core.pipeline.types import (
    PipelineConfig,
    PipelineStagesProtocol,
    Stage,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    WriteArtifacts,
)


class PipelineBaseCommon(PipelineRuntimeBase, PipelineStagesProtocol):
    """Shared orchestration helpers for ETL pipelines."""

    deterministic_folder_prefix: str = "_"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        warnings.warn(
            "PipelineBaseCommon устарел и будет удалён. Используйте UnifiedPipelineBase.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.pipeline_code = config.pipeline.name
        metadata = StagePlanMetadata(has_validator=self.validator is not None)
        self.pipeline_definition = PipelineDefinition(
            name=self.pipeline_code,
            runtime_factory=self.__class__,
            stages=tuple(build_default_stage_plan(None, metadata)),
        )
        super().__init__(config, self.pipeline_definition, run_id=run_id)
        self.output_root = Path(config.materialization.root)
        self.logs_directory = self.output_root.parent / "logs" / self.pipeline_code
        self.stage_plan: tuple[StageDescriptor, ...] = ()

    # Hook methods -----------------------------------------------------
    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def augment_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        return metadata

    # Factory helpers --------------------------------------------------
    def create_stage_factory(self) -> StageFactory:
        return StageFactory(self.pipeline_definition)

    # Orchestration ----------------------------------------------------
    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        metadata = StagePlanMetadata(dry_run=options.dry_run, has_validator=self.validator is not None)
        plan = tuple(build_default_stage_plan(context.descriptor, metadata))
        self.stage_plan = plan
        return plan

    # Deterministic layout ---------------------------------------------
    def build_run_stem(self, run_tag: str | None, mode: str | None) -> str:
        suffix = [self.pipeline_code]
        if mode:
            suffix.append(mode)
        if run_tag:
            suffix.append(run_tag)
        return self.deterministic_folder_prefix + "-".join(suffix)

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, WriteArtifacts]:
        run_stem = self.build_run_stem(run_tag, mode)
        target_dir = output_dir / run_stem
        target_dir.mkdir(parents=True, exist_ok=True)
        artifacts = WriteArtifacts(data_path=target_dir / f"{self.pipeline_code}.csv")
        return target_dir, artifacts

    # Metadata helpers --------------------------------------------------
    def build_run_metadata(
        self,
        context: StageContext,
        stage_plan: tuple[Stage, ...],
        durations: dict[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        metadata = super().build_run_metadata(context, stage_plan, durations, run_tag, mode)
        metadata.update({"started_at": datetime.now(timezone.utc).isoformat()})
        return self.augment_metadata(metadata)

    def resolve_logs_directory(self, output_dir: Path) -> Path:
        return self.logs_directory


__all__ = ["PipelineBaseCommon"]
