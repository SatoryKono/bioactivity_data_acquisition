"""StageFactory for building pipeline stage plans."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Callable

import pandas as pd

from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    Stage,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    StageResult,
    StageRuntimeContext,
)


class StageFactory:
    """Adapter that instantiates stage objects from descriptors."""

    def __init__(self, pipeline: PipelineBaseProtocol) -> None:
        self.pipeline = pipeline
        self._registry: dict[str, Callable[[StageDescriptor], Stage]] = {
            "extract": self._create_extract_stage,
            "transform": self._create_transform_stage,
            "validate": self._create_validate_stage,
            "save_results": self._create_save_results_stage,
        }

    def create(self, descriptor: StageDescriptor, context: StageContext) -> Stage:
        """Instantiate a stage based on the provided descriptor."""

        factory = self._registry.get(descriptor.kind)
        if factory is None:
            raise ValueError(f"Unknown stage kind '{descriptor.kind}'")
        return factory(descriptor)

    def build(self, descriptors: Sequence[StageDescriptor], context: StageContext) -> tuple[Stage, ...]:
        """Create stage instances honoring the descriptor order."""

        return tuple(self.create(descriptor, context) for descriptor in descriptors)

    # Internal helpers --------------------------------------------------
    def _create_extract_stage(self, descriptor: StageDescriptor) -> Stage:
        pipeline = self.pipeline

        class ExtractStage:
            name = descriptor.id

            def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
                options = runtime_context.options
                stage_context = runtime_context.context
                if options.dry_run:
                    frame = _empty_frame_from_schema(pipeline)
                else:
                    frame = pipeline.extract(stage_context.descriptor, options)
                if options.limit is not None and frame is not None:
                    frame = frame.head(options.limit)
                if options.sample is not None and frame is not None and not frame.empty:
                    frame = frame.sample(min(options.sample, len(frame)), random_state=0)
                stage_context.current_df = frame
                return StageResult(name=self.name, output=frame)

        return ExtractStage()

    def _create_transform_stage(self, descriptor: StageDescriptor) -> Stage:
        pipeline = self.pipeline

        class TransformStage:
            name = descriptor.id

            def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
                stage_context = runtime_context.context
                options = runtime_context.options
                if stage_context.current_df is None:
                    raise RuntimeError("transform stage requires extracted data")
                stage_context.current_df = pipeline.transform(stage_context.current_df, options)
                return StageResult(name=self.name, output=stage_context.current_df)

        return TransformStage()

    def _create_validate_stage(self, descriptor: StageDescriptor) -> Stage:
        pipeline = self.pipeline

        class ValidateStage:
            name = descriptor.id

            def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
                stage_context = runtime_context.context
                options = runtime_context.options
                if stage_context.current_df is None:
                    raise RuntimeError("validate stage requires transformed data")
                frame = _validate_with_schema(pipeline, stage_context.current_df)
                frame = pipeline.validate(frame, options)
                stage_context.current_df = _sort_dataframe(frame)
                return StageResult(name=self.name, output=stage_context.current_df)

        return ValidateStage()

    def _create_save_results_stage(self, descriptor: StageDescriptor) -> Stage:
        pipeline = self.pipeline

        class SaveResultsStage:
            name = descriptor.id

            def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
                stage_context = runtime_context.context
                options = runtime_context.options
                if stage_context.current_df is None:
                    raise RuntimeError("save_results stage requires validated data")
                artifacts = stage_context.artifacts or WriteArtifacts(
                    data_path=stage_context.output_dir / f"{_pipeline_name(pipeline)}.csv"
                )
                stage_context.artifacts = artifacts
                result = pipeline.save_results(stage_context.current_df, artifacts, options)
                if hasattr(result, "artifacts") and result.artifacts:
                    stage_context.artifacts = result.artifacts
                stage_context.metadata.setdefault("write_result", result)
                return StageResult(name=self.name, output=result)

        return SaveResultsStage()


def _pipeline_name(pipeline: PipelineBaseProtocol) -> str:
    return getattr(pipeline, "pipeline_name", None) or getattr(
        pipeline, "pipeline_code", pipeline.__class__.__name__
    )


def _empty_frame_from_schema(pipeline: PipelineBaseProtocol) -> pd.DataFrame:
    validator = getattr(pipeline, "validator", None)
    if validator is None:
        return pd.DataFrame()
    columns = {name: pd.Series(dtype=str(schema.dtype)) for name, schema in validator.columns.items()}
    return pd.DataFrame(columns)


def _validate_with_schema(pipeline: PipelineBaseProtocol, df: pd.DataFrame) -> pd.DataFrame:
    validator = getattr(pipeline, "validator", None)
    if validator is None:
        return df
    return validator.validate(df)


def _sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    columns = sorted(df.columns)
    if not columns:
        return df.reset_index(drop=True)
    return df.loc[:, columns].sort_values(by=columns).reset_index(drop=True)


__all__ = ["StageFactory"]
