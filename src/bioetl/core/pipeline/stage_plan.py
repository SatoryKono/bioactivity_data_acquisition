from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.types import PipelineBaseProtocol, Stage, StageContext, StageExecutionOptions


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


def build_default_pipeline_definition(pipeline: PipelineBaseProtocol) -> PipelineDefinition:
    """Construct default immutable definition for classic ETL pipelines."""

    def _run_extract(stage_context: StageContext, exec_options: StageExecutionOptions) -> pd.DataFrame:
        if exec_options.dry_run:
            frame = _empty_frame_from_schema(pipeline)
        else:
            frame = pipeline.extract(stage_context.descriptor, exec_options)
        if exec_options.limit is not None and frame is not None:
            frame = frame.head(exec_options.limit)
        if exec_options.sample is not None and frame is not None and not frame.empty:
            frame = frame.sample(min(exec_options.sample, len(frame)), random_state=0)
        stage_context.current_df = frame
        return frame

    def _run_transform(stage_context: StageContext, exec_options: StageExecutionOptions) -> pd.DataFrame:
        if stage_context.current_df is None:
            raise RuntimeError("transform stage requires extracted data")
        stage_context.current_df = pipeline.transform(stage_context.current_df, exec_options)
        return stage_context.current_df

    def _run_validate(stage_context: StageContext, exec_options: StageExecutionOptions) -> pd.DataFrame:
        if stage_context.current_df is None:
            raise RuntimeError("validate stage requires transformed data")
        frame = _validate_with_schema(pipeline, stage_context.current_df)
        frame = pipeline.validate(frame, exec_options)
        stage_context.current_df = _sort_dataframe(frame)
        return stage_context.current_df

    def _run_save_results(
        stage_context: StageContext, exec_options: StageExecutionOptions
    ) -> Any:
        if stage_context.current_df is None:
            raise RuntimeError("save_results stage requires validated data")
        artifacts = stage_context.artifacts or WriteArtifacts(
            data_path=stage_context.output_dir / f"{_pipeline_name(pipeline)}.csv"
        )
        stage_context.artifacts = artifacts
        result = pipeline.save_results(stage_context.current_df, artifacts, exec_options)
        if hasattr(result, "artifacts") and result.artifacts:
            stage_context.artifacts = result.artifacts
        stage_context.metadata.setdefault("write_result", result)
        return result

    stage_plan: tuple[Stage, ...] = (
        Stage("extract", _run_extract),
        Stage("transform", _run_transform),
        Stage("validate", _run_validate),
        Stage("save_results", _run_save_results),
    )

    metadata = {"name": _pipeline_name(pipeline)}
    version = getattr(pipeline, "pipeline_version", "1.0.0")
    return PipelineDefinition(stage_plan, metadata=metadata, version=version)


__all__ = ["build_default_pipeline_definition"]
