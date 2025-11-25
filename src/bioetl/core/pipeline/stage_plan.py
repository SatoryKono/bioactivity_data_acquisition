from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    PipelineStageCommand,
    StageContextProtocol,
    StageRuntimeContext,
)


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


def build_default_stage_plan(
    pipeline: PipelineBaseProtocol,
    context: StageContextProtocol,
    runtime: StageRuntimeContext,
) -> tuple[PipelineStageCommand, ...]:
    """Assemble a deterministic stage plan shared across pipeline bases."""

    validation_service = getattr(pipeline, "validation_service", None)
    write_service = getattr(pipeline, "write_service", None)

    def _run_extract(stage_context: StageContextProtocol, exec_runtime: StageRuntimeContext) -> pd.DataFrame:
        options = exec_runtime.options
        descriptor = exec_runtime.input_data
        if descriptor is None:
            descriptor = exec_runtime.attributes.get("descriptor")
        if options.dry_run:
            if validation_service:
                frame = validation_service.empty_frame()
            else:
                frame = _empty_frame_from_schema(pipeline)
        else:
            frame = pipeline.extract(descriptor, options)
        if options.limit is not None and frame is not None:
            frame = frame.head(options.limit)
        if options.sample is not None and frame is not None and not frame.empty:
            frame = frame.sample(min(options.sample, len(frame)), random_state=0)
        return frame

    def _run_transform(stage_context: StageContextProtocol, exec_runtime: StageRuntimeContext) -> pd.DataFrame:
        if exec_runtime.input_data is None:
            raise RuntimeError("transform stage requires extracted data")
        return pipeline.transform(exec_runtime.input_data, exec_runtime.options)

    def _run_validate(stage_context: StageContextProtocol, exec_runtime: StageRuntimeContext) -> pd.DataFrame:
        if exec_runtime.input_data is None:
            raise RuntimeError("validate stage requires transformed data")
        if validation_service:
            frame = validation_service.validate(
                exec_runtime.input_data, pipeline=pipeline, options=exec_runtime.options
            )
        else:
            frame = _validate_with_schema(pipeline, exec_runtime.input_data)
            frame = pipeline.validate(frame, exec_runtime.options)
            frame = _sort_dataframe(frame)
        return frame

    def _run_save_results(
        stage_context: StageContextProtocol, exec_runtime: StageRuntimeContext
    ) -> Any:
        if exec_runtime.input_data is None:
            raise RuntimeError("save_results stage requires validated data")
        artifacts = exec_runtime.attributes.get("artifacts") or WriteArtifacts(
            data_path=exec_runtime.attributes.get("output_dir", Path.cwd())
            / f"{_pipeline_name(pipeline)}.csv"
        )
        if write_service:
            result = write_service.save(
                exec_runtime.input_data,
                artifacts,
                exec_runtime.options,
                context=stage_context,
                runtime=exec_runtime,
            )
        else:
            result = pipeline.save_results(exec_runtime.input_data, artifacts, exec_runtime.options)
        if hasattr(result, "artifacts") and result.artifacts:
            exec_runtime.attributes["artifacts"] = result.artifacts
        exec_runtime.attributes.setdefault("metadata", {}).setdefault("write_result", result)
        return result

    stage_plan: tuple[PipelineStageCommand, ...] = (
        PipelineStageCommand("extract", _run_extract),
        PipelineStageCommand("transform", _run_transform),
        PipelineStageCommand("validate", _run_validate),
        PipelineStageCommand("save_results", _run_save_results),
    )

    if runtime.options.dry_run:
        stage_plan = tuple(command for command in stage_plan if command.name != "save_results")
        if getattr(pipeline, "validator", None) is None:
            stage_plan = tuple(command for command in stage_plan if command.name == "extract")

    return stage_plan


__all__ = ["build_default_stage_plan"]
