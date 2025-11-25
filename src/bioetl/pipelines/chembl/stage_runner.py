from __future__ import annotations

"""Adapters for running ChEMBL pipeline stages via the unified runner."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.types import (
    PipelineStageCommand,
    StageContext,
    StageExecutionOptions,
    StageRuntimeContext,
)
from bioetl.pipelines.chembl.common import ChemblPipelineContract

_PIPELINE_REGISTRY: dict[str, Callable[[], ChemblPipelineContract]] = {}

__all__ = [
    "register_pipeline",
    "get_pipeline_specs",
    "build_extract_plan",
    "run_chembl_stage",
]


_STAGE_ALIASES: dict[str, str] = {
    "write": "save_results",
}


def register_pipeline(code: str, factory: Callable[[], ChemblPipelineContract]) -> None:
    """Register a ChEMBL pipeline factory by short code."""

    _PIPELINE_REGISTRY[code] = factory


def get_pipeline_specs() -> dict[str, Callable[[], ChemblPipelineContract]]:
    """Return a copy of registered pipeline factories."""

    return dict(_PIPELINE_REGISTRY)


def _build_stage_contexts(
    pipeline: ChemblPipelineContract,
    output_dir: Path,
    *,
    run_tag: str | None = None,
    mode: str | None = None,
) -> tuple[StageContext, StageRuntimeContext]:
    target_dir, artifacts = pipeline.plan_run_artifacts(output_dir, run_tag, mode)  # type: ignore[arg-type]
    logger = UnifiedLogger.get(pipeline.__class__.__name__).bind(
        run_id=getattr(pipeline, "run_id", ""),
        pipeline=getattr(pipeline, "pipeline_code", pipeline.__class__.__name__),
    )
    stage_context = StageContext(
        logger=logger,
        request_id=getattr(pipeline, "run_id", ""),
        trace_id=getattr(pipeline, "run_id", ""),
        config=getattr(pipeline, "config", {}),
    )
    runtime_context = StageRuntimeContext(
        options=StageExecutionOptions(run_tag=run_tag, mode=mode, dry_run=pipeline.dry_run),
        attributes={"output_dir": target_dir, "artifacts": artifacts, "pipeline": pipeline},
    )
    return stage_context, runtime_context


def build_extract_plan(
    pipeline: ChemblPipelineContract,
    output_dir: Path,
    *,
    run_tag: str | None = None,
    mode: str | None = None,
) -> tuple[PipelineStageCommand, ...]:
    """Construct an extract-only stage plan for a pipeline."""

    context, runtime = _build_stage_contexts(pipeline, output_dir, run_tag=run_tag, mode=mode)
    runtime.options.dry_run = False
    factory = StageFactory(pipeline)  # type: ignore[arg-type]
    plan = factory.build(context, runtime, stages=("extract",))
    # Seed descriptor so the pipeline can reuse it during execution.
    runtime.attributes["descriptor"] = pipeline.build_descriptor()
    for command in plan:
        if command.name == "extract":
            runtime.input_data = command.handler(context, runtime)
    return plan


def run_chembl_stage(
    pipeline: ChemblPipelineContract,
    stage: str,
    *,
    output_dir: Path | None = None,
    df: pd.DataFrame | None = None,
    run_tag: str | None = None,
    mode: str | None = None,
    extended: bool = False,
    dry_run: bool | None = None,
    sample: int | None = None,
    limit: int | None = None,
    include_qc_metrics: bool = False,
    fail_on_schema_drift: bool = True,
    descriptor: Any | None = None,
) -> Any:
    """Execute a single pipeline stage using the unified runner helpers."""

    normalized_stage = _STAGE_ALIASES.get(stage, stage)
    output_root = output_dir or Path.cwd()

    if normalized_stage == "run":
        return pipeline.run(
            output_root,
            run_tag=run_tag,
            mode=mode,
            extended=extended,
            dry_run=dry_run,
            sample=sample,
            limit=limit,
            include_qc_metrics=include_qc_metrics,
            fail_on_schema_drift=fail_on_schema_drift,
        )

    if dry_run is not None:
        pipeline.dry_run = dry_run

    options = StageExecutionOptions(
        run_tag=run_tag,
        mode=mode,
        extended=extended,
        dry_run=pipeline.dry_run,
        sample=sample,
        limit=limit,
        include_qc_metrics=include_qc_metrics,
        fail_on_schema_drift=fail_on_schema_drift,
    )
    context, runtime = _build_stage_contexts(pipeline, output_root, run_tag=run_tag, mode=mode)
    runtime.options = options
    runtime.input_data = df
    if descriptor is None and normalized_stage == "extract":
        descriptor = getattr(pipeline, "build_descriptor", lambda: None)()
    runtime.attributes["descriptor"] = descriptor

    factory = StageFactory(pipeline)  # type: ignore[arg-type]
    stage_plan = factory.build(context, runtime, stages=(normalized_stage,))

    if not stage_plan:
        raise ValueError(f"No stage plan available for stage '{normalized_stage}'")

    result: Any = None
    for command in stage_plan:
        result = command.handler(context, runtime)
        runtime.input_data = result

    return result

