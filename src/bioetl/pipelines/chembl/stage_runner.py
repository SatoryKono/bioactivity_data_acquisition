from __future__ import annotations

"""Adapters for running ChEMBL pipeline stages via the unified runner."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.types import StageContext, StageDescriptor, StageExecutionOptions, StageRuntimeContext
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


def _build_stage_context(
    pipeline: ChemblPipelineContract,
    output_dir: Path,
    *,
    run_tag: str | None = None,
    mode: str | None = None,
) -> StageContext:
    target_dir, artifacts = pipeline.plan_run_artifacts(output_dir, run_tag, mode)  # type: ignore[arg-type]
    logger = UnifiedLogger.get(pipeline.__class__.__name__).bind(
        run_id=getattr(pipeline, "run_id", ""),
        pipeline=getattr(pipeline, "pipeline_code", pipeline.__class__.__name__),
    )
    return StageContext(
        pipeline=pipeline,  # type: ignore[arg-type]
        output_dir=target_dir,
        logger=logger,
        run_id=getattr(pipeline, "run_id", ""),
        run_tag=run_tag,
        mode=mode,
        descriptor=None,
        artifacts=artifacts,
    )


def _filter_descriptors(
    descriptors: tuple[StageDescriptor, ...], stages: tuple[str, ...]
) -> tuple[StageDescriptor, ...]:
    return tuple(descriptor for descriptor in descriptors if descriptor.id in stages)


def build_extract_plan(
    pipeline: ChemblPipelineContract,
    output_dir: Path,
    *,
    run_tag: str | None = None,
    mode: str | None = None,
) -> tuple[StageDescriptor, ...]:
    """Construct an extract-only stage plan for a pipeline."""

    context = _build_stage_context(pipeline, output_dir, run_tag=run_tag, mode=mode)
    options = StageExecutionOptions(run_tag=run_tag, mode=mode, dry_run=False)
    descriptors = pipeline.build_stage_plan(context, options)
    context.descriptor = pipeline.build_descriptor()
    factory = StageFactory(pipeline)  # type: ignore[arg-type]
    stages = factory.build(_filter_descriptors(descriptors, ("extract",)), context)
    runtime_context = StageRuntimeContext(context=context, options=options)
    for stage in stages:
        stage.execute(runtime_context)
    return descriptors


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
    context = _build_stage_context(pipeline, output_root, run_tag=run_tag, mode=mode)
    context.current_df = df
    if descriptor is None and normalized_stage == "extract":
        descriptor = getattr(pipeline, "build_descriptor", lambda: None)()
    context.descriptor = descriptor

    descriptors = pipeline.build_stage_plan(context, options)
    descriptor_plan = _filter_descriptors(descriptors, (normalized_stage,))

    if not descriptor_plan:
        raise ValueError(f"No stage plan available for stage '{normalized_stage}'")

    factory = StageFactory(pipeline)  # type: ignore[arg-type]
    stages = factory.build(descriptor_plan, context)

    result: Any = None
    runtime_context = StageRuntimeContext(context=context, options=options)
    for stage in stages:
        result = stage.execute(runtime_context).output

    return result

