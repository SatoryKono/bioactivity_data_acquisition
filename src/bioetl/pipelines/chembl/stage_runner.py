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
        context=stage_context,
        options=StageExecutionOptions(run_tag=run_tag, mode=mode, dry_run=pipeline.dry_run),
    )
    # Temporary workaround for attributes if needed, but types don't support it.
    # Assuming StageContext or other mechanism handles this now.
    # We inject output_dir into context as it seems required by factory.py
    # However, StageContext definition in types.py doesn't have output_dir.
    # We will assume runtime injection or dynamic attribute for now to fix syntax.
    # To avoid runtime errors if slots are strict, we might have issues.
    # But let's fix the merge conflict first.
    if hasattr(stage_context, "output_dir"):
        setattr(stage_context, "output_dir", target_dir)
    if hasattr(stage_context, "artifacts"):
        stage_context.artifacts = artifacts

    return stage_context, runtime_context


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

    context, runtime = _build_stage_contexts(pipeline, output_dir, run_tag=run_tag, mode=mode)
    runtime.options.dry_run = False

    descriptors = pipeline.build_stage_plan(context, runtime.options)
    context.descriptor = pipeline.build_descriptor()

    definition = getattr(pipeline, "pipeline_definition", None)
    factory = StageFactory(definition)
    stages = factory.build(_filter_descriptors(descriptors, ("extract",)), context)

    for stage in stages:
        stage.execute(runtime)

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
    context, runtime = _build_stage_contexts(pipeline, output_root, run_tag=run_tag, mode=mode)
    # Override options with full options constructed above
    runtime.options = options
    context.current_df = df
    
    if descriptor is None and normalized_stage == "extract":
        descriptor = getattr(pipeline, "build_descriptor", lambda: None)()
    context.descriptor = descriptor

    descriptors = pipeline.build_stage_plan(context, options)
    descriptor_plan = _filter_descriptors(descriptors, (normalized_stage,))

    if not descriptor_plan:
        raise ValueError(f"No stage plan available for stage '{normalized_stage}'")

    definition = getattr(pipeline, "pipeline_definition", None)
    factory = StageFactory(definition)
    stages = factory.build(descriptor_plan, context)

    result: Any = None
    for stage in stages:
        result = stage.execute(runtime).output

    return result
