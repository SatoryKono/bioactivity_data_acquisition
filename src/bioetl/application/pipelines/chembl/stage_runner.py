"""Adapters for running ChEMBL pipeline stages via the unified runner."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, TYPE_CHECKING, cast

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.types import (
    ArtifactStore,
    DefaultArtifactContext,
    DefaultDomainContext,
    DefaultExecutionContext,
    DefaultInfrastructureContext,
    PipelineBaseProtocol,
    StageCommand,
    StageContext,
    StageContextProtocol,
    StageDescriptor,
    StageExecutionOptions,
    StageFactoryContext,
    StageRuntimeContext,
)

if TYPE_CHECKING:
    from bioetl.pipelines.chembl.common import (
        ChemblPipelineContract as DomainChemblContract,
    )
else:
    class DomainChemblContract(Protocol):
        """Fallback definition if imports fail during runtime check."""


class ChemblApplicationContract(DomainChemblContract, Protocol):
    pipeline_definition: Any | None
    pipeline_code: str
    run_id: str
    dry_run: bool

    def _build_config_provider(self) -> Callable[[str], Any]:
        ...

    def plan_run_artifacts(
        self, output_dir: Path, run_tag: str | None, mode: str | None
    ) -> tuple[Path, Any]:
        ...

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        ...

    def build_descriptor(self) -> Any:
        ...

    def run(
        self,
        output_dir: Path,
        *,
        run_tag: str | None = None,
        mode: str | None = None,
        extended: bool = False,
        dry_run: bool | None = None,
        sample: int | None = None,
        limit: int | None = None,
        include_qc_metrics: bool = False,
        fail_on_schema_drift: bool = True,
        **options: Any,
    ) -> Any:
        ...


_PIPELINE_REGISTRY: dict[str, Callable[[], ChemblApplicationContract]] = {}

__all__ = [
    "ArtifactPlanner",
    "RuntimeContextBuilder",
    "StageExecutor",
    "register_pipeline",
    "get_pipeline_specs",
    "build_extract_plan",
    "run_chembl_stage",
]


_STAGE_ALIASES: dict[str, str] = {
    "write": "save_results",
}


class ArtifactPlanner:
    """Plans deterministic artifact locations for pipeline runs."""

    def __init__(self, pipeline: ChemblApplicationContract) -> None:
        self.pipeline = pipeline

    def plan(
        self,
        output_dir: Path,
        run_tag: str | None = None,
        mode: str | None = None,
    ) -> tuple[Path, ArtifactStore]:
        target_dir, artifacts = self.pipeline.plan_run_artifacts(
            output_dir, run_tag, mode
        )  # type: ignore[arg-type]
        return target_dir, ArtifactStore(artifacts)


class RuntimeContextBuilder:
    """Builds runtime contexts for executing pipeline stages."""

    def __init__(
        self,
        pipeline: ChemblApplicationContract,
        *,
        artifact_planner: ArtifactPlanner | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.artifact_planner = artifact_planner or ArtifactPlanner(pipeline)

    def build(
        self,
        output_dir: Path,
        *,
        options: StageExecutionOptions | None = None,
        run_tag: str | None = None,
        mode: str | None = None,
    ) -> tuple[StageContext, StageRuntimeContext]:
        target_dir, artifact_store = self.artifact_planner.plan(
            output_dir, run_tag, mode
        )
        logger = cast(
            UnifiedLogger,
            UnifiedLogger.get(self.pipeline.__class__.__name__).bind(
                run_id=getattr(self.pipeline, "run_id", ""),
                pipeline=getattr(
                    self.pipeline,
                    "pipeline_code",
                    self.pipeline.__class__.__name__,
                ),
            ),
        )
        resolved_options = options or StageExecutionOptions(
            run_tag=run_tag,
            mode=mode,
            dry_run=self.pipeline.dry_run,
        )
        execution_context = DefaultExecutionContext(
            logger=logger,
            request_id=getattr(self.pipeline, "run_id", ""),
            trace_id=getattr(self.pipeline, "run_id", ""),
        )
        stage_context = StageContext(
            execution=execution_context,
            domain=DefaultDomainContext(
                pipeline=cast(PipelineBaseProtocol, self.pipeline)
            ),
            infrastructure=DefaultInfrastructureContext(output_dir=target_dir),
            artifacts=DefaultArtifactContext(artifact_store=artifact_store),
            config_provider=getattr(self.pipeline, "_build_config_provider")(),
        )
        runtime_context = StageRuntimeContext(
            context=cast(StageContextProtocol, stage_context),
            options=resolved_options,
        )
        return stage_context, runtime_context


class StageExecutor:
    """Executes stage descriptors using :class:`StageFactory`."""

    def __init__(
        self,
        pipeline: ChemblApplicationContract,
        *,
        factory: StageFactory | None = None,
    ) -> None:
        definition = getattr(pipeline, "pipeline_definition", None)
        stage_factory_cls = (
            getattr(definition, "stage_factory", None) if definition else None
        )
        factory_cls = stage_factory_cls or StageFactory
        self.factory = factory or factory_cls(
            cast(PipelineBaseProtocol, pipeline)
        )

    def build(
        self,
        descriptors: tuple[StageDescriptor, ...],
        context: StageContext,
        options: StageExecutionOptions,
        stages: tuple[str, ...] | None = None,
    ) -> tuple[StageCommand, ...]:
        return self.factory.build(
            descriptors, cast(StageFactoryContext, context), options, stages
        )

    def run(
        self,
        descriptors: tuple[StageDescriptor, ...],
        context: StageContext,
        runtime: StageRuntimeContext,
        *,
        stages: tuple[str, ...] | None = None,
    ) -> Any:
        if runtime.options is None:
            msg = "Stage execution options are required"
            raise ValueError(msg)

        stage_plan = self.build(
            descriptors, context, runtime.options, stages
        )
        result: Any = None
        for stage in stage_plan:
            result = stage.execute(runtime).output
        return result


def register_pipeline(
    code: str, factory: Callable[[], ChemblApplicationContract]
) -> None:
    """Register a ChEMBL pipeline factory by short code."""

    _PIPELINE_REGISTRY[code] = factory


def get_pipeline_specs() -> dict[
    str, Callable[[], ChemblApplicationContract]
]:
    """Return a copy of registered pipeline factories."""

    return dict(_PIPELINE_REGISTRY)


def _filter_descriptors(
    descriptors: tuple[StageDescriptor, ...], stages: tuple[str, ...]
) -> tuple[StageDescriptor, ...]:
    return tuple(
        descriptor for descriptor in descriptors if descriptor.id in stages
    )


def build_extract_plan(
    pipeline: ChemblApplicationContract,
    output_dir: Path,
    *,
    run_tag: str | None = None,
    mode: str | None = None,
) -> tuple[StageDescriptor, ...]:
    """Construct an extract-only stage plan for a pipeline."""

    options = StageExecutionOptions(
        run_tag=run_tag, mode=mode, dry_run=False
    )
    context_builder = RuntimeContextBuilder(pipeline)
    context, runtime = context_builder.build(
        output_dir, options=options, run_tag=run_tag, mode=mode
    )

    if runtime.options is None:
        msg = "Stage execution options are required"
        raise ValueError(msg)

    descriptors = pipeline.build_stage_plan(context, runtime.options)
    context.descriptor = pipeline.build_descriptor()

    executor = StageExecutor(pipeline)
    executor.run(descriptors, context, runtime, stages=("extract",))
    return descriptors


def run_chembl_stage(
    pipeline: ChemblApplicationContract,
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
    context_builder = RuntimeContextBuilder(pipeline)
    context, runtime = context_builder.build(
        output_root, options=options, run_tag=run_tag, mode=mode
    )
    if df is not None:
        context.data_bucket.set(df)

    if descriptor is None and normalized_stage == "extract":
        descriptor = getattr(pipeline, "build_descriptor", lambda: None)()
    context.descriptor = descriptor

    descriptors = pipeline.build_stage_plan(context, options)
    descriptor_plan = _filter_descriptors(
        descriptors, (normalized_stage,)
    )

    if not descriptor_plan:
        raise ValueError(
            f"No stage plan available for stage '{normalized_stage}'"
        )

    executor = StageExecutor(pipeline)
    return executor.run(
        descriptor_plan, context, runtime, stages=(normalized_stage,)
    )
