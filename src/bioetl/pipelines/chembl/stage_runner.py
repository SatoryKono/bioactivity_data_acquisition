from __future__ import annotations

"""Stage runner utilities and registry for ChEMBL pipelines."""

from collections.abc import Callable
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.types import (
    PipelineStageCommand,
    PipelineStagesProtocol,
    StageContext,
    StageExecutionOptions,
)
from bioetl.pipelines.chembl.common import ChemblPipelineContract

_PIPELINE_REGISTRY: dict[str, Callable[[], ChemblPipelineContract]] = {}

__all__ = ["register_pipeline", "get_pipeline_specs", "build_extract_plan", "StageRunner"]


@dataclass(slots=True)
class StageAlias:
    stage: str
    handler: Callable[[Any, dict[str, Any]], Any]


class StageRunner:
    """Backward-compatible runner for executing individual pipeline stages."""

    def __init__(self, pipeline: PipelineStagesProtocol) -> None:
        self.pipeline = pipeline
        self._aliases: dict[str, StageAlias] = {}

    def register_alias(self, alias: str, stage: str) -> None:
        self._aliases[alias] = StageAlias(stage=stage, handler=self._resolve_stage(stage))

    def _resolve_stage(self, stage: str) -> Callable[[Any, dict[str, Any]], Any]:
        def _runner(pipeline: Any, options: dict[str, Any]) -> Any:
            method = getattr(pipeline, stage)
            if stage == "write":
                return method(options.get("df"), Path(options["output_dir"]), extended=options.get("extended", False))
            if stage == "run":
                return method(Path(options["output_dir"]), **{k: v for k, v in options.items() if k != "output_dir"})
            if "df" in options:
                return method(options["df"])
            return method()

        return _runner

    def run_stage(self, name: str, **options: Any) -> Any:
        alias = self._aliases.get(name)
        handler = alias.handler if alias else self._resolve_stage(name)
        return handler(self.pipeline, options)


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
    options = StageExecutionOptions(run_tag=run_tag, mode=mode)
    target_dir, artifacts = pipeline.plan_run_artifacts(output_dir, run_tag, mode)  # type: ignore[arg-type]
    # plan_run_artifacts is implemented on UnifiedPipelineBase; we keep typing narrow for contract users.
    return StageContext(
        pipeline=pipeline,  # type: ignore[arg-type]
        output_dir=target_dir,
        logger=None,  # type: ignore[arg-type]
        run_id=getattr(pipeline, "run_id", ""),
        run_tag=run_tag,
        mode=mode,
        descriptor=None,
        artifacts=artifacts,
    )


def build_extract_plan(
    pipeline: ChemblPipelineContract,
    output_dir: Path,
    *,
    run_tag: str | None = None,
    mode: str | None = None,
) -> tuple[PipelineStageCommand, ...]:
    """Construct an extract-only stage plan for a pipeline."""

    context = _build_stage_context(pipeline, output_dir, run_tag=run_tag, mode=mode)
    options = StageExecutionOptions(run_tag=run_tag, mode=mode, dry_run=False)
    factory = StageFactory(pipeline)  # type: ignore[arg-type]
    plan = factory.build(context, options, stages=("extract",))
    # Seed descriptor so the pipeline can reuse it during execution.
    context.descriptor = pipeline.build_descriptor()
    for command in plan:
        if command.name == "extract":
            command.handler(context, options)
    return plan
