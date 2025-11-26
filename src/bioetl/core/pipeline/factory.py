"""StageFactory for building pipeline stage plans."""
from __future__ import annotations

from collections.abc import Iterable, Sequence

from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    PipelineStagesProtocol,
    StageCommand,
    StageDescriptor,
    StageExecutionOptions,
    StageFactoryContext,
    StageResult,
    StageRuntimeContext,
    WriteArtifacts,
    WriteResult,
)


class StageFactory:
    """Factory that builds a sequence of :class:`StageCommand` objects."""

    def __init__(self, pipeline: PipelineStagesProtocol | PipelineBaseProtocol) -> None:
        self.pipeline = pipeline

    def build(
        self,
        descriptors: Iterable[StageDescriptor],
        context: StageFactoryContext,
        options: StageExecutionOptions,
        stages: Sequence[str] | None = None,
    ) -> tuple[StageCommand, ...]:
        """Build a stage plan from descriptors."""

        descriptors = tuple(descriptors)
        descriptor_map = {descriptor.id: descriptor for descriptor in descriptors}
        selected: list[StageDescriptor]
        if stages is None:
            selected = list(descriptors)
        else:
            selected = []
            for stage in stages:
                descriptor = descriptor_map.get(stage)
                if descriptor is None:
                    msg = f"Unknown stage '{stage}'"
                    raise ValueError(msg)
                selected.append(descriptor)

        return tuple(self._build_stage(descriptor) for descriptor in selected)

    def _build_stage(self, descriptor: StageDescriptor) -> StageCommand:
        return StageCommand(
            name=descriptor.id,
            handler=lambda ctx, runtime, d=descriptor: self._execute_descriptor(ctx, runtime, d),
            description=descriptor.kind,
        )

    def _execute_descriptor(
        self,
        context: StageFactoryContext,
        runtime: StageRuntimeContext,
        descriptor: StageDescriptor,
    ) -> StageResult:
        runtime.descriptor = descriptor
        context.descriptor = descriptor
        kind = descriptor.kind
        if kind == "extract":
            df = self.pipeline.extract(descriptor, runtime.options)
            context.set_current_df(df)
            return StageResult(name=descriptor.id, output=df)

        if kind == "transform":
            frame = context.require_current_df(stage=kind)
            df = self.pipeline.transform(frame, runtime.options)
            context.set_current_df(df)
            return StageResult(name=descriptor.id, output=df)

        if kind == "validate":
            frame = context.require_current_df(stage=kind)
            df = self.pipeline.validate(frame, runtime.options)
            context.set_current_df(df)
            return StageResult(name=descriptor.id, output=df)

        if kind == "save_results":
            frame = context.require_current_df(stage=kind)
            artifacts = context.get_artifacts()
            result = self.pipeline.save_results(frame, artifacts, runtime.options)
            if isinstance(result, WriteResult):
                context.set_artifacts(result.artifacts)
            return StageResult(name=descriptor.id, output=result)

        msg = f"Unknown stage kind '{kind}'"
        raise ValueError(msg)


__all__ = ["StageFactory"]
