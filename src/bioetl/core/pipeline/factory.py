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
    WriteResult,
)


class StageFactory:
    """Factory that builds a sequence of :class:`StageCommand` objects."""

    def __init__(
        self,
        pipeline: PipelineStagesProtocol | PipelineBaseProtocol,
    ) -> None:
        self.pipeline = pipeline

    def build(
        self,
        descriptors: Iterable[StageDescriptor],
        _context: StageFactoryContext,
        _options: StageExecutionOptions,
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
        def _handler(
            ctx: StageFactoryContext,
            runtime: StageRuntimeContext,
        ) -> StageResult:
            return self._execute_descriptor(ctx, runtime, descriptor)

        return StageCommand(
            name=descriptor.id,
            handler=_handler,
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
            context.data_bucket.set(df)
            return StageResult(name=descriptor.id, output=df)

        if kind == "transform":
            frame = context.data_bucket.require(stage=kind)
            df = self.pipeline.transform(frame, runtime.options)
            context.data_bucket.set(df)
            return StageResult(name=descriptor.id, output=df)

        if kind == "validate":
            frame = context.data_bucket.require(stage=kind)
            df = self.pipeline.validate(frame, runtime.options)
            context.data_bucket.set(df)
            return StageResult(name=descriptor.id, output=df)

        if kind == "save_results":
            frame = context.data_bucket.require(stage=kind)
            artifacts = context.artifact_store.get()
            result = self.pipeline.save_results(
                frame,
                artifacts,
                runtime.options,
            )
            if isinstance(result, WriteResult):
                context.artifact_store.set(result.artifacts)
            return StageResult(name=descriptor.id, output=result)

        msg = f"Unknown stage kind '{kind}'"
        raise ValueError(msg)


__all__ = ["StageFactory"]
