"""Tests for pipeline stage type definitions."""
from __future__ import annotations

from pathlib import Path

from typing import cast

from bioetl.core.logging import UnifiedLogger
from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    ArtifactStore,
    DefaultArtifactContext,
    DefaultDomainContext,
    DefaultExecutionContext,
    DefaultInfrastructureContext,
    StageCommand,
    StageContext,
    StageExecutionOptions,
    StageProtocol,
    StageResult,
    StageRuntimeContext,
    StageContextProtocol,
)


def _build_stage_context(request_id: str) -> StageContext:
    logger = cast(UnifiedLogger, UnifiedLogger.get("StageTypesTest"))
    return StageContext(
        execution=DefaultExecutionContext(
            logger=logger,
            request_id=request_id,
        ),
        domain=DefaultDomainContext(pipeline=None),
        infrastructure=DefaultInfrastructureContext(
            output_dir=Path("/tmp/out"),
        ),
        artifacts=DefaultArtifactContext(
            artifact_store=ArtifactStore(
                WriteArtifacts()  # type: ignore[call-arg]
            ),
        ),
    )


def test_stage_command_implements_protocol_and_wraps_output() -> None:
    """Ensure StageCommand satisfies the StageProtocol and wraps
    return values."""
    context = _build_stage_context("t-1")
    runtime_context = StageRuntimeContext(
        context=cast(StageContextProtocol, context),
        options=StageExecutionOptions(run_tag=None, mode=None),
    )

    command = StageCommand("demo", lambda ctx, runtime: "ok")

    assert isinstance(command, StageProtocol)
    result = command.execute(runtime_context)

    assert isinstance(result, StageResult)
    assert result.name == "demo"
    assert result.output == "ok"


def test_stage_command_preserves_stage_result() -> None:
    """Ensure StageCommand returns StageResult objects directly."""
    context = _build_stage_context("t-2")
    runtime_context = StageRuntimeContext(
        context=cast(StageContextProtocol, context),
        options=StageExecutionOptions(run_tag=None, mode=None),
    )

    result = StageResult(name="demo", output={"value": 1})
    command = StageCommand("demo", lambda ctx, runtime: result)

    assert command.execute(runtime_context) is result
