from __future__ import annotations

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import (
    StageCommand,
    StageContext,
    StageExecutionOptions,
    StageProtocol,
    StageResult,
    StageRuntimeContext,
)


def test_stage_command_implements_protocol_and_wraps_output() -> None:
    logger = UnifiedLogger.get("StageTypesTest")
    runtime_context = StageRuntimeContext(
        context=StageContext(logger=logger, request_id="t-1"),
        options=StageExecutionOptions(run_tag=None, mode=None),
    )

    command = StageCommand("demo", lambda ctx, runtime: "ok")

    assert isinstance(command, StageProtocol)
    result = command.execute(runtime_context)

    assert isinstance(result, StageResult)
    assert result.name == "demo"
    assert result.output == "ok"


def test_stage_command_preserves_stage_result() -> None:
    logger = UnifiedLogger.get("StageTypesTest")
    runtime_context = StageRuntimeContext(
        context=StageContext(logger=logger, request_id="t-2"),
        options=StageExecutionOptions(run_tag=None, mode=None),
    )

    result = StageResult(name="demo", output={"value": 1})
    command = StageCommand("demo", lambda ctx, runtime: result)

    assert command.execute(runtime_context) is result
