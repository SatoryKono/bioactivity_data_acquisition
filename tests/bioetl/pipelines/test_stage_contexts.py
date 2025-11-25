from __future__ import annotations

from unittest.mock import MagicMock

from bioetl.core.pipeline.runtime import StagePlanExecutor
from bioetl.core.pipeline.types import PipelineStageCommand


def test_stage_context_uses_expected_client(stage_context_factory, runtime_context_factory) -> None:
    client_primary = MagicMock()
    client_secondary = MagicMock()
    context = stage_context_factory(clients={"primary": client_primary, "secondary": client_secondary})
    runtime = runtime_context_factory(attributes={"payload": 5})

    command = PipelineStageCommand(
        "custom", lambda ctx, rt: ctx.get_client("primary").process(rt.attributes["payload"])
    )

    executor = StagePlanExecutor()
    executor.execute((command,), context, runtime, include_qc_metrics=False)

    client_primary.process.assert_called_once_with(5)
    client_secondary.process.assert_not_called()


def test_stage_context_emits_metrics(stage_context_factory, runtime_context_factory) -> None:
    emitter = MagicMock()
    context = stage_context_factory()
    context.metric_emitter = emitter
    runtime = runtime_context_factory()

    command = PipelineStageCommand("metric", lambda ctx, rt: ctx.emit_metric("items", 1, {"stage": "test"}))

    executor = StagePlanExecutor()
    executor.execute((command,), context, runtime, include_qc_metrics=False)

    emitter.assert_called_once_with("items", 1, {"stage": "test"})
