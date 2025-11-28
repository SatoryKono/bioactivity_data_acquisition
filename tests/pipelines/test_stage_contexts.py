"""Tests for stage context behavior."""
from __future__ import annotations

from unittest.mock import MagicMock

from bioetl.core.pipeline.runtime import StagePlanExecutor
from bioetl.clients.chembl.entities import ChemblEntity
from bioetl.core.pipeline.types import (
    ClientNamespace,
    PipelineStageCommand,
    StageExecutionOptions,
)


def test_stage_context_uses_expected_client(
    stage_context_factory, runtime_context_factory
) -> None:
    """Test that stage context correctly retrieves and uses clients."""
    client_primary = MagicMock()
    client_secondary = MagicMock()
    context = stage_context_factory(
        clients={
            ClientNamespace.CHEMBL: {
                ChemblEntity.ACTIVITY: client_primary,
                ChemblEntity.ASSAY: client_secondary,
            }
        }
    )
    runtime = runtime_context_factory(attributes={"payload": 5})

    command = PipelineStageCommand(
        "custom",
        lambda ctx, rt: ctx.get_client(
            ClientNamespace.CHEMBL, ChemblEntity.ACTIVITY
        ).process(
            rt.attributes["payload"]
        ),
    )

    executor = StagePlanExecutor()
    options = runtime.options or StageExecutionOptions(run_tag=None, mode=None)
    executor.execute((command,), context, options, runtime_context=runtime)

    client_primary.process.assert_called_once_with(5)
    client_secondary.process.assert_not_called()


def test_stage_context_emits_metrics(
    stage_context_factory, runtime_context_factory
) -> None:
    """Test that metrics are emitted through the context."""
    emitter = MagicMock()
    context = stage_context_factory(metric_emitter=emitter)
    runtime = runtime_context_factory()

    command = PipelineStageCommand(
        "metric",
        lambda ctx, rt: ctx.emit_metric(
            "items", 1, {"stage": "test"}
        ),
    )

    executor = StagePlanExecutor()
    options = runtime.options or StageExecutionOptions(run_tag=None, mode=None)
    executor.execute((command,), context, options, runtime_context=runtime)

    emitter.assert_called_once_with("items", 1, {"stage": "test"})
