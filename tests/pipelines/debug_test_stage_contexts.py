
from __future__ import annotations

from unittest.mock import MagicMock
import pytest

from bioetl.core.pipeline.runtime import StagePlanExecutor
from bioetl.core.pipeline.types import (
    ChemblEntity,
    ClientNamespace,
    PipelineStageCommand,
    StageExecutionOptions,
)

# Replicating the fixtures locally for the debug test since they are in conftest.py
# I need to import them or rely on pytest to find them if I put this file in the same dir.
# Since it's in tests/pipelines, pytest should find conftest.py fixtures automatically.

def test_stage_context_uses_expected_client_debug(
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
    durations, error = executor.execute((command,), context, options, runtime_context=runtime)

    print(f"DEBUG: durations={durations}, error={error}")
    
    if error:
        pytest.fail(f"Executor failed with error: {error}")

    client_primary.process.assert_called_once_with(5)
    client_secondary.process.assert_not_called()
