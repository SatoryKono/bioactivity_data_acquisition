from __future__ import annotations

from typing import Callable, Mapping
from unittest.mock import MagicMock

import pytest

from bioetl.core.pipeline.types import StageContext, StageExecutionOptions, StageRuntimeContext


@pytest.fixture
def stage_context_factory() -> Callable[..., StageContext]:
    def _build(
        *,
        logger: MagicMock | None = None,
        clients: Mapping[str, object] | None = None,
        config: Mapping[str, object] | None = None,
    ) -> StageContext:
        return StageContext(
            logger=logger or MagicMock(),
            request_id="req-1",
            trace_id="trace-1",
            clients=clients or {},
            config=config or {},
        )

    return _build


@pytest.fixture
def runtime_context_factory() -> Callable[..., StageRuntimeContext]:
    def _build(
        *,
        options: StageExecutionOptions | None = None,
        input_data: object | None = None,
        attributes: dict[str, object] | None = None,
    ) -> StageRuntimeContext:
        return StageRuntimeContext(
            options=options or StageExecutionOptions(run_tag=None, mode=None),
            input_data=input_data,
            attributes=attributes or {},
        )

    return _build
