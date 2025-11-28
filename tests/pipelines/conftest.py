from __future__ import annotations

from typing import Callable, Mapping
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from bioetl.core.pipeline.types import (
    DefaultArtifactContext,
    DefaultDomainContext,
    DefaultExecutionContext,
    DefaultInfrastructureContext,
    StageContext,
    StageExecutionOptions,
    StageRuntimeContext,
)


@pytest.fixture
def stage_context_factory() -> Callable[..., StageContext]:
    class _MappingClientContext:
        """Lightweight client context backed by a simple name mapping."""

        def __init__(self, mapping: Mapping[str, object]) -> None:
            self._mapping = mapping

        def get_client(
            self, name: str, entity: object | None = None
        ) -> object:
            _ = entity
            return self._mapping[name]

    def _build(
        *,
        logger: MagicMock | None = None,
        clients: Mapping[str, object] | None = None,
        config: Mapping[str, object] | None = None,
        metric_emitter: Callable[..., None] | None = None,
    ) -> StageContext:
        cfg = config or {}
        execution = DefaultExecutionContext(
            logger=logger or MagicMock(),
            request_id="req-1",
            trace_id="trace-1",
        )
        domain = DefaultDomainContext()
        infrastructure = DefaultInfrastructureContext()
        artifacts = DefaultArtifactContext()

        raw_clients = clients or {}
        client_mapping: dict[str, object] = {}
        for name, client in raw_clients.items():
            if hasattr(client, "process"):
                client_mapping[name] = SimpleNamespace(
                    process=client.process
                )
            else:
                client_mapping[name] = client

        client_context = _MappingClientContext(client_mapping)

        return StageContext(
            execution=execution,
            domain=domain,
            infrastructure=infrastructure,
            artifacts=artifacts,
            config_provider=lambda key, cfg=cfg: cfg[key],
            clients=client_context,
            metric_emitter=metric_emitter,
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
