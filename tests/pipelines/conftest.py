from __future__ import annotations

from typing import Callable, Mapping
from unittest.mock import MagicMock

import pytest

from bioetl.core.pipeline.types import (
    ClientNamespace,
    EnricherEntity,
    ChemblEntity,
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
        clients: Mapping[str | ClientNamespace, Mapping[object, object]] | None = None,
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

        class _ClientFactoryStub:
            def __init__(self, mapping: Mapping[object, object], normalizer: Callable[[object], str]):
                self._normalize = normalizer
                self._mapping = {normalizer(key): value for key, value in mapping.items()}

            def create(self, entity: object) -> object:
                return self._mapping[self._normalize(entity)]

            def __getattr__(self, name: str) -> Callable[[], object]:  # pragma: no cover - passthrough
                if name in self._mapping:
                    return lambda: self._mapping[name]
                raise AttributeError(name)

        factories: dict[str, object] = {}
        for namespace, mapping in (clients or {}).items():
            normalized_ns = namespace.value if isinstance(namespace, ClientNamespace) else str(namespace)
            if normalized_ns == ClientNamespace.CHEMBL.value:
                factories[normalized_ns] = _ClientFactoryStub(mapping, lambda value: ChemblEntity(value).value)
            elif normalized_ns == ClientNamespace.ENRICHER.value:
                factories[normalized_ns] = _ClientFactoryStub(mapping, lambda value: EnricherEntity(value).value)
            else:
                msg = f"Unsupported namespace for stub factory: {normalized_ns}"
                raise ValueError(msg)

        return StageContext(
            execution=execution,
            domain=domain,
            infrastructure=infrastructure,
            artifacts=artifacts,
            config_provider=lambda key, cfg=cfg: cfg[key],
            clients=client_context,
            client_factories=factories,
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
