"""Билдер для сервисов метаданных рантайма."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from bioetl.core.pipeline.services import (
    MetadataRuntimeService,
    MetadataService,
    RunMetadataBuilder,
    default_metadata_runtime_service_factory,
)
from bioetl.core.runtime.metadata import MetadataCoordinator, MetadataRuntimeBuilderProtocol


@dataclass(slots=True)
class MetadataRuntimeBuilder(MetadataRuntimeBuilderProtocol):
    """Билдер для сервисов метаданных."""

    config: Any
    pipeline_code: str
    metadata_service: MetadataService | None = None
    metadata_service_factory: Callable[[MetadataCoordinator], MetadataService] | None = None
    metadata_runtime_service: MetadataRuntimeService | None = None
    metadata_runtime_service_factory: Callable[
        [MetadataCoordinator], MetadataRuntimeService
    ] | None = None
    run_metadata_builder: RunMetadataBuilder | None = None
    logs_directory_resolver: Callable[[Path], Path] | None = None

    def build(self) -> MetadataCoordinator:
        """Построить координатор метаданных."""

        logs_resolver = self.logs_directory_resolver or (lambda path: path)

        if self.metadata_runtime_service is not None:
            return MetadataCoordinator(
                metadata_runtime_service=self.metadata_runtime_service,
                logs_directory_resolver=logs_resolver,
            )

        factory = self.metadata_runtime_service_factory or default_metadata_runtime_service_factory(
            config=self.config,
            pipeline_code=self.pipeline_code,
            metadata_service=self.metadata_service,
            metadata_service_factory=self.metadata_service_factory,
            run_metadata_builder=self.run_metadata_builder,
            logs_directory_resolver=logs_resolver,
        )
        return MetadataCoordinator.from_factory(
            metadata_runtime_service_factory=factory,
            logs_directory_resolver=logs_resolver,
        )


__all__ = ["MetadataRuntimeBuilder"]
