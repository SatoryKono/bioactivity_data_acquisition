from __future__ import annotations

"""Стратегии извлечения данных для ChEMBL-пайплайнов."""

from typing import Iterable, Protocol, TYPE_CHECKING

import pandas as pd

from bioetl.core.pipeline.unified import (
    ChemblExtractionDescriptor,
    ChemblExtractionServiceDescriptor,
)

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.core.pipeline.types import StageExecutionOptions
    from .base import ChemblCommonPipeline


class ExtractionStrategy(Protocol):
    """Интерфейс стратегии извлечения."""

    def supports(self, descriptor_type: str) -> bool:
        """Проверить поддержку дескриптора указанного типа."""

    def run(
        self,
        pipeline: "ChemblCommonPipeline",
        descriptor: (
            ChemblExtractionServiceDescriptor | ChemblExtractionDescriptor | None
        ),
        options: "StageExecutionOptions",
    ) -> pd.DataFrame:
        """Запустить извлечение."""


class DataclassExtractionStrategy:
    """Извлечение через dataclass-дескрипторы."""

    def supports(self, descriptor_type: str) -> bool:
        return descriptor_type == "dataclass"

    def run(
        self,
        pipeline: "ChemblCommonPipeline",
        descriptor: (
            ChemblExtractionServiceDescriptor | ChemblExtractionDescriptor | None
        ),
        options: "StageExecutionOptions",
    ) -> pd.DataFrame:
        descriptor = descriptor or pipeline.build_descriptor()
        if isinstance(descriptor, ChemblExtractionDescriptor):
            return pipeline._extract_with_dataclass_descriptor(descriptor, options)

        if pipeline.validation_service:
            return pipeline.validation_service.empty_frame()
        return pd.DataFrame()


class ServiceExtractionStrategy:
    """Извлечение через сервис-дескрипторы."""

    def supports(self, descriptor_type: str) -> bool:
        return descriptor_type == "service"

    def run(
        self,
        pipeline: "ChemblCommonPipeline",
        descriptor: (
            ChemblExtractionServiceDescriptor | ChemblExtractionDescriptor | None
        ),
        options: "StageExecutionOptions",
    ) -> pd.DataFrame:
        ids = (
            pipeline.config.get("ids")
            if isinstance(pipeline.config, dict)
            else None
        )
        descriptor = descriptor or pipeline.build_descriptor()

        if isinstance(descriptor, ChemblExtractionServiceDescriptor):
            frame, _stats = pipeline.run_descriptor_extraction(
                descriptor,
                ids if isinstance(ids, Iterable) else None,
                summary_event=f"{pipeline.entity_name}_summary",
                batch_size=int(
                    pipeline._get_config_value("sources.chembl.batch_size")
                ),
            )
            if frame.empty and pipeline.validation_service:
                return pipeline.validation_service.empty_frame()
            return frame

        if pipeline.validation_service:
            return pipeline.validation_service.empty_frame()
        return pd.DataFrame()


class ExtractionStrategyFactory:
    """Фабрика стратегий извлечения."""

    def __init__(
        self, strategies: Iterable[ExtractionStrategy] | None = None
    ) -> None:
        self._strategies = list(
            strategies
            or [DataclassExtractionStrategy(), ServiceExtractionStrategy()]
        )

    def get(self, descriptor_type: str) -> ExtractionStrategy:
        """Получить стратегию по типу дескриптора без ветвлений."""

        try:
            return next(
                strategy
                for strategy in self._strategies
                if strategy.supports(descriptor_type)
            )
        except StopIteration as exc:  # pragma: no cover - защита от неверных типов
            raise ValueError(
                f"Unsupported descriptor type: {descriptor_type!r}"
            ) from exc


__all__ = [
    "ExtractionStrategy",
    "DataclassExtractionStrategy",
    "ServiceExtractionStrategy",
    "ExtractionStrategyFactory",
]
