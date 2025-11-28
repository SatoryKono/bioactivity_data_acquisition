"""Базовые абстракции для тонких ChEMBL-пайплайнов."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol

import pandera as pa

from bioetl.core.pipeline.services import DefaultValidationService
from bioetl.core.pipeline.types import RunResult, StageContext, StageDescriptor, StageExecutionOptions
from bioetl.pipelines.chembl.common.base import ChemblCommonPipeline


class ChemblPipelineProtocol(Protocol):
    """Минимальный контракт для запуска ChEMBL-пайплайна."""

    def run(
        self,
        output_dir: Path,
        *,
        run_tag: str | None = None,
        mode: str | None = None,
        extended: bool = False,
        dry_run: bool | None = None,
        sample: int | None = None,
        limit: int | None = None,
        include_qc_metrics: bool = False,
        fail_on_schema_drift: bool = True,
        enable_validation: bool = True,
    ) -> RunResult:
        ...

    def configure(self, config: Mapping[str, Any]) -> None:
        ...

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        ...

    def get_release(self) -> str | None:
        ...


class ChemblPipeline(ChemblCommonPipeline):
    """Общий базовый класс для тонких ChEMBL-пайплайнов."""

    pipeline_code: str = "chembl_pipeline"

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        validator: pa.DataFrameSchema | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(config, run_id=run_id, **kwargs)
        if validator is not None:
            self.validator = validator
            self.validation_service = DefaultValidationService(validator)

    def configure(self, config: Mapping[str, Any]) -> None:
        """Обновить конфигурацию и повторно провалидировать базовые настройки."""

        self.config = config
        self._validate_common_config()

    def build_descriptor(self):
        """Использовать универсальное построение дескриптора ChEMBL."""

        return self._descriptor_factory.build(self.entity_name)

    def get_release(self) -> str | None:
        """Вернуть текущий номер релиза ChEMBL."""

        return getattr(self.extraction_service, "chembl_release", None)


__all__ = ["ChemblPipeline", "ChemblPipelineProtocol"]
