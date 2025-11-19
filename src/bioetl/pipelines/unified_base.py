"""Unified pipeline base that composes the shared mixins."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from bioetl.chembl.common.descriptor import (
    ChemblDescriptorBuilderMixin,
    ChemblDescriptorSpec,
    ChemblPipelineBase,
)
from bioetl.config.runtime import QCReportRuntimeOptions
from bioetl.core.pipeline import PipelineStagesProtocol, RunResult

from .mixins import (
    BatchIdExtractionMixin,
    IOArtifactsMixin,
    LoggingMixin,
    NestedSerializerMixin,
    PaginatedExtractorMixin,
    RecordNormalizationMixin,
    ReleaseHandshakeMixin,
    SchemaValidationMixin,
    TransformMixin,
)


@runtime_checkable
class ChemblPipelineContract(PipelineStagesProtocol, Protocol):
    """Protocol implemented by all ChEMBL pipelines."""

    actor: str
    id_column: str | None

    def descriptor_spec(self) -> ChemblDescriptorSpec[Any]: ...


class UnifiedPipelineBase(
    LoggingMixin,
    ReleaseHandshakeMixin,
    PaginatedExtractorMixin,
    SchemaValidationMixin,
    RecordNormalizationMixin,
    NestedSerializerMixin,
    BatchIdExtractionMixin,
    TransformMixin,
    IOArtifactsMixin,
    ChemblDescriptorBuilderMixin,
    ChemblPipelineBase,
    ChemblPipelineContract,
):
    """ChEMBL-focused pipeline base that wires mixins into the public contract.

    Наследники получают единый жизненный цикл `extract → transform → validate →
    write` без переопределения ``run``. Каждый подключённый mixin отвечает за
    конкретный контракт: `LoggingMixin` — за структурированные логи стадий,
    `ReleaseHandshakeMixin` — за handshake и запись release, `BatchIdExtractionMixin`
    — за `extract_by_ids`, `TransformMixin` — за трансформационный конвейер, а
    `IOArtifactsMixin` — за детерминированную запись артефактов. Дочерним
    пайплайнам достаточно реализовать `build_descriptor` и при необходимости
    переопределить отдельные хуки (`pre_transform`, `domain_enrich`,
    `augment_metadata` и т. д.). Stage-runner (`bioetl.pipelines.chembl.stage_runner`)
    использует этот базовый класс и `PipelineStagesProtocol`, чтобы собирать
    частичные планы стадий; покрытие обеспечивается тестами в
    ``tests/bioetl/pipelines/chembl/test_stage_runner.py`` и
    ``tests/bioetl/pipelines/test_unified_base.py``.
    """

    def run(
        self,
        output_dir: Path,
        *,
        extended: bool = False,
        include_correlation: bool = False,
        include_qc_metrics: bool = False,
        qc_reports: QCReportRuntimeOptions | None = None,
        qc_thresholds: Mapping[str, float] | None = None,
        fail_on_qc_violation: bool = False,
    ) -> RunResult:
        """Execute the unified ETL flow with deterministic output artifacts.

        Parameters mirror the public CLI flags и позволяют управлять
        дополнительными отчётами (корреляции, QC метрики) без изменения
        пользовательского контракта `PipelineBase.run`.
        """
        return super().run(
            output_dir,
            extended=extended,
            include_correlation=include_correlation,
            include_qc_metrics=include_qc_metrics,
            qc_reports=qc_reports,
            qc_thresholds=qc_thresholds,
            fail_on_qc_violation=fail_on_qc_violation,
        )

    # Hooks -----------------------------------------------------------------

    def prepare_run(self) -> None:  # pragma: no cover - optional hook
        """Hook invoked before the extract stage begins."""

    def finalize_run(self, result: RunResult | None) -> None:  # pragma: no cover
        """Hook invoked after the write stage completes."""
