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
from bioetl.config.loader import load_pipeline_config
from bioetl.config.models.models import PipelineConfig
from bioetl.config.runtime import QCReportRuntimeOptions
from bioetl.core.pipeline.orchestration import (
    PipelineStagesProtocol,
    RunResult,
)
from bioetl.core.logging import LogEvents

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

        Этот метод фиксирует публичный контракт для всех ChEMBL‑пайплайнов:
        ``prepare_run → extract → transform → validate → save_results →``
        ``finalize_run``. Внутри используется стандартный ``StageFactory`` из
        :class:`bioetl.core.pipeline.common.PipelineBaseCommon`, поэтому
        дополнительные стадии, такие как ``cleanup``, продолжают добавляться
        фабрикой, но основной поток остаётся неизменным. Параметры полностью
        соответствуют CLI и пробрасываются в базовую реализацию, которая
        агрегирует длительности стадий, сведения валидации и пути артефактов в
        :class:`RunResult`.
        """
        log = self.logger_for(stage="run")
        log.info(
            LogEvents.STAGE_RUN_START,
            extended=bool(extended),
            include_correlation=bool(include_correlation),
            include_qc_metrics=bool(include_qc_metrics),
        )
        result = super().run(
            output_dir,
            extended=extended,
            include_correlation=include_correlation,
            include_qc_metrics=include_qc_metrics,
            qc_reports=qc_reports,
            qc_thresholds=qc_thresholds,
            fail_on_qc_violation=fail_on_qc_violation,
        )
        log.info(
            LogEvents.STAGE_RUN_FINISH,
            stage_durations_ms=dict(result.stage_durations_ms),
            records=result.records,
        )
        return result

    def __init__(
        self,
        config: PipelineConfig | str | Path,
        run_id: str,
    ) -> None:
        """Normalise configuration inputs before delegating to the parent."""

        if isinstance(config, PipelineConfig):
            validated_config = config
        elif isinstance(config, (str, Path)):
            validated_config = load_pipeline_config(config)
        else:
            msg = (
                "config must be a PipelineConfig instance or path to a YAML file"
            )
            raise TypeError(msg)

        super().__init__(validated_config, run_id)

    # Hooks -----------------------------------------------------------------

    def prepare_run(self) -> None:  # pragma: no cover - optional hook
        """Lifecycle hook executed before :meth:`extract` is invoked.

        Реализации могут выполнять handshake с API, инициализировать кеши или
        проверять внешние зависимости. Ошибки из этого метода прерывают запуск,
        чтобы не скрывать проблемы до последующих стадий.
        """

    def finalize_run(
        self, result: RunResult | None
    ) -> None:  # pragma: no cover
        """Lifecycle hook executed after all stages complete.

        Вызывается даже при исключениях на стадиях (best-effort), поэтому
        реализации должны быть максимально защищёнными и не допускать
        скрытия исходной ошибки. ``result`` содержит итоговые артефакты, когда
        запись завершилась успешно, либо ``None`` при преждевременном выходе.
        """
