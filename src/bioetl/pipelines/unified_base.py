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
from bioetl.core.pipeline.orchestration import (
    PipelineStagesProtocol,
    RunResult,
)

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

    Наследники получают единый жизненный цикл
    ``prepare_run → extract → transform → validate → save_results →
    finalize_run`` без переопределения :meth:`run`. Каждый подключённый
    mixin отвечает за конкретный контракт: :class:`LoggingMixin` — за
    структурированные логи стадий, :class:`ReleaseHandshakeMixin` — за
    handshake и запись release, :class:`BatchIdExtractionMixin` — за
    :meth:`extract_by_ids`, :class:`TransformMixin` — за
    трансформационный конвейер, а :class:`IOArtifactsMixin` — за
    детерминированную запись артефактов.

    Stage-runner (``bioetl.pipelines.chembl.stage_runner``) использует
    этот базовый класс и :class:`PipelineStagesProtocol`, чтобы собирать
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
        """Execute the unified ETL lifecycle and return collected artefacts.

        This method preserves the public contract of
        :meth:`bioetl.core.pipeline.PipelineBase.run` and delegates to the
        base implementation without altering the orchestration logic.
        The lifecycle is executed as
        ``prepare_run → extract → transform → validate → save_results →
        finalize_run``.

        Parameters mirror the public CLI flags and control optional
        artefacts (correlation reports, QC metrics) while keeping the
        underlying contract stable. All logging, stage duration tracking
        and QC aggregation are handled by the base pipeline and reflected
        in the returned :class:`RunResult`.
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
        """Hook invoked before the extract stage begins.

        Subclasses may override this to perform per-run initialisation
        such as warming caches or registering additional resources. The
        default implementation delegates to the base pipeline so that
        shared behaviour remains centralised.
        """
        super().prepare_run()

    def finalize_run(
        self, result: RunResult | None
    ) -> None:  # pragma: no cover
        """Hook invoked after the write stage completes.

        Subclasses may override this to perform additional cleanup or
        telemetry. The default implementation delegates to the base
        pipeline so that finalisation logic, including retention and
        client cleanup, stays centralised.
        """
        super().finalize_run(result)
