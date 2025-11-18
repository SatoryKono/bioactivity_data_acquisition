"""Unified pipeline base that composes the shared mixins."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from bioetl.chembl.common.descriptor import ChemblPipelineBase
from bioetl.config.runtime import QCReportRuntimeOptions
from bioetl.core.pipeline import RunResult

from .mixins import (
    IOArtifactsMixin,
    LoggingMixin,
    PaginatedExtractorMixin,
    ReleaseHandshakeMixin,
    SchemaValidationMixin,
    TransformMixin,
)


class UnifiedPipelineBase(
    LoggingMixin,
    ReleaseHandshakeMixin,
    PaginatedExtractorMixin,
    SchemaValidationMixin,
    TransformMixin,
    IOArtifactsMixin,
    ChemblPipelineBase,
):
    """Base class that will be adopted by the refactored ChEMBL pipelines."""

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
