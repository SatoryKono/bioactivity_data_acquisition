"""Template pipeline implementation with shared orchestration concerns."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from bioetl.core.logging import UnifiedLogger
from bioetl.core.types import RunResult
from bioetl.pipelines.base import PipelineBase


class UnifiedPipelineBase(PipelineBase):
    """Pipeline mixin providing default :meth:`run` orchestration.

    Subclasses can override individual stages while reusing logging, optional
    quality-control hooks, and Pandera-based validation hooks.
    """

    def __init__(self, run_id: str, *, logger: UnifiedLogger | None = None) -> None:
        super().__init__(run_id, logger=logger)

    def extract(self, **options: Any) -> Any:  # pragma: no cover - to be implemented in subclasses
        raise NotImplementedError

    def transform(self, data: Any, **options: Any) -> Any:
        return data

    def validate(self, data: Any, **options: Any) -> Any:
        return data

    def write(self, data: Any, output_dir: Path, **options: Any) -> RunResult:
        output_dir.mkdir(parents=True, exist_ok=True)
        return RunResult(status="noop", output_path=output_dir, records_processed=0)

    def before_run(self, options: Mapping[str, Any] | None = None) -> None:
        """Hook executed before the pipeline stages start."""

    def after_run(self, result: RunResult) -> None:
        """Hook executed after the pipeline has completed."""

    def run(
        self,
        output_dir: Path,
        *,
        extended: bool = False,
        include_qc_metrics: bool = False,
        **options: Any,
    ) -> RunResult:
        """Execute the full pipeline with logging and optional QC hooks."""

        self.logger.info(
            "run_start",
            run_id=self.run_id,
            output_dir=str(output_dir),
            extended=extended,
            include_qc_metrics=include_qc_metrics,
        )
        self.before_run(options or {})

        extracted = self.extract(extended=extended, **options)
        transformed = self.transform(extracted, extended=extended, **options)
        validated = self.validate(transformed, include_qc_metrics=include_qc_metrics, **options)
        result = self.write(validated, output_dir, include_qc_metrics=include_qc_metrics, **options)

        self.logger.info(
            "run_complete",
            run_id=self.run_id,
            status=result.status,
            output_path=str(result.output_path) if result.output_path else None,
            records_processed=result.records_processed,
        )
        self.after_run(result)
        return result

    def emit_qc_metrics(self, data: Any, output_dir: Path) -> None:
        """Placeholder for QC metrics generation when :pycode:`include_qc_metrics=True`."""

        self.logger.info("qc_metrics_skipped", output_dir=str(output_dir))


__all__ = ["UnifiedPipelineBase"]
