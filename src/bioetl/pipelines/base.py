"""Base pipeline class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pandera as pa
from pandera import errors as pa_errors

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import OutputArtifacts, UnifiedOutputWriter
from bioetl.core.qc import (
    SeverityLevel,
    build_qc_report,
    evaluate_thresholds,
    should_fail,
)

logger = UnifiedLogger.get(__name__)


class PipelineBase(ABC):
    """Базовый класс для всех пайплайнов."""

    def __init__(self, config: PipelineConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        self.output_writer = UnifiedOutputWriter(run_id)
        self.validation_schemas = self.get_validation_schemas()
        logger.info("pipeline_initialized", pipeline=config.pipeline.name, run_id=run_id)

    def get_validation_schemas(self) -> Mapping[str, pa.DataFrameModel]:
        """Return mapping of validation schemas per stage."""

        return {}

    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Извлекает данные из источника."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформирует данные."""
        pass

    def _validate_stage(self, stage: str, df: pd.DataFrame) -> pd.DataFrame:
        """Validate DataFrame using configured schema for stage."""

        schema = self.validation_schemas.get(stage)
        if schema is None or df.empty:
            return df

        try:
            validated_df = schema.validate(df, lazy=True)
            logger.info("schema_validated", stage=stage, rows=len(validated_df))
            return validated_df
        except pa_errors.SchemaError as exc:
            logger.error("schema_validation_failed", stage=stage, error=str(exc))
            raise

    def validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate incoming input DataFrame."""

        return self._validate_stage("input", df)

    def validate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate raw extracted DataFrame."""

        return self._validate_stage("raw", df)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate normalized DataFrame before export."""

        return self._validate_stage("normalized", df)

    def compute_qc_metrics(self, df: pd.DataFrame) -> dict[str, float]:
        """Compute QC metrics for the pipeline output."""

        return {}

    def evaluate_qc(self, metrics: Mapping[str, float]) -> list:
        """Evaluate QC metrics against configured thresholds."""

        if not metrics or not self.config.qc.enabled:
            return []

        violations = evaluate_thresholds(metrics, self.config.qc)
        severity_threshold = SeverityLevel.from_str(self.config.qc.severity_threshold)

        for violation in violations:
            logger.warning("qc_threshold_violation", message=violation.message)

        if should_fail(violations, severity_threshold):
            message = "; ".join(v.message for v in violations)
            logger.error("qc_threshold_exceeded", threshold=str(severity_threshold), details=message)
            raise RuntimeError(f"QC thresholds violated: {message}")

        return violations

    def create_qc_summary(
        self, metrics: Mapping[str, float], violations: list
    ) -> pd.DataFrame | None:
        """Create DataFrame summarizing QC metrics for export."""

        if not metrics:
            return None

        thresholds = self.config.qc.thresholds if self.config.qc else {}
        return build_qc_report(metrics, thresholds, violations)

    def export(
        self,
        df: pd.DataFrame,
        output_path: Path,
        extended: bool = False,
        qc_summary: pd.DataFrame | None = None,
    ) -> OutputArtifacts:
        """Экспортирует данные с QC отчетами."""
        logger.info("exporting_data", path=output_path, rows=len(df))
        return self.output_writer.write(
            df, output_path, extended=extended, qc_summary=qc_summary
        )

    def run(
        self, output_path: Path, extended: bool = False, *args: Any, **kwargs: Any
    ) -> OutputArtifacts:
        """Запускает полный пайплайн: extract → transform → validate → export."""
        logger.info("pipeline_started", pipeline=self.config.pipeline.name)

        try:
            # Extract
            raw_df = self.extract(*args, **kwargs)
            logger.info("extraction_completed", rows=len(raw_df))

            raw_df = self.validate_raw(raw_df)

            # Transform
            df = self.transform(raw_df)
            logger.info("transformation_completed", rows=len(df))

            # Validate normalized dataset
            df = self.validate(df)
            logger.info("validation_completed", rows=len(df))

            metrics = self.compute_qc_metrics(df)
            if metrics:
                logger.info("qc_metrics_computed", metrics=metrics)
            violations = self.evaluate_qc(metrics)
            qc_summary = self.create_qc_summary(metrics, violations)

            # Export
            artifacts = self.export(
                df, output_path, extended=extended, qc_summary=qc_summary
            )
            logger.info("pipeline_completed", artifacts=str(artifacts.dataset))

            return artifacts

        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            raise

