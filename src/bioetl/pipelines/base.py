"""Base pipeline class."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import OutputArtifacts, UnifiedOutputWriter

logger = UnifiedLogger.get(__name__)


class PipelineBase(ABC):
    """Базовый класс для всех пайплайнов."""

    def __init__(self, config: PipelineConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        self.output_writer = UnifiedOutputWriter(run_id)
        self.validation_issues: list[dict[str, Any]] = []
        self.qc_metrics: dict[str, Any] = {}
        self.qc_summary_data: dict[str, Any] = {}
        self.qc_missing_mappings = pd.DataFrame()
        self.qc_enrichment_metrics = pd.DataFrame()
        logger.info("pipeline_initialized", pipeline=config.pipeline.name, run_id=run_id)

    _SEVERITY_LEVELS: dict[str, int] = {"info": 0, "warning": 1, "error": 2, "critical": 3}

    def record_validation_issue(self, issue: dict[str, Any]) -> None:
        """Store a validation issue for later QC reporting."""

        issue = issue.copy()
        issue.setdefault("metric", "validation_issue")
        issue.setdefault("severity", "info")
        self.validation_issues.append(issue)

    def _severity_value(self, severity: str) -> int:
        """Convert severity label to comparable integer."""

        return self._SEVERITY_LEVELS.get(severity.lower(), 0)

    def _should_fail(self, severity: str) -> bool:
        """Determine if the given severity breaches the configured threshold."""

        threshold = self.config.qc.severity_threshold.lower()
        return self._severity_value(severity) >= self._severity_value(threshold)

    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Извлекает данные из источника."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформирует данные."""
        pass

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидирует данные через Pandera."""
        # Placeholder for now
        return df

    def export(
        self,
        df: pd.DataFrame,
        output_path: Path,
        extended: bool = False,
    ) -> OutputArtifacts:
        """Экспортирует данные с QC отчетами."""
        logger.info("exporting_data", path=output_path, rows=len(df))
        return self.output_writer.write(
            df,
            output_path,
            extended=extended,
            issues=self.validation_issues,
            qc_metrics=self.qc_metrics,
            qc_summary=self.qc_summary_data,
            qc_missing_mappings=self.qc_missing_mappings,
            qc_enrichment_metrics=self.qc_enrichment_metrics,
        )

    def run(
        self, output_path: Path, extended: bool = False, *args: Any, **kwargs: Any
    ) -> OutputArtifacts:
        """Запускает полный пайплайн: extract → transform → validate → export."""
        logger.info("pipeline_started", pipeline=self.config.pipeline.name)

        try:
            # Extract
            df = self.extract(*args, **kwargs)
            logger.info("extraction_completed", rows=len(df))

            # Transform
            df = self.transform(df)
            logger.info("transformation_completed", rows=len(df))

            # Validate
            df = self.validate(df)
            logger.info("validation_completed", rows=len(df))

            # Export
            artifacts = self.export(df, output_path, extended=extended)
            logger.info("pipeline_completed", artifacts=str(artifacts.dataset))

            return artifacts

        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            raise

