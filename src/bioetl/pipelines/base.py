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
        logger.info("pipeline_initialized", pipeline=config.pipeline.name, run_id=run_id)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
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
        return self.output_writer.write(df, output_path, extended=extended)

    def run(self, output_path: Path, extended: bool = False) -> OutputArtifacts:
        """Запускает полный пайплайн: extract → transform → validate → export."""
        logger.info("pipeline_started", pipeline=self.config.pipeline.name)

        try:
            # Extract
            df = self.extract()
            logger.info("extraction_completed", rows=len(df))

            # Transform
            df = self.transform(df)
            logger.info("transformation_completed", rows=len(df))

            # Validate
            df = self.validate(df)
            logger.info("validation_completed", rows=len(df))

            # Export
            artifacts = self.export(df, output_path, extended=extended)
            logger.info("pipeline_completed", artifacts=len(artifacts))

            return artifacts

        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            raise

