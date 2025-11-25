from __future__ import annotations

"""Общие утилиты и базовые классы для ChEMBL пайплайнов."""

from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from bioetl.core.pipeline.unified import ChemblExtractionDescriptor, ChemblPipelineBase
from bioetl.pipelines.chembl.common.descriptor import ConfigValidationError
from bioetl.pipelines.chembl.config_validator import ChemblConfigValidator
from bioetl.pipelines.chembl.entity_extractor import ChemblExtractor
from bioetl.pipelines.chembl.entity_writer import ChemblWriter


class ChemblEntityPipeline(ChemblPipelineBase):
    """Базовый класс для сущностей ChEMBL с общей валидацией."""

    entity_name: str = "chembl"
    required_sort_fields: Sequence[str] = ()

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        config_validator: ChemblConfigValidator | None = None,
        extractor: ChemblExtractor | None = None,
        writer: ChemblWriter | None = None,
    ) -> None:
        super().__init__(config, run_id=run_id)
        self.config_validator = config_validator or ChemblConfigValidator(
            entity_name=self.entity_name, required_sort_fields=self.required_sort_fields
        )
        self.extractor = extractor or ChemblExtractor()
        self.writer = writer or ChemblWriter()
        self.config_validator.validate(self.config)

    # ------------------------------------------------------------------
    # Общая конфигурация
    # ------------------------------------------------------------------
    def get_config_value(self, dotted_path: str) -> Any:
        return self.config_validator._get_config_value(self.config, dotted_path)

    # ------------------------------------------------------------------
    # Обработка стадий
    # ------------------------------------------------------------------
    def extract(self) -> pd.DataFrame:
        return self.extractor.extract(self, summary_event=f"{self.entity_name}_summary")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Тонкий слой для переопределения в наследниках.
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Дополнительные пользовательские проверки можно внедрить в наследниках.
        return df

    def write(self, df: pd.DataFrame, output_dir: Path, *, extended: bool = False) -> Path:
        return self.writer.write(self, df, output_dir, extended=extended)

    # ------------------------------------------------------------------
    def build_generic_descriptor(self) -> ChemblExtractionDescriptor:
        return self.extractor.build_generic_descriptor(self)

    # Наследники обязаны предоставить build_descriptor
    def build_descriptor(self) -> ChemblExtractionDescriptor:
        raise NotImplementedError
