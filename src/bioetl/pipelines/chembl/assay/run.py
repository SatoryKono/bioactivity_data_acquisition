"""Запуск ChEMBL Assay pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pandera as pa

from bioetl.core.io import PipelineOutputService
from bioetl.core.pipeline.services import DefaultValidationService
from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.types import (
    StageExecutionOptions,
    WriteResult,
)
from bioetl.pipelines.chembl.common.base import ChemblCommonPipeline
from bioetl.core.schemas import AssaySchema

class ChemblAssayPipeline(ChemblCommonPipeline):
    """Заготовка пайплайна для выгрузки assay."""

    entity_name = "assay"
    required_sort_fields = ("assay_chembl_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(
            config,
            run_id=run_id,
            descriptor_type="service",  # используем стандартный паттерн
        )
        self.validator = AssaySchema
        self.validation_service = DefaultValidationService(self.validator)

    def build_descriptor(self) -> Any:  # pragma: no cover - тонкий слой
        return super().build_descriptor()

    # ------------------------------------------------------------------
    # Stage hooks
    # ------------------------------------------------------------------
    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().pre_transform(df)
        df = self._normalize_nested_parameters(df)
        df = self._ensure_assay_class_mapping(df)
        df = self._ensure_target_integrity(df)
        return df

    def validate(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        df = super().validate(df, options)
        id_column = (
            "assay_chembl_id"
            if "assay_chembl_id" in df.columns
            else "assay_id"
        )
        if id_column in df.columns:
            missing = df[id_column].isna().sum()
            if missing:
                raise pa.errors.SchemaError(
                    schema=self.validator,
                    data=df,
                    message=f"Найдены пустые идентификаторы ассая: {missing}",
                )
        return df

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_service = PipelineOutputService(self.config)
        try:
            output_dir = (
                artifacts.data_path.parent
                if artifacts.data_path
                else Path.cwd()
            )
            return output_service.save(df, artifacts, output_dir)
        except ValueError:
            return super().save_results(df, artifacts, options)
        except Exception:  # pragma: no cover - fallback совместимости
            return super().save_results(df, artifacts, options)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _normalize_nested_parameters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Сериализация вложенных параметров в зависимости от конфигурации."""

        serialization_mode = (
            self.config.get("postprocess", {})
            .get("nested_serialization", "flatten")
            if isinstance(self.config, Mapping)
            else "flatten"
        )
        if "assay_parameters" not in df.columns or df.empty:
            return df

        if serialization_mode == "json":
            df = df.copy()
            df["assay_parameters"] = df["assay_parameters"].apply(
                lambda value: value if pd.isna(value) else str(value)
            )
            return df

        if serialization_mode == "flatten":
            expanded = df["assay_parameters"].apply(
                lambda val: val or {}
            ).apply(pd.Series)
            expanded.columns = [
                f"assay_param_{col}" for col in expanded.columns
            ]
            return pd.concat(
                [df.drop(columns=["assay_parameters"]), expanded], axis=1
            )

        return df

    def _ensure_assay_class_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Подготовка поля assay_class_map перед QC/валидацией."""

        if "assay_class_map" in df.columns:
            return df

        if not df.empty:
            df = df.copy()
            df["assay_class_map"] = None
        return df

    def _ensure_target_integrity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Референциальная целостность target_chembl_id."""

        if df.empty:
            return df

        if "target_chembl_id" not in df.columns:
            df = df.copy()
            df["target_chembl_id"] = None
            return df

        df = df.copy()
        df["target_chembl_id"].fillna("qc:missing_target", inplace=True)
        return df


__all__ = ["ChemblAssayPipeline"]
