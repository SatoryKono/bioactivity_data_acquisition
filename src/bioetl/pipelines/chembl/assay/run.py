from __future__ import annotations

"""Запуск ChEMBL Assay pipeline."""

from typing import Any, Mapping, TYPE_CHECKING

import pandas as pd
import pandera as pa

from bioetl.core.io.artifacts import RunArtifacts
from bioetl.pipelines.chembl.common import ChemblEntityPipeline
from bioetl.schemas import AssaySchema

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.core.io.output import UnifiedOutputWriter


class ChemblAssayPipeline(ChemblEntityPipeline):
    """Заготовка пайплайна для выгрузки assay."""

    entity_name = "assay"
    required_sort_fields = ("assay_chembl_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(config, run_id=run_id)
        self.validator = AssaySchema

    def build_descriptor(self):  # pragma: no cover - тонкий слой
        return self._build_generic_descriptor()

    # ------------------------------------------------------------------
    # Stage hooks
    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().transform(df)
        df = self._normalize_nested_parameters(df)
        df = self._ensure_assay_class_mapping(df)
        df = self._ensure_target_integrity(df)
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().validate(df)
        id_column = "assay_chembl_id" if "assay_chembl_id" in df.columns else "assay_id"
        if id_column in df.columns:
            missing = df[id_column].isna().sum()
            if missing:
                raise pa.errors.SchemaError(f"Найдены пустые идентификаторы ассая: {missing}")
        return df

    def write(self, df: pd.DataFrame, output_dir, *, extended: bool = False):
        writer = self._resolve_unified_writer(output_dir)
        if writer:
            artifacts = RunArtifacts(output_dir=output_dir, logs_directory=output_dir / "logs")
            try:
                result = writer.write_dataset_atomic(df, artifacts, format="csv")
            except Exception:  # pragma: no cover - опциональная интеграция
                return super().write(df, output_dir, extended=extended)
            try:  # pragma: no cover - QC может быть не настроен
                from bioetl.core.io.output import emit_qc_artifact

                emit_qc_artifact(df, artifacts)
            except Exception:
                pass
            return result.data_path
        return super().write(df, output_dir, extended=extended)

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
            df["assay_parameters"] = df["assay_parameters"].apply(lambda value: value if pd.isna(value) else str(value))
            return df

        if serialization_mode == "flatten":
            expanded = df["assay_parameters"].apply(lambda val: val or {}).apply(pd.Series)
            expanded.columns = [f"assay_param_{col}" for col in expanded.columns]
            return pd.concat([df.drop(columns=["assay_parameters"]), expanded], axis=1)

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

    def _resolve_unified_writer(self, output_dir):
        io_cfg = self.config.get("io") if isinstance(self.config, Mapping) else None
        if isinstance(io_cfg, Mapping):
            writer = io_cfg.get("writer")
            if writer is not None:
                if hasattr(writer, "output_dir"):
                    writer.output_dir = output_dir
                return writer
        return None


__all__ = ["ChemblAssayPipeline"]

