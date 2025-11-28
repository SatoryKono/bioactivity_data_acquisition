"""Запуск ChEMBL Target pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from bioetl.core.io import PipelineOutputService
from bioetl.core.io.artifacts import WriteArtifacts
from bioetl.core.pipeline.services import DefaultValidationService
from bioetl.core.pipeline.types import (
    StageExecutionOptions,
    WriteResult,
)
from bioetl.core.schemas import TargetSchema
from bioetl.pipelines.chembl.common.base import ChemblCommonPipeline


class ChemblTargetPipeline(ChemblCommonPipeline):
    """Каркас пайплайна для ChEMBL Target с обогащением UniProt/IUPHAR."""

    entity_name = "target"
    required_sort_fields = ("target_chembl_id",)

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
    ) -> None:
        super().__init__(
            config,
            run_id=run_id,
            descriptor_type="service",
        )
        self.validator = TargetSchema
        self.validation_service = DefaultValidationService(self.validator)

    def build_descriptor(self) -> Any:
        """Build descriptor for target pipeline."""
        return super().build_descriptor()

    # ------------------------------------------------------------------
    # Stage hooks
    # ------------------------------------------------------------------
    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().domain_enrich(df)
        return self._merge_enrichment(df)

    def validate(
        self,
        df: pd.DataFrame,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        df = super().validate(df, options)
        if (
            "target_chembl_id" in df.columns
            and df["target_chembl_id"].isna().any()
        ):
            raise ValueError(
                "target_chembl_id обязательный для сущности target"
            )
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        output_service = PipelineOutputService(self.config)
        try:
            return output_service.save(
                df, Path(artifacts.output_path), options
            )
        except ValueError as e:
            self._logger.warning(  # type: ignore[attr-defined]
                "PipelineOutputService failed, falling back to parent",
                error=str(e),
            )
            return super().save_results(df, artifacts, options)
        except Exception:  # pragma: no cover - совместимость с legacy
            self._logger.error(  # type: ignore[attr-defined]
                "Unexpected error in save_results, falling back to parent",
                exc_info=True,
            )
            return super().save_results(df, artifacts, options)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _merge_enrichment(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        if "uniprot_id" in df.columns:
            df["uniprot_payload"] = self.enrichers.enrich(
                df["uniprot_id"], "uniprot"
            )
        if "iuphar_id" in df.columns:
            df["iuphar_payload"] = self.enrichers.enrich(
                df["iuphar_id"], "iuphar"
            )
        return df


__all__ = ["ChemblTargetPipeline"]
