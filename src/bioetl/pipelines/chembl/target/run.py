from __future__ import annotations

"""Запуск ChEMBL Target pipeline."""

from typing import Any, Mapping

import pandas as pd

from bioetl.core.io import PipelineOutputService
from bioetl.core.pipeline.services import DefaultValidationService
from bioetl.core.pipeline.types import StageExecutionOptions, WriteArtifacts, WriteResult
from bioetl.pipelines.chembl.common import ChemblCommonPipeline
from bioetl.core.schemas import TargetSchema

class ChemblTargetPipeline(ChemblCommonPipeline):
    """Каркас пайплайна для ChEMBL Target с обогащением UniProt/IUPHAR."""

    entity_name = "target"
    required_sort_fields = ("target_chembl_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(
            config,
            run_id=run_id,
            descriptor_type="service",
        )
        self.validator = TargetSchema
        self.validation_service = DefaultValidationService(self.validator)

    def build_descriptor(self):  # pragma: no cover
        return self._build_generic_descriptor()

    # ------------------------------------------------------------------
    # Stage hooks
    # ------------------------------------------------------------------
    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().domain_enrich(df)
        return self._merge_enrichment(df)

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        df = super().validate(df, options)
        if "target_chembl_id" in df.columns and df["target_chembl_id"].isna().any():
            raise ValueError("target_chembl_id обязательный для сущности target")
        return df

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_service = PipelineOutputService(self.config)
        try:
            return output_service.save(df, artifacts, options)
        except ValueError:
            return super().save_results(df, artifacts, options)
        except Exception:  # pragma: no cover - совместимость с legacy
            return super().save_results(df, artifacts, options)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _merge_enrichment(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        if "uniprot_id" in df.columns:
            df["uniprot_payload"] = df["uniprot_id"].apply(self._enrich_uniprot)
        if "iuphar_id" in df.columns:
            df["iuphar_payload"] = df["iuphar_id"].apply(self._enrich_iuphar)
        return df

    def _enrich_uniprot(self, uniprot_id: Any) -> Mapping[str, Any] | None:
        client = self.config.get("enrichers", {}).get("uniprot_client") if isinstance(self.config, Mapping) else None
        if callable(getattr(client, "fetch", None)) and pd.notna(uniprot_id):
            return client.fetch(uniprot_id)
        return None

    def _enrich_iuphar(self, iuphar_id: Any) -> Mapping[str, Any] | None:
        client = self.config.get("enrichers", {}).get("iuphar_client") if isinstance(self.config, Mapping) else None
        if callable(getattr(client, "fetch", None)) and pd.notna(iuphar_id):
            return client.fetch(iuphar_id)
        return None


__all__ = ["ChemblTargetPipeline"]

