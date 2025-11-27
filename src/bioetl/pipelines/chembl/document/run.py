from __future__ import annotations

"""Запуск ChEMBL Document pipeline."""

from typing import Any, Mapping

import pandas as pd

from bioetl.core.io import PipelineOutputService
from bioetl.core.pipeline.services import DefaultValidationService
from bioetl.core.pipeline.types import StageExecutionOptions, WriteArtifacts, WriteResult
from bioetl.pipelines.chembl.common import ChemblCommonPipeline, ConfigValidationError
from bioetl.core.schemas import DocumentSchema

class ChemblDocumentPipeline(ChemblCommonPipeline):
    """Скелет пайплайна для ChEMBL Document с обогащением внешними источниками."""

    entity_name = "document"
    required_sort_fields = ("document_chembl_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(config, run_id=run_id)
        self.validator = DocumentSchema
        self.validation_service = DefaultValidationService(self.validator)
        self.mode = self._resolve_mode(config)
        self.fallback_policy = self._resolve_fallback_policy(config)
        self.enrichment_chain = self._build_enrichment_chain()

    def build_descriptor(self):  # pragma: no cover
        return self._build_generic_descriptor()

    # ------------------------------------------------------------------
    # Stage hooks
    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        df = super().transform(df, options)
        if df.empty:
            return df
        df = self._apply_enrichment_chain(df)
        return df

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        df = super().validate(df, options)
        if "document_chembl_id" in df.columns and df["document_chembl_id"].isna().any():
            raise ConfigValidationError("document_chembl_id не должен быть пустым")
        return df

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_service = PipelineOutputService(self.config)
        try:
            return output_service.save(df, artifacts, options)
        except ValueError:
            return super().save_results(df, artifacts, options)
        except Exception:  # pragma: no cover - обратная совместимость
            return super().save_results(df, artifacts, options)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _resolve_mode(self, config: Mapping[str, Any]) -> str:
        mode = (
            config.get("mode", "chembl")
            if isinstance(config, Mapping)
            else "chembl"
        )
        if mode not in {"chembl", "all"}:
            raise ConfigValidationError("document.mode должен быть chembl|all")
        return mode

    def _resolve_fallback_policy(self, config: Mapping[str, Any]) -> str:
        fallbacks = config.get("fallbacks") if isinstance(config, Mapping) else None
        policy = fallbacks.get("policy") if isinstance(fallbacks, Mapping) else "ordered"
        if policy not in {"ordered", "best_effort", "strict"}:
            raise ConfigValidationError("fallbacks.policy должен быть ordered|best_effort|strict")
        return policy

    def _build_enrichment_chain(self) -> tuple[str, ...]:
        base_chain = (
            "cache",
            "semantic_scholar.title_search",
            "pubmed",
            "crossref",
        )
        return base_chain if self.mode == "all" else (base_chain[0],)

    def _apply_enrichment_chain(self, df: pd.DataFrame) -> pd.DataFrame:
        chain_marker = " > ".join(self.enrichment_chain)
        df = df.copy()
        df["enrichment_chain"] = chain_marker
        df["fallback_policy"] = self.fallback_policy
        return df


__all__ = ["ChemblDocumentPipeline"]

