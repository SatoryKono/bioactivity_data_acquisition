from __future__ import annotations

"""Запуск ChEMBL TestItem pipeline."""

from typing import Any, Mapping

import pandas as pd

from bioetl.core.io import PipelineOutputService
from bioetl.core.pipeline.services import DefaultValidationService
from bioetl.core.pipeline.types import StageExecutionOptions, WriteArtifacts, WriteResult
from bioetl.pipelines.chembl.common import ChemblCommonPipeline
from bioetl.core.schemas import TestItemSchema

class TestItemChemblPipeline(ChemblCommonPipeline):
    """Скелет пайплайна для testitem: молекулы + PubChem обогащение."""

    entity_name = "testitem"
    required_sort_fields = ("test_item_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(
            config,
            run_id=run_id,
            descriptor_type="service",
        )
        self.validator = TestItemSchema
        self.validation_service = DefaultValidationService(self.validator)

    def build_descriptor(self):  # pragma: no cover
        return super().build_descriptor()

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().pre_transform(df)
        df = self._canonicalize_inchikey(df)
        df = self._normalize_molecule_properties(df)
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().domain_enrich(df)
        return self._enrich_pubchem(df)

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        df = super().validate(df, options)
        if "test_item_id" in df.columns and df["test_item_id"].isna().any():
            raise ValueError("test_item_id обязателен для testitem")
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
    def _canonicalize_inchikey(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "inchi_key" not in df.columns:
            return df
        df = df.copy()
        df["inchi_key"] = df["inchi_key"].str.upper().str.strip()
        return df

    def _enrich_pubchem(self, df: pd.DataFrame) -> pd.DataFrame:
        client = self.config.get("enrichers", {}).get("pubchem_client") if isinstance(self.config, Mapping) else None
        if df.empty or client is None:
            return df
        if not callable(getattr(client, "lookup", None)):
            return df
        df = df.copy()
        df["pubchem_enrichment"] = df["inchi_key"].apply(
            lambda inchikey: client.lookup(inchikey) if pd.notna(inchikey) else None
        )
        return df

    def _normalize_molecule_properties(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        if "smiles" in df.columns:
            df["smiles"] = df["smiles"].fillna("")
        if "molecular_weight" in df.columns:
            df["molecular_weight"] = pd.to_numeric(df["molecular_weight"], errors="coerce").round(3)
        return df


__all__ = ["TestItemChemblPipeline"]
