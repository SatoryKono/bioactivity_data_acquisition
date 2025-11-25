from __future__ import annotations

"""Запуск ChEMBL TestItem pipeline."""

from pathlib import Path
from typing import Mapping, TYPE_CHECKING

import pandas as pd

from bioetl.core.io.artifacts import RunArtifacts
from bioetl.core.pipeline.types import StageExecutionOptions, WriteArtifacts, WriteResult
from bioetl.pipelines.chembl.common import ChemblEntityPipeline
from bioetl.schemas import TestItemSchema

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.core.io.output import UnifiedOutputWriter


class TestItemChemblPipeline(ChemblEntityPipeline):
    """Скелет пайплайна для testitem: молекулы + PubChem обогащение."""

    entity_name = "testitem"
    required_sort_fields = ("test_item_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(config, run_id=run_id)
        self.validator = TestItemSchema

    def build_descriptor(self):  # pragma: no cover
        return self._build_generic_descriptor()

    def transform(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        df = super().transform(df, options)
        df = self._canonicalize_inchikey(df)
        df = self._enrich_pubchem(df)
        df = self._normalize_molecule_properties(df)
        return df

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        df = super().validate(df, options)
        if "test_item_id" in df.columns and df["test_item_id"].isna().any():
            raise ValueError("test_item_id обязателен для testitem")
        return df

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> WriteResult:
        output_dir = artifacts.data_path.parent if artifacts.data_path else Path.cwd()
        writer = self._resolve_unified_writer(output_dir)
        if writer:
            run_artifacts = RunArtifacts(
                output_dir=output_dir,
                logs_directory=output_dir / "logs",
                write_artifacts=artifacts,
            )
            try:
                result = writer.write_dataset_atomic(df, run_artifacts, format="csv")
            except Exception:  # pragma: no cover
                return super().save_results(df, artifacts, options)
            try:  # pragma: no cover
                from bioetl.core.io.output import emit_qc_artifact

                emit_qc_artifact(df, run_artifacts)
            except Exception:
                pass
            return result
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

    def _resolve_unified_writer(self, output_dir):
        io_cfg = self.config.get("io") if isinstance(self.config, Mapping) else None
        if isinstance(io_cfg, Mapping):
            writer = io_cfg.get("writer")
            if writer is not None:
                if hasattr(writer, "output_dir"):
                    writer.output_dir = output_dir
                return writer
        return None


__all__ = ["TestItemChemblPipeline"]
