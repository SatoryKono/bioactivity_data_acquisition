from __future__ import annotations

"""Запуск ChEMBL Target pipeline."""

from typing import Any, Mapping, TYPE_CHECKING

import pandas as pd

from bioetl.core.io.artifacts import RunArtifacts
from bioetl.pipelines.chembl.common import ChemblEntityPipeline
from bioetl.schemas import TargetSchema

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.core.io.output import UnifiedOutputWriter


class ChemblTargetPipeline(ChemblEntityPipeline):
    """Каркас пайплайна для ChEMBL Target с обогащением UniProt/IUPHAR."""

    entity_name = "target"
    required_sort_fields = ("target_chembl_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(config, run_id=run_id)
        self.validator = TargetSchema

    def build_descriptor(self):  # pragma: no cover
        return self.build_generic_descriptor()

    # ------------------------------------------------------------------
    # Stage hooks
    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().transform(df)
        return self._merge_enrichment(df)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().validate(df)
        if "target_chembl_id" in df.columns and df["target_chembl_id"].isna().any():
            raise ValueError("target_chembl_id обязательный для сущности target")
        return df

    def write(self, df: pd.DataFrame, output_dir, *, extended: bool = False):
        writer = self._resolve_unified_writer(output_dir)
        if writer:
            artifacts = RunArtifacts(output_dir=output_dir, logs_directory=output_dir / "logs")
            try:
                result = writer.write_dataset_atomic(df, artifacts, format="csv")
            except Exception:  # pragma: no cover
                return super().write(df, output_dir, extended=extended)
            try:  # pragma: no cover
                from bioetl.core.io.output import emit_qc_artifact

                emit_qc_artifact(df, artifacts)
            except Exception:
                pass
            return result.data_path
        return super().write(df, output_dir, extended=extended)

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

    def _resolve_unified_writer(self, output_dir):
        io_cfg = self.config.get("io") if isinstance(self.config, Mapping) else None
        if isinstance(io_cfg, Mapping):
            writer = io_cfg.get("writer")
            if writer is not None:
                if hasattr(writer, "output_dir"):
                    writer.output_dir = output_dir
                return writer
        return None


__all__ = ["ChemblTargetPipeline"]

