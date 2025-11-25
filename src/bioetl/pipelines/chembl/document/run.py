from __future__ import annotations

"""Запуск ChEMBL Document pipeline."""

from typing import Any, Mapping, TYPE_CHECKING

import pandas as pd

from bioetl.core.io.artifacts import RunArtifacts
from bioetl.pipelines.chembl.common import ChemblEntityPipeline, ConfigValidationError
from bioetl.schemas import DocumentSchema

if TYPE_CHECKING:  # pragma: no cover
    from bioetl.core.io.output import UnifiedOutputWriter


class ChemblDocumentPipeline(ChemblEntityPipeline):
    """Скелет пайплайна для ChEMBL Document с обогащением внешними источниками."""

    entity_name = "document"
    required_sort_fields = ("document_chembl_id",)

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(config, run_id=run_id)
        self.validator = DocumentSchema
        self.mode = self._resolve_mode(config)
        self.fallback_policy = self._resolve_fallback_policy(config)
        self.enrichment_chain = self._build_enrichment_chain()

    def build_descriptor(self):  # pragma: no cover
        return self.build_generic_descriptor()

    # ------------------------------------------------------------------
    # Stage hooks
    # ------------------------------------------------------------------
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().transform(df)
        if df.empty:
            return df
        df = self._apply_enrichment_chain(df)
        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().validate(df)
        if "document_chembl_id" in df.columns and df["document_chembl_id"].isna().any():
            raise ConfigValidationError("document_chembl_id не должен быть пустым")
        return df

    def write(self, df: pd.DataFrame, output_dir, *, extended: bool = False):
        writer = self._resolve_unified_writer(output_dir)
        if writer:
            artifacts = RunArtifacts(output_dir=output_dir, logs_directory=output_dir / "logs")
            try:
                result = writer.write_dataset_atomic(df, artifacts, format="csv")
            except Exception:  # pragma: no cover - опциональный путь
                return super().write(df, output_dir, extended=extended)
            try:  # pragma: no cover - QC необязателен
                from bioetl.core.io.output import emit_qc_artifact

                emit_qc_artifact(df, artifacts)
            except Exception:
                pass
            return result.data_path
        return super().write(df, output_dir, extended=extended)

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

    def _resolve_unified_writer(self, output_dir):
        io_cfg = self.config.get("io") if isinstance(self.config, Mapping) else None
        if isinstance(io_cfg, Mapping):
            writer = io_cfg.get("writer")
            if writer is not None:
                if hasattr(writer, "output_dir"):
                    writer.output_dir = output_dir
                return writer
        return None


__all__ = ["ChemblDocumentPipeline"]

