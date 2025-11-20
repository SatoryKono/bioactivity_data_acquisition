"""Упрощённый Document-пайплайн."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

import pandas as pd

from bioetl.pipelines.chembl.common import BaseChemblPipeline


class ChemblDocumentPipeline(BaseChemblPipeline):
    entity_name = "document"
    id_column = "document_chembl_id"

    def __init__(
        self,
        config: Any,
        run_id: str,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        writer=None,
    ) -> None:
        super().__init__(config, run_id, source, writer=writer)

    def get_normalization_rules(self) -> Mapping[str, Any]:
        return {
            "field_mappings": {
                "document_id": "document_id",
                "title": "title",
            }
        }

    def get_schema(self):
        return {"document_id": lambda series: series.notna()}

    def _build_document_client(self, bundle):
        return bundle.entity_client

    def save_results(self, df: pd.DataFrame, output_path, **_: Any):
        df.to_csv(output_path, index=False)
        return {"output_path": str(output_path), "rows": len(df)}


__all__ = ["ChemblDocumentPipeline"]
