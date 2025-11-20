"""Упрощённый Target-пайплайн."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

import pandas as pd

from bioetl.pipelines.chembl.common import BaseChemblPipeline


class ChemblTargetPipeline(BaseChemblPipeline):
    entity_name = "target"
    id_column = "target_chembl_id"

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
        return {"field_mappings": {"target_id": "target_id", "pref_name": "pref_name"}}

    def get_schema(self):
        return {"target_id": lambda series: series.notna()}

    def save_results(self, df: pd.DataFrame, output_path, **_: Any):
        df.to_csv(output_path, index=False)
        return {"output_path": str(output_path), "rows": len(df)}


__all__ = ["ChemblTargetPipeline"]
