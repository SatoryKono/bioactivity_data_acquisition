"""Упрощённый TestItem-пайплайн."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

import pandas as pd

from bioetl.pipelines.chembl.common import BaseChemblPipeline


class ChemblTestItemPipeline(BaseChemblPipeline):
    entity_name = "testitem"
    id_column = "molecule_chembl_id"

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
        return {"field_mappings": {"test_item_id": "test_item_id", "name": "name"}}

    def get_schema(self):
        return {"test_item_id": lambda series: series.notna()}

    def save_results(self, df: pd.DataFrame, output_path, **_: Any):
        df.to_csv(output_path, index=False)
        return {"output_path": str(output_path), "rows": len(df)}

    def run(self) -> pd.DataFrame:  # type: ignore[override]
        return super().run()


# Backward-compatible alias expected by tests and stage wrappers
TestItemChemblPipeline = ChemblTestItemPipeline


__all__ = ["ChemblTestItemPipeline", "TestItemChemblPipeline"]
