"""Упрощённый Activity-пайплайн ChEMBL на базе общего каркаса."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

import pandas as pd

from bioetl.pipelines.chembl.common import BaseChemblPipeline


class ChemblActivityPipeline(BaseChemblPipeline):
    """Adapter-класс, реализующий правила нормализации/обогащения для активности."""

    def __init__(
        self,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        writer=None,
    ) -> None:
        super().__init__(source)
        self.writer = writer

    def get_normalization_rules(self) -> Mapping[str, Any]:
        return {
            "field_mappings": {
                "activity_id": "activity_id",
                "assay_id": "assay_id",
                "value": "value",
            },
            "value_normalizers": {
                "activity_id": lambda v: int(v) if v is not None else None,
                "value": lambda v: float(v) if v is not None else None,
            },
        }

    def get_enrichment_rules(self):
        def add_flags(record: Mapping[str, Any]) -> Mapping[str, Any]:
            enriched = dict(record)
            enriched["is_active"] = bool(record.get("value"))
            return enriched

        return [add_flags]

    def get_schema(self):
        return {
            "activity_id": lambda series: series.notna(),
            "assay_id": lambda series: series.notna(),
        }

    # CLI совместимость
    def save_results(self, df: pd.DataFrame, output_path, **_: Any):
        df.to_csv(output_path, index=False)
        return {"output_path": str(output_path), "rows": len(df)}

    def run(self) -> pd.DataFrame:  # type: ignore[override]
        df = super().run()
        return df


__all__ = ["ChemblActivityPipeline"]
