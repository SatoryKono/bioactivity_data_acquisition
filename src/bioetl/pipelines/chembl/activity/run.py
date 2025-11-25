from __future__ import annotations

"""Запуск ChEMBL Activity pipeline."""

from bioetl.pipelines.chembl.common import ChemblEntityPipeline


class ChemblActivityPipeline(ChemblEntityPipeline):
    entity_name = "activity"
    required_sort_fields = ("activity_id",)

    def build_descriptor(self):  # pragma: no cover - thin wrapper
        return self._build_generic_descriptor()


__all__ = ["ChemblActivityPipeline"]

