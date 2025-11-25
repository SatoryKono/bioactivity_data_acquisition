from __future__ import annotations

"""Запуск ChEMBL TestItem pipeline."""

from bioetl.pipelines.chembl.common import ChemblEntityPipeline


class TestItemChemblPipeline(ChemblEntityPipeline):
    entity_name = "test_item"
    required_sort_fields = ("test_item_id",)

    def build_descriptor(self):  # pragma: no cover
        return self._build_generic_descriptor()


__all__ = ["TestItemChemblPipeline"]

