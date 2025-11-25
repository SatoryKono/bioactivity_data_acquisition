from __future__ import annotations

"""Запуск ChEMBL Assay pipeline."""

from bioetl.pipelines.chembl.common import ChemblEntityPipeline


class ChemblAssayPipeline(ChemblEntityPipeline):
    entity_name = "assay"
    required_sort_fields = ("assay_chembl_id",)

    def build_descriptor(self):  # pragma: no cover - тонкий слой
        return self._build_generic_descriptor()


__all__ = ["ChemblAssayPipeline"]

