from __future__ import annotations

"""Запуск ChEMBL Target pipeline."""

from bioetl.pipelines.chembl.common import ChemblEntityPipeline


class ChemblTargetPipeline(ChemblEntityPipeline):
    entity_name = "target"
    required_sort_fields = ("target_chembl_id",)

    def build_descriptor(self):  # pragma: no cover
        return self._build_generic_descriptor()


__all__ = ["ChemblTargetPipeline"]

