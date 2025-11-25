from __future__ import annotations

"""Запуск ChEMBL Document pipeline."""

from bioetl.pipelines.chembl.common import ChemblEntityPipeline


class ChemblDocumentPipeline(ChemblEntityPipeline):
    entity_name = "document"
    required_sort_fields = ("document_chembl_id",)

    def build_descriptor(self):  # pragma: no cover
        return self._build_generic_descriptor()


__all__ = ["ChemblDocumentPipeline"]

