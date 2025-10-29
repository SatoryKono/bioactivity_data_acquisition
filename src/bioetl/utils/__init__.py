"""Пакет утилит BioETL.

Экспортирует наиболее часто используемые вспомогательные функции, чтобы они
были доступны через пространство имён ``bioetl.utils``. Это повторяет
поведение, на которое полагаются пайплайны (например, ``TargetPipeline``)
при импортировании ``finalize_pipeline_output`` напрямую из пакета.
"""

from bioetl.utils.dataframe import finalize_pipeline_output, resolve_schema_column_order
from bioetl.utils.qc import (
    accumulate_summary,
    prepare_enrichment_metrics_table,
    prepare_missing_mappings_table,
    register_fallback_statistics,
)

__all__ = [
    "accumulate_summary",
    "finalize_pipeline_output",
    "prepare_enrichment_metrics_table",
    "prepare_missing_mappings_table",
    "register_fallback_statistics",
    "resolve_schema_column_order",
]
