"""Пакет утилит BioETL.

Экспортирует наиболее часто используемые вспомогательные функции, чтобы они
были доступны через пространство имён ``bioetl.utils``. Это повторяет
поведение, на которое полагаются пайплайны (например, ``TargetPipeline``)
при импортировании ``finalize_pipeline_output`` напрямую из пакета.
"""

__all__ = [
    "finalize_output_dataset",
    "finalize_pipeline_output",
    "load_input_frame",
    "resolve_input_path",
    "resolve_schema_column_order",
]


def __getattr__(name: str):
    """Lazily expose utility helpers to avoid import-time cycles."""

    if name in {"finalize_pipeline_output", "resolve_schema_column_order"}:
        from bioetl.utils import dataframe as dataframe_module

        return getattr(dataframe_module, name)

    if name in {"finalize_output_dataset"}:
        from bioetl.utils import output as output_module

        return getattr(output_module, name)

    if name in {"load_input_frame", "resolve_input_path"}:
        from bioetl.utils import io as io_module

        return getattr(io_module, name)

    raise AttributeError(f"module 'bioetl.utils' has no attribute '{name}'")
