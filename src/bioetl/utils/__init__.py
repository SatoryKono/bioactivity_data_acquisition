"""Пакет утилит BioETL.

Экспортирует наиболее часто используемые вспомогательные функции, чтобы они
были доступны через пространство имён ``bioetl.utils``. Это повторяет
поведение, на которое полагаются пайплайны (например, ``TargetPipeline``)
при импортировании ``finalize_pipeline_output`` напрямую из пакета.
"""

from bioetl.utils.dataframe import finalize_pipeline_output

__all__ = ["finalize_pipeline_output"]
