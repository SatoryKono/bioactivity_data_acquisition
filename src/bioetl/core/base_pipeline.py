"""Совместимость для устаревших импортов базового пайплайна.

Модуль оставлен для поддержки кода, который по-прежнему импортирует классы
из `bioetl.core.base_pipeline`. Вся функциональность переехала в
`bioetl.pipelines.base`, поэтому здесь выполняется только реэкспорт.
"""

from __future__ import annotations

from bioetl.pipelines.base import (
    PipelineBase,
    RunArtifacts,
    RunResult,
    WriteArtifacts,
    WriteResult,
)

__all__ = [
    "PipelineBase",
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
]

