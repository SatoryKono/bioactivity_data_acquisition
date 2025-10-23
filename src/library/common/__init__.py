"""Common utilities and base classes for all ETL pipelines."""

from .exit_codes import ExitCode
from .pipeline_base import PipelineBase
from .writer_base import BaseWriter, ETLResult

__all__ = [
    "ExitCode",
    "ETLResult", 
    "PipelineBase",
    "BaseWriter",
]
