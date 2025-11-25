"""Pipeline orchestration interfaces.

This module defines the contracts that orchestrate extract/transform/validate
/write pipelines without tying them to specific infrastructure concerns.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from collections.abc import Callable

from bioetl.core.logging import UnifiedLogger
from bioetl.core.types import RunResult

PipelineFactory = Callable[..., "PipelineBase"]
"""Callable that produces a configured :class:`PipelineBase` instance."""


@dataclass
class PipelineStageCommand:
    """Lightweight descriptor used to build pipeline stages dynamically."""

    name: str
    handler: Callable[[Any], Any]
    description: str | None = None


@runtime_checkable
class PipelineStagesProtocol(Protocol):
    """Protocol describing the default ETL stages."""

    def extract(self, **options: Any) -> Any: ...

    def transform(self, data: Any, **options: Any) -> Any: ...

    def validate(self, data: Any, **options: Any) -> Any: ...

    def write(self, data: Any, output_dir: Path, **options: Any) -> RunResult: ...


class PipelineBase(abc.ABC):
    """Abstract base class for orchestration-level pipelines."""

    def __init__(self, run_id: str, *, logger: UnifiedLogger | None = None) -> None:
        self.run_id = run_id
        self.logger = logger or UnifiedLogger(self.__class__.__name__)

    @abc.abstractmethod
    def extract(self, **options: Any) -> Any:
        """Fetch raw data from an upstream source."""

    @abc.abstractmethod
    def transform(self, data: Any, **options: Any) -> Any:
        """Apply domain transformations to the extracted data."""

    @abc.abstractmethod
    def validate(self, data: Any, **options: Any) -> Any:
        """Validate data using domain schemas (e.g., Pandera)."""

    @abc.abstractmethod
    def write(self, data: Any, output_dir: Path, **options: Any) -> RunResult:
        """Persist transformed data to an output sink."""

    @abc.abstractmethod
    def run(
        self,
        output_dir: Path,
        *,
        extended: bool = False,
        include_qc_metrics: bool = False,
        **options: Any,
    ) -> RunResult:
        """Execute the pipeline end-to-end."""


__all__ = [
    "PipelineBase",
    "PipelineFactory",
    "PipelineStagesProtocol",
    "PipelineStageCommand",
]
