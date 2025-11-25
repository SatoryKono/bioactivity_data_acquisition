"""Shared core types used across pipeline layers."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Any


@dataclass
class RunResult:
    """Result of a pipeline run."""

    status: str
    output_path: Path | None
    records_processed: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class BatchExtractionStats:
    """Metadata describing a batch extraction."""

    batch_size: int
    total_batches: int
    source_name: str | None = None


@dataclass
class ChemblExtractionDescriptor:
    """Describes extraction boundaries for ChEMBL data pulls."""

    load_meta_id: str
    source_release: str | None = None
    filters: Mapping[str, Any] = field(default_factory=dict)


__all__ = ["RunResult", "BatchExtractionStats", "ChemblExtractionDescriptor"]
