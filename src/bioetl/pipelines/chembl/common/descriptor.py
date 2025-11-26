"""Descriptor helpers shared across ChEMBL pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from bioetl.core.pipeline.types import PipelineConfig, RunResult

__all__ = [
    "BatchPlan",
    "ChemblExtractionDescriptor",
    "ChemblPipelineContract",
    "ConfigValidationError",
    "descriptor_from_csv",
    "descriptor_from_options",
]


class ConfigValidationError(ValueError):
    """Raised when user supplied configuration is invalid."""


@dataclass(slots=True)
class BatchPlan:
    """Batch parameters used during extraction."""

    batch_size: int | None = None
    chunk_size: int | None = None


@dataclass(slots=True)
class ChemblExtractionDescriptor:
    """Lightweight description of what should be extracted from ChEMBL."""

    ids: list[str] | None
    pagination: dict[str, Any] | None
    mode: str = "chembl"
    batch_plan: BatchPlan | None = None
    release: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in {"chembl", "all"}:
            raise ConfigValidationError(
                "mode must be either 'chembl' or 'all'"
            )


class ChemblPipelineContract(Protocol):
    """Contract implemented by concrete ChEMBL pipelines."""

    id_column: str | None
    pipeline_code: str

    def run(
        self,
        output_dir: Path,
        *,
        extended: bool = False,
        include_qc_metrics: bool = False,
        **options: Any,
    ) -> RunResult:
        ...

    def build_descriptor(self) -> ChemblExtractionDescriptor:
        ...

    def run_descriptor_extraction(
        self,
        descriptor: ChemblExtractionDescriptor,
        *,
        batch_size: int | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        ...

    def resolve_chembl_release(
        self, config: PipelineConfig
    ) -> tuple[str | None, dict[str, Any]]:
        ...


# ---------------------------------------------------------------------------
# Descriptor builders
# ---------------------------------------------------------------------------

def _sanitize_ids(ids: list[str] | None) -> list[str] | None:
    if ids is None:
        return None
    return [str(item).strip() for item in ids if str(item).strip()]


def descriptor_from_csv(
    path: str | Path,
    *,
    id_column: str = "chembl_id",
    mode: str = "chembl",
    batch_size: int | None = None,
    chunk_size: int | None = None,
    release: str | None = None,
) -> ChemblExtractionDescriptor:
    """Build a descriptor from an input CSV file containing identifiers."""

    frame = pd.read_csv(path)
    if id_column not in frame.columns:
        msg = f"CSV file {path} must contain column '{id_column}'"
        raise ConfigValidationError(msg)
    ids = frame[id_column].dropna().astype(str).tolist()
    plan = BatchPlan(batch_size=batch_size, chunk_size=chunk_size)
    return ChemblExtractionDescriptor(
        ids=_sanitize_ids(ids),
        pagination=None,
        mode=mode,
        batch_plan=plan,
        release=release,
    )


def descriptor_from_options(
    *,
    ids: list[str] | None = None,
    pagination: dict | None = None,
    mode: str = "chembl",
    batch_size: int | None = None,
    chunk_size: int | None = None,
    release: str | None = None,
) -> ChemblExtractionDescriptor:
    """Build a descriptor from CLI/runtime options."""

    plan = BatchPlan(batch_size=batch_size, chunk_size=chunk_size)
    return ChemblExtractionDescriptor(
        ids=_sanitize_ids(ids),
        pagination=pagination,
        mode=mode,
        batch_plan=plan,
        release=release,
    )
