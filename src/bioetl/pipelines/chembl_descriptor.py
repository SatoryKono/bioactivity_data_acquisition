"""Backward-compatibility shim for relocated ChEMBL pipeline descriptor helpers."""

from __future__ import annotations

import warnings

from bioetl.pipelines.chembl.common.descriptor import (
    BatchExtractionContext,
    BatchExtractionStats,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)

__all__ = [
    "BatchExtractionContext",
    "BatchExtractionStats",
    "ChemblExtractionContext",
    "ChemblExtractionDescriptor",
    "ChemblPipelineBase",
]

warnings.warn(
    (
        "`bioetl.pipelines.chembl_descriptor` is deprecated; "
        "import from `bioetl.pipelines.chembl.common.descriptor` instead."
    ),
    DeprecationWarning,
    stacklevel=2,
)
