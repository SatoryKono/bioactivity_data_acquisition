"""Compatibility proxy for the ChEMBL assay pipeline."""

from bioetl.sources.chembl.assay.pipeline import (
    _NULLABLE_INT_COLUMNS,
    AssayPipeline,
)

__all__ = ["AssayPipeline", "_NULLABLE_INT_COLUMNS"]
