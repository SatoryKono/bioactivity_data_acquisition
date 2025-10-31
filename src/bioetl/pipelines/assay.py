"""Compatibility proxy for the ChEMBL assay pipeline."""

from bioetl.sources.chembl.assay.pipeline import (
    AssayPipeline,
    _NULLABLE_INT_COLUMNS,
)

__all__ = ["AssayPipeline", "_NULLABLE_INT_COLUMNS"]
