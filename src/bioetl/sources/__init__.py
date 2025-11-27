"""Backward-compat shim for bioetl.sources -> bioetl.infrastructure.sources"""

from __future__ import annotations

from bioetl.infrastructure.sources.chembl.common import (
    BaseChemblNormalizer,
    ColumnMapping,
    ColumnNormalizationSpec,
    build_records_from_payload,
)
import warnings

# Issue deprecation warning on first import
warnings.warn(
    "Importing from 'bioetl.sources' is deprecated; "
    "use 'bioetl.infrastructure.sources' instead",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "BaseChemblNormalizer",
    "ColumnMapping",
    "ColumnNormalizationSpec",
    "build_records_from_payload",
]
