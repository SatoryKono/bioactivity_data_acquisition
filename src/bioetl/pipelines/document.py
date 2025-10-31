"""Compatibility proxy for the ChEMBL document pipeline."""

from bioetl.sources.chembl.document.pipeline import (
    DEFAULT_DOCUMENT_PIPELINE_MODE,
    DOCUMENT_EXTERNAL_ADAPTER_DEFINITIONS,
    DOCUMENT_PIPELINE_MODES,
    DocumentPipeline,
)

__all__ = [
    "DocumentPipeline",
    "DOCUMENT_EXTERNAL_ADAPTER_DEFINITIONS",
    "DOCUMENT_PIPELINE_MODES",
    "DEFAULT_DOCUMENT_PIPELINE_MODE",
]
