"""ChEMBL pipeline modules (proxy for backward compatibility).

This module provides backward compatibility imports for code that references
`bioetl.pipelines.chembl.*` instead of the new `bioetl.pipelines.*` structure.

The actual implementations are in:
- `bioetl.pipelines.assay` for assay pipelines
- `bioetl.pipelines.activity` for activity pipelines
- `bioetl.pipelines.target` for target pipelines
- `bioetl.pipelines.document` for document pipelines
- `bioetl.pipelines.testitem` for testitem pipelines
"""

from __future__ import annotations

from bioetl.pipelines.assay import ChemblAssayPipeline
from bioetl.pipelines.chembl.shared import ChemblPipelineBase

__all__ = [
    "ChemblAssayPipeline",
    "ChemblPipelineBase",
]

