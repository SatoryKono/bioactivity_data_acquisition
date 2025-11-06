"""ChEMBL Assay pipeline (proxy for backward compatibility).

This module provides backward compatibility for code that imports from
`bioetl.pipelines.chembl.assay`. The actual implementation is in
`bioetl.pipelines.assay.assay`.
"""

from __future__ import annotations

from bioetl.pipelines.assay.assay import ChemblAssayPipeline

__all__ = [
    "ChemblAssayPipeline",
]
