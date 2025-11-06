"""ChEMBL Assay enrichment functions (proxy for backward compatibility).

This module provides backward compatibility for code that imports from
`bioetl.pipelines.chembl.assay_enrichment`. The actual implementation is in
`bioetl.pipelines.assay.assay_enrichment`.
"""

from __future__ import annotations

from bioetl.pipelines.assay.assay_enrichment import (
    enrich_with_assay_classifications,
    enrich_with_assay_parameters,
)

__all__ = [
    "enrich_with_assay_classifications",
    "enrich_with_assay_parameters",
]

