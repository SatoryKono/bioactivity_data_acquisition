"""ChEMBL Assay transform utilities (proxy for backward compatibility).

This module provides backward compatibility for code that imports from
`bioetl.pipelines.chembl.assay_transform`. The actual implementation is in
`bioetl.pipelines.assay.assay_transform`.
"""

from __future__ import annotations

from bioetl.pipelines.assay.assay_transform import (
    header_rows_serialize,
    serialize_array_fields,
)

__all__ = [
    "header_rows_serialize",
    "serialize_array_fields",
]

