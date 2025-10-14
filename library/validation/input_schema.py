"""Pandera schema definitions for pipeline inputs."""
from __future__ import annotations

import pandera.pandas as pa


INPUT_SCHEMA = pa.DataFrameSchema(
    {
        "document_chembl_id": pa.Column(str, nullable=False, coerce=True),
        "doi": pa.Column(str, nullable=True, coerce=True, required=False),
        "pmid": pa.Column(str, nullable=True, coerce=True, required=False),
    },
    strict=False,
    coerce=True,
    name="FetchPublicationsInput",
)
