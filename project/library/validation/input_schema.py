"""Pandera schema for pipeline input."""
from __future__ import annotations

import pandera as pa
from pandera import Column, DataFrameSchema

from library.io.normalize import normalise_doi, to_lc_stripped


class InputColumns:
    DOCUMENT_ID = "document_chembl_id"
    DOI = "doi"
    PMID = "pmid"


input_schema = DataFrameSchema(
    {
        InputColumns.DOCUMENT_ID: Column(pa.String, nullable=False),
        InputColumns.DOI: Column(pa.String, nullable=True, coerce=True, checks=pa.Check(lambda s: s.map(normalise_doi).notna() | s.isna())),
        InputColumns.PMID: Column(pa.String, nullable=True, coerce=True, checks=pa.Check(lambda s: s.map(to_lc_stripped).str.isnumeric() | s.isna())),
    },
    coerce=True,
    strict=False,
)
