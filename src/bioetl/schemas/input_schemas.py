"""Pandera schemas describing CLI input tables for pipeline entrypoints."""

from __future__ import annotations

import pandas as pd

from bioetl.pandera_pandas import DataFrameModel, Field, pa
from bioetl.pandera_typing import Series


class _BaseInputSchema(DataFrameModel):
    """Common Pandera configuration for pipeline seed datasets."""

    class Config:
        strict = False
        ordered = True
        coerce = True


class DocumentInputSchema(pa.DataFrameModel):
    """Validate input identifiers for the document pipeline."""

    document_chembl_id: Series[str] = pa.Field(
        regex=r"^CHEMBL\d+$",
        nullable=False,
        unique=True,
        description="Валидный идентификатор документа ChEMBL",
    )

    class Config:
        strict = True
        ordered = True
        coerce = True


class ActivityInputSchema(_BaseInputSchema):
    """Validate identifier seeds for the Activity pipeline."""

    activity_id: Series[pd.Int64Dtype] = Field(
        nullable=True,
        ge=1,
        description="Primary integer identifier used by the ChEMBL activity API",
    )
    activity_chembl_id: Series[str] = Field(
        nullable=True,
        regex=r"^CHEMBL\d+$",
        description="Optional legacy activity identifier in CHEMBL format",
    )


class AssayInputSchema(_BaseInputSchema):
    """Validate assay identifiers consumed by the Assay pipeline."""

    assay_chembl_id: Series[str] = Field(
        nullable=False,
        regex=r"^CHEMBL\d+$",
        unique=True,
        description="Assay identifier in CHEMBL format",
    )


class CrossrefInputSchema(_BaseInputSchema):
    """Validate seed dataset for Crossref enrichment."""

    doi: Series[str] = Field(
        nullable=True,
        description="Raw DOI associated with the document",
    )
    doi_clean: Series[str] = Field(
        nullable=True,
        description="Normalized DOI used for deterministic matching",
    )
    title: Series[str] = Field(
        nullable=True,
        description="Primary title of the document",
    )
    crossref_doi: Series[str] = Field(
        nullable=True,
        description="Existing Crossref DOI on record",
    )
    crossref_doi_clean: Series[str] = Field(
        nullable=True,
        description="Normalised Crossref DOI used to deduplicate records",
    )


class OpenAlexInputSchema(_BaseInputSchema):
    """Validate seed dataset for OpenAlex enrichment."""

    doi: Series[str] = Field(nullable=True)
    doi_clean: Series[str] = Field(nullable=True)
    pmid: Series[str] = Field(nullable=True)
    title: Series[str] = Field(nullable=True)
    openalex_id: Series[str] = Field(nullable=True)


class PubMedInputSchema(_BaseInputSchema):
    """Validate seed dataset for PubMed enrichment."""

    pmid: Series[str] = Field(
        nullable=True,
        description="PubMed identifier extracted from ChEMBL",
    )
    doi: Series[str] = Field(nullable=True)
    doi_clean: Series[str] = Field(nullable=True)
    title: Series[str] = Field(nullable=True)


class SemanticScholarInputSchema(_BaseInputSchema):
    """Validate seed dataset for Semantic Scholar enrichment."""

    pmid: Series[str] = Field(nullable=True)
    doi: Series[str] = Field(nullable=True)
    doi_clean: Series[str] = Field(nullable=True)
    title: Series[str] = Field(nullable=True)
    paper_id: Series[str] = Field(nullable=True)


class PubChemInputSchema(_BaseInputSchema):
    """Validate identifiers required for PubChem enrichment."""

    molecule_chembl_id: Series[str] = Field(
        nullable=False,
        regex=r"^CHEMBL\d+$",
        description="Primary CHEMBL identifier for the molecule",
    )
    standard_inchi_key: Series[str] = Field(
        nullable=True,
        description="Standard InChIKey used for PubChem lookups",
    )


class TargetInputSchema(_BaseInputSchema):
    """Validate the seed dataset consumed by the Target pipeline."""

    target_chembl_id: Series[str] = Field(
        nullable=False,
        regex=r"^CHEMBL\d+$",
        description="Primary CHEMBL identifier for the target",
    )
    pref_name: Series[str] = Field(nullable=True)
    target_type: Series[str] = Field(nullable=True)
    organism: Series[str] = Field(nullable=True)
    taxonomy: Series[str] = Field(nullable=True)
    hgnc_id: Series[str] = Field(nullable=True)
    uniprot_accession: Series[str] = Field(nullable=True)
    iuphar_type: Series[str] = Field(nullable=True)
    iuphar_class: Series[str] = Field(nullable=True)
    iuphar_subclass: Series[str] = Field(nullable=True)


class TestItemInputSchema(_BaseInputSchema):
    """Validate identifiers required by the Test Item pipeline."""

    molecule_chembl_id: Series[str] = Field(
        nullable=False,
        regex=r"^CHEMBL\d+$",
        description="CHEMBL identifier for the molecule/test item",
    )
    parent_chembl_id: Series[str] = Field(
        nullable=True,
        regex=r"^CHEMBL\d+$",
        description="Optional parent molecule CHEMBL identifier",
    )


class UniProtInputSchema(_BaseInputSchema):
    """Validate seed dataset for UniProt enrichment."""

    uniprot_accession: Series[str] = Field(
        nullable=False,
        description="Primary UniProt accession",
    )
    gene_symbol: Series[str] = Field(nullable=True)
    organism: Series[str] = Field(nullable=True)
    taxonomy_id: Series[pd.Int64Dtype] = Field(nullable=True, ge=0)


__all__ = [
    "ActivityInputSchema",
    "AssayInputSchema",
    "CrossrefInputSchema",
    "DocumentInputSchema",
    "OpenAlexInputSchema",
    "PubMedInputSchema",
    "SemanticScholarInputSchema",
    "PubChemInputSchema",
    "TargetInputSchema",
    "TestItemInputSchema",
    "UniProtInputSchema",
]

