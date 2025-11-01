"""PubMed adapter definition and standalone pipeline."""

from __future__ import annotations

from typing import Final

from bioetl.adapters.pubmed import PubMedAdapter
from bioetl.pipelines.external_source import ExternalSourcePipeline
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec
from bioetl.sources.pubmed.schema import PubMedNormalizedSchema

__all__ = ["PUBMED_ADAPTER_DEFINITION", "PubMedPipeline"]


PUBMED_ADAPTER_DEFINITION: Final[AdapterDefinition] = AdapterDefinition(
    adapter_cls=PubMedAdapter,
    api_fields={
        "base_url": FieldSpec(default="https://eutils.ncbi.nlm.nih.gov/entrez/eutils"),
        "rate_limit_max_calls": FieldSpec(default=3),
        "rate_limit_period": FieldSpec(default=1.0),
        "rate_limit_jitter": FieldSpec(default=True),
        "headers": FieldSpec(default_factory=dict),
    },
    adapter_fields={
        "batch_size": FieldSpec(default=200),
        "workers": FieldSpec(default=1),
        "tool": FieldSpec(
            default="bioactivity_etl",
            env="PUBMED_TOOL",
            coalesce_default_on_blank=True,
        ),
        "email": FieldSpec(default="", env="PUBMED_EMAIL"),
        "api_key": FieldSpec(default="", env="PUBMED_API_KEY"),
    },
)


class PubMedPipeline(ExternalSourcePipeline):
    """Pipeline dedicated to PubMed E-utilities enrichment."""

    source_name: Final[str] = "pubmed"
    adapter_definition: Final[AdapterDefinition] = PUBMED_ADAPTER_DEFINITION
    normalized_schema = PubMedNormalizedSchema
    business_key: Final[str] = "pmid"
    metadata_source_system: Final[str] = "pubmed"
    expected_input_columns: Final[tuple[str, ...]] = (
        "pmid",
        "doi",
        "doi_clean",
        "title",
    )
    identifier_columns: Final[dict[str, tuple[str, ...]]] = {
        "pmid": ("pmid", "chembl_pmid", "pubmed_pmid", "openalex_pmid", "semantic_scholar_pmid"),
        "doi": (
            "doi",
            "doi_clean",
            "chembl_doi",
            "crossref_doi",
            "crossref_doi_clean",
            "openalex_doi",
            "pubmed_doi",
            "semantic_scholar_doi",
        ),
        "title": ("title", "pubmed_article_title"),
    }
    match_columns: Final[tuple[str, ...]] = ("pubmed_pmid", "pmid")
    sort_by: Final[tuple[str, ...]] = ("pmid",)
