"""Semantic Scholar adapter definition and standalone pipeline."""

from __future__ import annotations

from typing import Final

from bioetl.adapters.semantic_scholar import SemanticScholarAdapter
from bioetl.pipelines.external_source import ExternalSourcePipeline
from bioetl.schemas.pipeline_inputs import SemanticScholarInputSchema
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec
from bioetl.sources.semantic_scholar.schema import SemanticScholarNormalizedSchema

__all__ = ["SEMANTIC_SCHOLAR_ADAPTER_DEFINITION", "SemanticScholarPipeline"]


SEMANTIC_SCHOLAR_ADAPTER_DEFINITION: Final[AdapterDefinition] = AdapterDefinition(
    adapter_cls=SemanticScholarAdapter,
    api_fields={
        "base_url": FieldSpec(default="https://api.semanticscholar.org/graph/v1"),
        "rate_limit_max_calls": FieldSpec(default=1),
        "rate_limit_period": FieldSpec(default=1.25),
        "rate_limit_jitter": FieldSpec(default=True),
        "headers": FieldSpec(default_factory=dict),
    },
    adapter_fields={
        "batch_size": FieldSpec(default=50),
        "workers": FieldSpec(default=1),
        "api_key": FieldSpec(default="", env="SEMANTIC_SCHOLAR_API_KEY"),
    },
)


class SemanticScholarPipeline(ExternalSourcePipeline):
    """Pipeline using the Semantic Scholar Graph API for enrichment."""

    source_name: Final[str] = "semantic_scholar"
    adapter_definition: Final[AdapterDefinition] = SEMANTIC_SCHOLAR_ADAPTER_DEFINITION
    normalized_schema = SemanticScholarNormalizedSchema
    business_key: Final[str] = "paper_id"
    metadata_source_system: Final[str] = "semantic_scholar"
    input_schema: Final[type[SemanticScholarInputSchema]] = SemanticScholarInputSchema
    expected_input_columns: Final[tuple[str, ...]] = (
        "pmid",
        "doi",
        "doi_clean",
        "title",
        "paper_id",
    )
    identifier_columns: Final[dict[str, tuple[str, ...]]] = {
        "pmid": ("pmid", "chembl_pmid", "pubmed_pmid", "semantic_scholar_pmid"),
        "doi": (
            "doi",
            "doi_clean",
            "chembl_doi",
            "crossref_doi",
            "crossref_doi_clean",
            "openalex_doi",
            "semantic_scholar_doi",
        ),
        "title": ("title", "semantic_scholar_title"),
    }
    match_columns: Final[tuple[str, ...]] = ("paper_id", "pubmed_id", "doi_clean")
    sort_by: Final[tuple[str, ...]] = ("paper_id", "doi_clean")
