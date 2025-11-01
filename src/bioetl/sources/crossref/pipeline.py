"""Crossref adapter definition and standalone pipeline."""

from __future__ import annotations

from typing import Final

from bioetl.adapters.crossref import CrossrefAdapter
from bioetl.pipelines.external_source import ExternalSourcePipeline
from bioetl.sources.crossref.schema import CrossrefNormalizedSchema
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec

__all__ = ["CROSSREF_ADAPTER_DEFINITION", "CrossrefPipeline"]


CROSSREF_ADAPTER_DEFINITION: Final[AdapterDefinition] = AdapterDefinition(
    adapter_cls=CrossrefAdapter,
    api_fields={
        "base_url": FieldSpec(default="https://api.crossref.org"),
        "rate_limit_max_calls": FieldSpec(default=2),
        "rate_limit_period": FieldSpec(default=1.0),
        "rate_limit_jitter": FieldSpec(default=True),
        "headers": FieldSpec(default_factory=dict),
    },
    adapter_fields={
        "batch_size": FieldSpec(default=100),
        "workers": FieldSpec(default=2),
        "mailto": FieldSpec(default="", env="CROSSREF_MAILTO"),
    },
)


class CrossrefPipeline(ExternalSourcePipeline):
    """Pipeline coordinating DOI enrichment against the Crossref REST API."""

    source_name: Final[str] = "crossref"
    adapter_definition: Final[AdapterDefinition] = CROSSREF_ADAPTER_DEFINITION
    normalized_schema = CrossrefNormalizedSchema
    business_key: Final[str] = "doi_clean"
    metadata_source_system: Final[str] = "crossref"
    expected_input_columns: Final[tuple[str, ...]] = (
        "doi",
        "doi_clean",
        "title",
        "crossref_doi",
        "crossref_doi_clean",
    )
    identifier_columns: Final[dict[str, tuple[str, ...]]] = {
        "doi": (
            "doi",
            "doi_clean",
            "chembl_doi",
            "crossref_doi",
            "crossref_doi_clean",
            "openalex_doi",
            "pubmed_doi",
        ),
        "title": ("title", "crossref_title"),
    }
    match_columns: Final[tuple[str, ...]] = ("crossref_doi_clean", "doi_clean")
    sort_by: Final[tuple[str, ...]] = ("doi_clean", "crossref_doi_clean")
