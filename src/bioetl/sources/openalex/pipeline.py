"""OpenAlex adapter definition and standalone pipeline."""

from __future__ import annotations

from typing import Final

from bioetl.adapters.openalex import OpenAlexAdapter
from bioetl.pipelines.external_source import ExternalSourcePipeline
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec
from bioetl.sources.openalex.schema import OpenAlexNormalizedSchema

__all__ = ["OPENALEX_ADAPTER_DEFINITION", "OpenAlexPipeline"]


OPENALEX_ADAPTER_DEFINITION: Final[AdapterDefinition] = AdapterDefinition(
    adapter_cls=OpenAlexAdapter,
    api_fields={
        "base_url": FieldSpec(default="https://api.openalex.org"),
        "rate_limit_max_calls": FieldSpec(default=10),
        "rate_limit_period": FieldSpec(default=1.0),
        "rate_limit_jitter": FieldSpec(default=True),
        "headers": FieldSpec(default_factory=dict),
    },
    adapter_fields={
        "batch_size": FieldSpec(default=100),
        "workers": FieldSpec(default=4),
        "mailto": FieldSpec(default="", env="OPENALEX_MAILTO"),
    },
)


class OpenAlexPipeline(ExternalSourcePipeline):
    """Pipeline orchestrating enrichment against the OpenAlex Works API."""

    source_name: Final[str] = "openalex"
    adapter_definition: Final[AdapterDefinition] = OPENALEX_ADAPTER_DEFINITION
    normalized_schema = OpenAlexNormalizedSchema
    business_key: Final[str] = "doi_clean"
    metadata_source_system: Final[str] = "openalex"
    expected_input_columns: Final[tuple[str, ...]] = (
        "doi",
        "doi_clean",
        "pmid",
        "title",
        "openalex_id",
    )
    identifier_columns: Final[dict[str, tuple[str, ...]]] = {
        "doi": ("doi", "doi_clean", "chembl_doi", "openalex_doi", "openalex_doi_clean"),
        "pmid": ("pmid", "chembl_pmid", "openalex_pmid"),
        "title": ("title", "openalex_title"),
    }
    match_columns: Final[tuple[str, ...]] = ("openalex_id", "openalex_doi_clean", "doi_clean")
    sort_by: Final[tuple[str, ...]] = ("doi_clean", "openalex_id")
