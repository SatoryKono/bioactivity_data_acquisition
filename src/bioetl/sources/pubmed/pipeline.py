"""Configuration profile for the PubMed enrichment source."""

from bioetl.adapters.pubmed import PubMedAdapter
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec

__all__ = ["PUBMED_ADAPTER_DEFINITION"]


PUBMED_ADAPTER_DEFINITION = AdapterDefinition(
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
