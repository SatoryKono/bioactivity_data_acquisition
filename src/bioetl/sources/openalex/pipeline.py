"""Configuration profile for the OpenAlex enrichment source."""

from bioetl.adapters.openalex import OpenAlexAdapter
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec

__all__ = ["OPENALEX_ADAPTER_DEFINITION"]


OPENALEX_ADAPTER_DEFINITION = AdapterDefinition(
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
    },
)
