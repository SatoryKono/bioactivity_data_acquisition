"""Configuration profile for the Crossref enrichment source."""

from bioetl.adapters.crossref import CrossrefAdapter
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec

__all__ = ["CROSSREF_ADAPTER_DEFINITION"]


CROSSREF_ADAPTER_DEFINITION = AdapterDefinition(
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
