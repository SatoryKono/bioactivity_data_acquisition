"""Configuration profile for the Semantic Scholar enrichment source."""

from bioetl.adapters.semantic_scholar import SemanticScholarAdapter
from bioetl.sources.document.pipeline import AdapterDefinition, FieldSpec

__all__ = ["SEMANTIC_SCHOLAR_ADAPTER_DEFINITION"]


SEMANTIC_SCHOLAR_ADAPTER_DEFINITION = AdapterDefinition(
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
