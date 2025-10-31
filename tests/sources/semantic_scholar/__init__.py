"""Semantic Scholar adapter unit tests."""

from __future__ import annotations

import unittest

from bioetl.adapters.semantic_scholar import SemanticScholarAdapter
from tests.sources._mixins import AdapterTestMixin

__all__ = ["SemanticScholarAdapterTestCase"]


class SemanticScholarAdapterTestCase(AdapterTestMixin, unittest.TestCase):
    """Shared fixture base for Semantic Scholar adapter tests."""

    ADAPTER_CLASS = SemanticScholarAdapter
    API_CONFIG_OVERRIDES = {
        "name": "semantic_scholar",
        "base_url": "https://api.semanticscholar.org/graph/v1",
        "rate_limit_period": 1.25,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 50,
        "workers": 1,
        "api_key": "test_key",
    }
