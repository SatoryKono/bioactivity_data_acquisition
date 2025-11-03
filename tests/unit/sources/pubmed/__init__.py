"""PubMed adapter unit tests."""

from __future__ import annotations

import unittest

from bioetl.sources.pubmed.pipeline import PUBMED_ADAPTER_DEFINITION
from tests.unit.sources._mixins import AdapterTestMixin

__all__ = ["PubMedAdapterTestCase"]


class PubMedAdapterTestCase(AdapterTestMixin, unittest.TestCase):
    """Shared fixture base for PubMed adapter tests."""

    ADAPTER_DEFINITION = PUBMED_ADAPTER_DEFINITION
    API_CONFIG_OVERRIDES = {
        "name": "pubmed",
        "base_url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
        "rate_limit_max_calls": 3,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 200,
        "workers": 1,
        "email": "test@example.com",
        "api_key": "test_key",
    }
