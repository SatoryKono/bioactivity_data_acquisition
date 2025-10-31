"""OpenAlex adapter unit tests."""

from __future__ import annotations

import unittest

from bioetl.adapters.openalex import OpenAlexAdapter
from tests.sources._mixins import AdapterTestMixin

__all__ = ["OpenAlexAdapterTestCase"]


class OpenAlexAdapterTestCase(AdapterTestMixin, unittest.TestCase):
    """Shared fixture base for OpenAlex adapter tests."""

    ADAPTER_CLASS = OpenAlexAdapter
    API_CONFIG_OVERRIDES = {
        "name": "openalex",
        "base_url": "https://api.openalex.org",
        "rate_limit_max_calls": 10,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 100,
        "workers": 4,
    }
