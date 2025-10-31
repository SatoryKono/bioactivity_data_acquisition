"""Crossref adapter unit tests."""

from __future__ import annotations

import unittest

from bioetl.adapters.crossref import CrossrefAdapter
from tests.sources._mixins import AdapterTestMixin

__all__ = ["CrossrefAdapterTestCase"]


class CrossrefAdapterTestCase(AdapterTestMixin, unittest.TestCase):
    """Shared fixture base for Crossref adapter tests."""

    ADAPTER_CLASS = CrossrefAdapter
    API_CONFIG_OVERRIDES = {
        "name": "crossref",
        "base_url": "https://api.crossref.org",
        "rate_limit_max_calls": 2,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 100,
        "workers": 2,
        "mailto": "test@example.com",
    }
