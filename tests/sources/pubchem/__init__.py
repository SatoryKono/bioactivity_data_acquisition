"""PubChem adapter tests."""

from __future__ import annotations

import unittest

from bioetl.adapters.pubchem import PubChemAdapter
from tests.sources._mixins import AdapterTestMixin

__all__ = ["PubChemAdapterTestCase"]


class PubChemAdapterTestCase(AdapterTestMixin, unittest.TestCase):
    """Shared fixture base for PubChem adapter tests."""

    ADAPTER_CLASS = PubChemAdapter
    API_CONFIG_OVERRIDES = {
        "name": "pubchem",
        "base_url": "https://pubchem.ncbi.nlm.nih.gov/rest/pug",
        "rate_limit_max_calls": 5,
        "rate_limit_period": 1.0,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 50,
        "workers": 1,
    }
