from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from bioetl.clients.enrichers.facade import ClientMethodStrategy, build_enricher_facade
from bioetl.clients.enrichers.factory import NULL_ENRICHER_FACTORY
from bioetl.clients.enrichers.strategy_registry import StrategyRegistry


def test_strategy_registry_derives_default_strategies():
    client = MagicMock()
    client.lookup.return_value = {"result": "ok"}

    registry = StrategyRegistry.from_config({"pubchem_client": client})

    assert registry is not None
    facade = build_enricher_facade(NULL_ENRICHER_FACTORY, registry)
    enriched = facade.enrich(pd.Series(["Q"], index=[0]), "pubchem")

    assert enriched == {"result": "ok"}


def test_strategy_registry_uses_explicit_mapping():
    client = MagicMock()
    client.fetch.return_value = {"payload": 1}

    registry = StrategyRegistry.from_config(
        {
            "strategies": {
                "custom": ClientMethodStrategy(lambda _factory: client, "fetch"),
            }
        }
    )

    facade = build_enricher_facade(NULL_ENRICHER_FACTORY, registry)
    enriched = facade.enrich(pd.Series(["Q"], index=[0]), "custom")

    assert enriched == {"payload": 1}
