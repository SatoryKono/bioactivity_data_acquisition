"""Contract tests for :mod:`bioetl.core.api_client`."""

from __future__ import annotations

from bioetl.core import api_client


def test_fallback_manager_strategies() -> None:
    """Ensure fallback strategies stay in sync with documentation."""

    expected = {"cache", "partial_retry", "network", "timeout", "5xx"}

    config = api_client.APIConfig(name="chembl", base_url="https://chembl/api")

    assert set(strategy.lower() for strategy in config.fallback_strategies) == expected

    assert api_client.FALLBACK_MANAGER_SUPPORTED_STRATEGIES == {
        "network",
        "timeout",
        "5xx",
    }

    manager = api_client.FallbackManager(tuple(sorted(expected)))

    assert set(manager.strategies) == expected
