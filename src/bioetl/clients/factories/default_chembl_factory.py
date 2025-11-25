from __future__ import annotations

"""Default factories for building ChEMBL clients."""

from typing import Any

from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.core.http.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.pipeline.types import PipelineConfig


def _build_default_api_client(config: PipelineConfig | None = None) -> UnifiedAPIClient:
    base_url = "https://www.ebi.ac.uk/chembl/api/data"
    api_config = APIConfig(
        base_url=base_url,
        timeout_sec=30,
        max_retries=3,
        backoff_factor=1,
        max_backoff_sec=8,
        rate_limit_calls=20,
        rate_limit_period_sec=1.0,
        cache_enabled=False,
        cache_ttl_sec=60,
        circuit_breaker_fail_max=5,
        circuit_breaker_reset_sec=30,
    )
    return UnifiedAPIClient(api_config)


def default_activity_client_factory(config: PipelineConfig | None) -> ChemblActivityClient:
    """Build a :class:`ChemblActivityClient` using default API settings."""

    api_client = _build_default_api_client(config)
    return ChemblActivityClient(api_client)


__all__ = ["default_activity_client_factory"]
