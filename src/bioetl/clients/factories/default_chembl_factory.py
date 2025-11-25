from __future__ import annotations

from collections.abc import Callable
from typing import Any

from bioetl.clients.entities import ChemblEntityClientFactory
from bioetl.base_classes import BaseApiClient
from bioetl.clients.chembl import (
    ChemblActivityClient,
    ChemblAssayClient,
    ChemblDocumentClient,
    ChemblTargetClient,
    ChemblTestItemClient,
)
from bioetl.config.models import PipelineConfig
from bioetl.core.http.api_client import APIConfig, UnifiedAPIClient


def _resolve_api_config(config: PipelineConfig) -> APIConfig:
    metadata = config.metadata or {}
    chembl_cfg = metadata.get("chembl_api", {}) if isinstance(metadata, dict) else {}

    def _get(key: str, default: Any) -> Any:
        if isinstance(chembl_cfg, dict):
            return chembl_cfg.get(key, default)
        return default

    return APIConfig(
        base_url=str(_get("base_url", "https://www.ebi.ac.uk/chembl/api/data")),
        timeout_sec=float(_get("timeout_sec", 30)),
        max_retries=int(_get("max_retries", 3)),
        backoff_factor=float(_get("backoff_factor", 1)),
        max_backoff_sec=float(_get("max_backoff_sec", 30)),
        rate_limit_calls=int(_get("rate_limit_calls", 10)),
        rate_limit_period_sec=float(_get("rate_limit_period_sec", 1.0)),
        cache_enabled=bool(_get("cache_enabled", True)),
        cache_ttl_sec=int(_get("cache_ttl_sec", 300)),
        circuit_breaker_fail_max=int(_get("circuit_breaker_fail_max", 5)),
        circuit_breaker_reset_sec=int(_get("circuit_breaker_reset_sec", 60)),
        default_headers=dict(_get("default_headers", {})) if isinstance(_get("default_headers", {}), dict) else {},
        user_agent=str(_get("user_agent", "bioetl-chembl-client")),
    )


def default_chembl_factory(
    config: PipelineConfig, api_client: UnifiedAPIClient | None = None
) -> dict[str, Callable[[], BaseApiClient]]:
    """Построить фабрику клиентов ChEMBL на основе конфигурации."""

    api_config = _resolve_api_config(config)
    shared_client = api_client or UnifiedAPIClient(api_config)

    return {
        "activity": lambda: ChemblActivityClient(shared_client),
        "assay": lambda: ChemblAssayClient(shared_client),
        "document": lambda: ChemblDocumentClient(shared_client),
        "target": lambda: ChemblTargetClient(shared_client),
        "testitem": lambda: ChemblTestItemClient(shared_client),
    }


def default_activity_client_factory(config: PipelineConfig, api_client: UnifiedAPIClient | None = None) -> ChemblActivityClient:
    """Построить клиент ChEMBL Activity с настройками по умолчанию."""

    factory = default_chembl_factory(config, api_client=api_client)
    builder = factory.get("activity")
    if builder is None:  # pragma: no cover - защитная проверка
        raise RuntimeError("Activity client builder is not configured")
    client = builder()
    if not isinstance(client, ChemblActivityClient):
        raise TypeError("Configured activity builder did not produce ChemblActivityClient")
    return client


__all__ = ["default_chembl_factory", "default_activity_client_factory"]
