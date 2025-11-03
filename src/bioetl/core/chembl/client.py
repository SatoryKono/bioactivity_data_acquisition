"""Helpers for constructing ChEMBL API clients."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from bioetl.config import PipelineConfig
from bioetl.config.models import TargetSourceConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config

__all__ = [
    "ChemblClientContext",
    "build_chembl_client_context",
    "create_chembl_client",
]


@dataclass
class ChemblClientContext:
    """Resolved ChEMBL client configuration returned by the factory helpers."""

    client: UnifiedAPIClient
    source_config: TargetSourceConfig
    batch_size: int
    max_url_length: int | None
    base_url: str


def build_chembl_client_context(
    config: PipelineConfig,
    *,
    defaults: Mapping[str, Any] | None = None,
    batch_size_cap: int | None = None,
) -> ChemblClientContext:
    """Create a :class:`ChemblClientContext` for the provided configuration."""

    resolved_defaults: dict[str, Any] = {
        "enabled": True,
        "base_url": "https://www.ebi.ac.uk/chembl/api/data",
    }
    if defaults:
        resolved_defaults.update(defaults)

    factory = APIClientFactory.from_pipeline_config(config)
    chembl_source = ensure_target_source_config(
        config.sources.get("chembl"),
        defaults=resolved_defaults,
    )

    api_config = factory.create("chembl", chembl_source)
    client = UnifiedAPIClient(api_config)

    batch_default = resolved_defaults.get("batch_size")
    resolved_batch_size = chembl_source.batch_size or batch_default or 1
    resolved_batch_size = max(1, int(resolved_batch_size))
    if batch_size_cap is not None:
        resolved_batch_size = min(resolved_batch_size, int(batch_size_cap))

    max_url_default = resolved_defaults.get("max_url_length")
    resolved_max_url = chembl_source.max_url_length or max_url_default
    max_url_length: int | None = None
    if resolved_max_url is not None:
        max_url_length = max(1, int(resolved_max_url))

    return ChemblClientContext(
        client=client,
        source_config=chembl_source,
        batch_size=resolved_batch_size,
        max_url_length=max_url_length,
        base_url=str(chembl_source.base_url),
    )


def create_chembl_client(
    config: PipelineConfig,
    *,
    defaults: Mapping[str, Any] | None = None,
    batch_size_cap: int | None = None,
) -> ChemblClientContext:
    """Construct a ``UnifiedAPIClient`` for the ChEMBL source with shared defaults."""

    return build_chembl_client_context(
        config,
        defaults=defaults,
        batch_size_cap=batch_size_cap,
    )
