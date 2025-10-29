"""Core components: logger, API client, output writer."""

from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import (
    OutputArtifacts,
    OutputMetadata,
    UnifiedOutputWriter,
)

__all__ = [
    "UnifiedLogger",
    "UnifiedAPIClient",
    "APIConfig",
    "APIClientFactory",
    "ensure_target_source_config",
    "UnifiedOutputWriter",
    "OutputArtifacts",
    "OutputMetadata",
]

