"""Core components: logger, API client, output writer."""

from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.core.fallback_manager import FallbackManager
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import (
    OutputArtifacts,
    OutputMetadata,
    UnifiedOutputWriter,
)
from bioetl.core.unified_schema import (
    SchemaRegistration,
    SchemaRegistry,
    get_schema,
    get_schema_metadata,
    register_schema,
)
from bioetl.core.unified_schema import (
    get_registry as get_schema_registry,
)
from bioetl.core.unified_schema import (
    is_registered as is_schema_registered,
)

__all__ = [
    "UnifiedLogger",
    "UnifiedAPIClient",
    "APIConfig",
    "APIClientFactory",
    "ensure_target_source_config",
    "FallbackManager",
    "UnifiedOutputWriter",
    "OutputArtifacts",
    "OutputMetadata",
    "SchemaRegistry",
    "SchemaRegistration",
    "get_schema_registry",
    "get_schema",
    "get_schema_metadata",
    "is_schema_registered",
    "register_schema",
]
