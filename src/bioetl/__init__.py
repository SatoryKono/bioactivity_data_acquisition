"""BioETL package scaffold implementing layered ETL architecture."""

from bioetl.public import (  # noqa: F401 re-exported API
    BaseApiClient,
    INormalizer,
    IParser,
    PipelineBase,
    PipelineConfig,
    RunResult,
    UnifiedLogger,
    load_config,
)

__all__ = [
    "__version__",
    "BaseApiClient",
    "INormalizer",
    "IParser",
    "PipelineBase",
    "PipelineConfig",
    "RunResult",
    "UnifiedLogger",
    "load_config",
]

__version__ = "0.1.0"
