"""Public entrypoint for the BioETL core package.

This module re-exports the supported surface area from the reorganised
`bioetl.core` subpackages while maintaining backwards-compatible short imports.
"""

from __future__ import annotations

from .http import (
    APIClientFactory,
    CircuitBreaker,
    CircuitBreakerOpenError,
    TokenBucketLimiter,
    UnifiedAPIClient,
    merge_http_configs,
)
from .io import (
    DeterministicWriteArtifacts,
    QCUnits,
    RunArtifacts,
    WriteArtifacts,
    WriteResult,
    build_write_artifacts,
    compute_hash,
    emit_qc_artifact,
    ensure_columns,
    ensure_hash_columns,
    escape_delims,
    finalise_output,
    hash_from_mapping,
    header_rows_serialize,
    plan_run_artifacts,
    prepare_dataframe,
    serialise_metadata,
    serialize_array_fields,
    serialize_objects,
    serialize_simple_list,
    write_dataset_atomic,
    write_frame_like,
    write_yaml_atomic,
)
from .logging import (
    DEFAULT_LOG_LEVEL,
    MANDATORY_FIELDS,
    LogConfig,
    LogEvents,
    LogFormat,
    LoggerConfig,
    UnifiedLogger,
    bind_global_context,
    client_event,
    configure_logging,
    emit,
    get_logger,
    reset_global_context,
    stage_event,
)
from .runtime.base_pipeline_compat import PipelineBase, RunResult
from .runtime.base_source import BaseSourceConfig, BaseSourceParameters
from .runtime.cli_base import CliCommandBase, CliEntrypoint
from .runtime.errors import BioETLError
from .runtime.load_meta_store import LoadMetaStore
from .schema import (
    IdentifierRule,
    IdentifierStats,
    SchemaColumnFactory,
    StringRule,
    StringStats,
    format_failure_cases,
    normalize_identifier_columns,
    normalize_string_columns,
    summarize_schema_errors,
)
from .utils import clear_vocab_store_cache, get_ids, join_activity_with_molecule, load_vocab_store

__all__ = [
    # HTTP
    "APIClientFactory",
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "TokenBucketLimiter",
    "UnifiedAPIClient",
    "merge_http_configs",
    # Logging
    "DEFAULT_LOG_LEVEL",
    "LogConfig",
    "LogEvents",
    "LogFormat",
    "LoggerConfig",
    "MANDATORY_FIELDS",
    "UnifiedLogger",
    "bind_global_context",
    "client_event",
    "configure_logging",
    "emit",
    "get_logger",
    "reset_global_context",
    "stage_event",
    # IO
    "DeterministicWriteArtifacts",
    "QCUnits",
    "RunArtifacts",
    "WriteArtifacts",
    "WriteResult",
    "build_write_artifacts",
    "compute_hash",
    "emit_qc_artifact",
    "ensure_columns",
    "ensure_hash_columns",
    "escape_delims",
    "finalise_output",
    "hash_from_mapping",
    "header_rows_serialize",
    "plan_run_artifacts",
    "prepare_dataframe",
    "serialise_metadata",
    "serialize_array_fields",
    "serialize_objects",
    "serialize_simple_list",
    "write_dataset_atomic",
    "write_frame_like",
    "write_yaml_atomic",
    # Runtime
    "BaseSourceConfig",
    "BaseSourceParameters",
    "BioETLError",
    "CliCommandBase",
    "CliEntrypoint",
    "LoadMetaStore",
    "PipelineBase",
    "RunResult",
    # Utils
    "clear_vocab_store_cache",
    "get_ids",
    "join_activity_with_molecule",
    "load_vocab_store",
    # Schema
    "IdentifierRule",
    "IdentifierStats",
    "StringRule",
    "StringStats",
    "SchemaColumnFactory",
    "normalize_identifier_columns",
    "normalize_string_columns",
    "format_failure_cases",
    "summarize_schema_errors",
]
