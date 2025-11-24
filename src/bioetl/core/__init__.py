"""Public entrypoint for the BioETL core package.

This module re-exports the supported surface area from the reorganised
`bioetl.core` subpackages while maintaining backwards-compatible short imports.
"""

from __future__ import annotations

import warnings
from importlib import import_module
from typing import TYPE_CHECKING

from infrastructure.http import (
    APIClientFactory,
    CircuitBreaker,
    CircuitBreakerOpenError,
    TokenBucketLimiter,
    UnifiedAPIClient,
    merge_http_configs,
)
from infrastructure.io import (
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
from infrastructure.logging import (
    DEFAULT_LOG_LEVEL,
    MANDATORY_FIELDS,
    LogConfig,
    LogEvents,
    LogFormat,
    LoggerConfig,
    UnifiedLogger,
    configure_logging,
    get_logger,
)
from infrastructure.runtime.cli_base import CliCommandBase, CliEntrypoint
from infrastructure.runtime.errors import BioETLError
from infrastructure.runtime.lazy_loader import resolve_lazy_attr
from infrastructure.runtime.load_meta_store import LoadMetaStore
from infrastructure.schema import (
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
from common.core_utils import (
    clear_vocab_store_cache,
    get_ids,
    load_vocab_store,
)

if TYPE_CHECKING:  # pragma: no cover - imports for static analyzers only
    from bioetl.base_classes import BaseApiClient, INormalizer, IParser
    from infrastructure.chembl import join_activity_with_molecule
    from infrastructure.chembl.release_tracker import ChemblReleaseMixin

__all__ = [
    # Common
    "ChemblReleaseMixin",
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
    "configure_logging",
    "get_logger",
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
    "BioETLError",
    "CliCommandBase",
    "CliEntrypoint",
    "LoadMetaStore",
    # Utils
    "clear_vocab_store_cache",
    "get_ids",
    "join_activity_with_molecule",
    "load_vocab_store",
    # Schema
    "IdentifierRule",
    "IdentifierStats",
    "SchemaColumnFactory",
    "StringRule",
    "StringStats",
    "normalize_identifier_columns",
    "normalize_string_columns",
    "format_failure_cases",
    "summarize_schema_errors",
    # Deprecated exports (kept for backwards compatibility)
    "BaseApiClient",
    "IParser",
    "INormalizer",
]

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "CliCommandBase": ("infrastructure.runtime.cli_base", "CliCommandBase"),
    "CliEntrypoint": ("infrastructure.runtime.cli_base", "CliEntrypoint"),
    "BioETLError": ("infrastructure.runtime.errors", "BioETLError"),
    "LoadMetaStore": ("infrastructure.runtime.load_meta_store", "LoadMetaStore"),
}

_lazy_resolver = resolve_lazy_attr(globals(), _LAZY_EXPORTS, cache=True)

_DEPRECATED_EXPORTS: dict[str, tuple[str, str, str]] = {
    "ChemblReleaseMixin": (
        "infrastructure.chembl.release_tracker",
        "ChemblReleaseMixin",
        (
            "'ChemblReleaseMixin' is now provided via "
            "'infrastructure.chembl.release_tracker'. "
            "Importing it from 'bioetl.core' is deprecated and will "
            "be removed in a future release."
        ),
    ),
    "join_activity_with_molecule": (
        "infrastructure.chembl",
        "join_activity_with_molecule",
        (
            "'join_activity_with_molecule' is now provided via "
            "'infrastructure.chembl'. Importing it from 'bioetl.core' "
            "is deprecated and will be removed in a future release."
        ),
    ),
    "BaseApiClient": (
        "bioetl.base_classes",
        "BaseApiClient",
        (
            "'BaseApiClient' is now provided via 'bioetl.base_classes'. "
            "Importing it from 'bioetl.core' is deprecated and will be "
            "removed in a future release."
        ),
    ),
    "IParser": (
        "bioetl.base_classes",
        "IParser",
        (
            "'IParser' moved to 'bioetl.base_classes'. "
            "Importing it from 'bioetl.core' is deprecated."
        ),
    ),
    "INormalizer": (
        "bioetl.base_classes",
        "INormalizer",
        (
            "'INormalizer' moved to 'bioetl.base_classes'. "
            "Importing it from 'bioetl.core' is deprecated."
        ),
    ),
}


def __getattr__(name: str) -> object:
    if name in _DEPRECATED_EXPORTS:
        module_name, attr_name, message = _DEPRECATED_EXPORTS[name]
        warnings.warn(message, DeprecationWarning, stacklevel=2)
        module = import_module(module_name)
        return getattr(module, attr_name)
    return _lazy_resolver(name)
