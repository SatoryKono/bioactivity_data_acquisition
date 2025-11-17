"""Public entrypoint for the BioETL core package.

This module re-exports the supported surface area from the reorganised
`bioetl.core` subpackages while maintaining backwards-compatible short imports.
"""

from __future__ import annotations

from collections.abc import Container, Mapping, MutableMapping
from typing import Any, Callable

from .common import ChemblReleaseMixin
from .utils import clear_vocab_store_cache, get_ids, join_activity_with_molecule, load_vocab_store

LazyMappingValue = str | tuple[str, str]
CachePolicy = bool | Container[str]


def _should_cache(name: str, policy: CachePolicy) -> bool:
    if policy is True:
        return True
    if policy is False:
        return False
    return name in policy


def _resolve_lazy_attr(
    namespace: MutableMapping[str, Any],
    mapping: Mapping[str, LazyMappingValue],
    *,
    cache: CachePolicy = False,
) -> Callable[[str], Any]:
    def loader(name: str) -> Any:
        target = mapping.get(name)
        if target is None:
            raise AttributeError(name)

        module_name: str
        attr_name: str
        if isinstance(target, tuple):
            module_name, attr_name = target
        else:
            module_name, attr_name = target, name

        module = __import__(module_name, fromlist=[attr_name])
        value = getattr(module, attr_name)
        if _should_cache(name, cache):
            namespace[name] = value
        return value

    return loader

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
    "StringRule",
    "StringStats",
    "SchemaColumnFactory",
    "normalize_identifier_columns",
    "normalize_string_columns",
    "format_failure_cases",
    "summarize_schema_errors",
]

_LAZY_EXPORTS: dict[str, LazyMappingValue] = {
    # Runtime
    "CliCommandBase": ("bioetl.application.runtime.cli_base", "CliCommandBase"),
    "CliEntrypoint": ("bioetl.application.runtime.cli_base", "CliEntrypoint"),
    "BioETLError": ("bioetl.application.runtime.errors", "BioETLError"),
    "LoadMetaStore": ("bioetl.application.runtime.load_meta_store", "LoadMetaStore"),
    # HTTP
    "APIClientFactory": ("bioetl.infrastructure.http", "APIClientFactory"),
    "CircuitBreaker": ("bioetl.infrastructure.http", "CircuitBreaker"),
    "CircuitBreakerOpenError": ("bioetl.infrastructure.http", "CircuitBreakerOpenError"),
    "TokenBucketLimiter": ("bioetl.infrastructure.http", "TokenBucketLimiter"),
    "UnifiedAPIClient": ("bioetl.infrastructure.http", "UnifiedAPIClient"),
    "merge_http_configs": ("bioetl.infrastructure.http", "merge_http_configs"),
    # Logging
    "DEFAULT_LOG_LEVEL": ("bioetl.infrastructure.logging", "DEFAULT_LOG_LEVEL"),
    "LogConfig": ("bioetl.infrastructure.logging", "LogConfig"),
    "LogEvents": ("bioetl.infrastructure.logging", "LogEvents"),
    "LogFormat": ("bioetl.infrastructure.logging", "LogFormat"),
    "LoggerConfig": ("bioetl.infrastructure.logging", "LoggerConfig"),
    "MANDATORY_FIELDS": ("bioetl.infrastructure.logging", "MANDATORY_FIELDS"),
    "UnifiedLogger": ("bioetl.infrastructure.logging", "UnifiedLogger"),
    "configure_logging": ("bioetl.infrastructure.logging", "configure_logging"),
    "get_logger": ("bioetl.infrastructure.logging", "get_logger"),
    # IO
    "DeterministicWriteArtifacts": ("bioetl.infrastructure.io", "DeterministicWriteArtifacts"),
    "QCUnits": ("bioetl.infrastructure.io", "QCUnits"),
    "RunArtifacts": ("bioetl.infrastructure.io", "RunArtifacts"),
    "WriteArtifacts": ("bioetl.infrastructure.io", "WriteArtifacts"),
    "WriteResult": ("bioetl.infrastructure.io", "WriteResult"),
    "build_write_artifacts": ("bioetl.infrastructure.io", "build_write_artifacts"),
    "compute_hash": ("bioetl.infrastructure.io", "compute_hash"),
    "emit_qc_artifact": ("bioetl.infrastructure.io", "emit_qc_artifact"),
    "ensure_columns": ("bioetl.infrastructure.io", "ensure_columns"),
    "ensure_hash_columns": ("bioetl.infrastructure.io", "ensure_hash_columns"),
    "escape_delims": ("bioetl.infrastructure.io", "escape_delims"),
    "hash_from_mapping": ("bioetl.infrastructure.io", "hash_from_mapping"),
    "header_rows_serialize": ("bioetl.infrastructure.io", "header_rows_serialize"),
    "plan_run_artifacts": ("bioetl.infrastructure.io", "plan_run_artifacts"),
    "prepare_dataframe": ("bioetl.infrastructure.io", "prepare_dataframe"),
    "serialise_metadata": ("bioetl.infrastructure.io", "serialise_metadata"),
    "serialize_array_fields": ("bioetl.infrastructure.io", "serialize_array_fields"),
    "serialize_objects": ("bioetl.infrastructure.io", "serialize_objects"),
    "serialize_simple_list": ("bioetl.infrastructure.io", "serialize_simple_list"),
    "write_dataset_atomic": ("bioetl.infrastructure.io", "write_dataset_atomic"),
    "write_frame_like": ("bioetl.infrastructure.io", "write_frame_like"),
    "write_yaml_atomic": ("bioetl.infrastructure.io", "write_yaml_atomic"),
    # Schema
    "IdentifierRule": ("bioetl.domain.schema", "IdentifierRule"),
    "IdentifierStats": ("bioetl.domain.schema", "IdentifierStats"),
    "SchemaColumnFactory": ("bioetl.domain.schema", "SchemaColumnFactory"),
    "StringRule": ("bioetl.domain.schema", "StringRule"),
    "StringStats": ("bioetl.domain.schema", "StringStats"),
    "normalize_identifier_columns": ("bioetl.domain.schema", "normalize_identifier_columns"),
    "normalize_string_columns": ("bioetl.domain.schema", "normalize_string_columns"),
    "format_failure_cases": ("bioetl.domain.schema", "format_failure_cases"),
    "summarize_schema_errors": ("bioetl.domain.schema", "summarize_schema_errors"),
}

_lazy_resolver = _resolve_lazy_attr(globals(), _LAZY_EXPORTS, cache=True)


def __getattr__(name: str) -> object:
    return _lazy_resolver(name)
