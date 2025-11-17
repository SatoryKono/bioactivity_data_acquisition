from __future__ import annotations

from importlib import import_module

import pytest


@pytest.mark.unit
def test_core_public_exports_are_available() -> None:
    import bioetl.core as core  # Import lazily once for assertions

    expected_symbols = {
        # HTTP layer
        "UnifiedAPIClient": "bioetl.infrastructure.http",
        "APIClientFactory": "bioetl.infrastructure.http",
        "CircuitBreaker": "bioetl.infrastructure.http",
        "CircuitBreakerOpenError": "bioetl.infrastructure.http",
        "TokenBucketLimiter": "bioetl.infrastructure.http",
        "merge_http_configs": "bioetl.infrastructure.http",
        # Logging layer
        "UnifiedLogger": "bioetl.infrastructure.logging",
        "LogEvents": "bioetl.infrastructure.logging",
        # IO layer
        "DeterministicWriteArtifacts": "bioetl.infrastructure.io",
        "RunArtifacts": "bioetl.infrastructure.io",
        "WriteArtifacts": "bioetl.infrastructure.io",
        "WriteResult": "bioetl.infrastructure.io",
        "build_write_artifacts": "bioetl.infrastructure.io",
        "plan_run_artifacts": "bioetl.infrastructure.io",
        "write_dataset_atomic": "bioetl.infrastructure.io",
        "compute_hash": "bioetl.infrastructure.io",
        "hash_from_mapping": "bioetl.infrastructure.io",
        "ensure_columns": "bioetl.infrastructure.io",
        "ensure_hash_columns": "bioetl.infrastructure.io",
        "emit_qc_artifact": "bioetl.infrastructure.io",
        "prepare_dataframe": "bioetl.infrastructure.io",
        "serialise_metadata": "bioetl.infrastructure.io",
        "write_frame_like": "bioetl.infrastructure.io",
        "write_yaml_atomic": "bioetl.infrastructure.io",
        "QCUnits": "bioetl.infrastructure.io",
        # Schema layer
        "SchemaColumnFactory": "bioetl.domain.schema",
        "IdentifierRule": "bioetl.domain.schema",
        "IdentifierStats": "bioetl.domain.schema",
        "normalize_identifier_columns": "bioetl.domain.schema",
        "normalize_string_columns": "bioetl.domain.schema",
        "StringRule": "bioetl.domain.schema",
        "StringStats": "bioetl.domain.schema",
        "format_failure_cases": "bioetl.domain.schema",
        "summarize_schema_errors": "bioetl.domain.schema",
        # Runtime layer
        "BioETLError": "bioetl.application.runtime",
        "CliCommandBase": "bioetl.application.runtime",
        "CliEntrypoint": "bioetl.application.runtime",
        "LoadMetaStore": "bioetl.application.runtime",
        # Utils layer
        "join_activity_with_molecule": "bioetl.core.utils",
        "load_vocab_store": "bioetl.core.utils",
        "get_ids": "bioetl.core.utils",
        "clear_vocab_store_cache": "bioetl.core.utils",
    }

    for symbol, module_name in expected_symbols.items():
        assert hasattr(core, symbol), f"{symbol} missing from bioetl.core"
        exported = getattr(core, symbol)
        module = import_module(module_name)
        module_value = getattr(module, symbol)
        assert (
            exported is module_value
        ), f"{symbol} in bioetl.core differs from {module_name}"


@pytest.mark.unit
@pytest.mark.parametrize(
    "module_name",
    [
        "bioetl.core.api_client",
        "bioetl.core.logger",
        "bioetl.core.log_events",
        "bioetl.core.output",
        "bioetl.core.hashing",
        "bioetl.core.serialization",
        "bioetl.core.normalizers",
        "bioetl.core.cli_base",
        "bioetl.core.errors",
        "bioetl.core.load_meta_store",
        "bioetl.core.base_pipeline",
        "bioetl.core.config",
        "bioetl.core.config.base_source",
        "bioetl.core.interfaces",
        "bioetl.application.runtime.base_pipeline_compat",
        "bioetl.application.runtime.base_source",
    ],
)
def test_removed_compat_modules_raise_import_error(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        import_module(module_name)

