from __future__ import annotations

from importlib import import_module

import pytest


@pytest.mark.unit
def test_core_public_exports_are_available() -> None:
    import bioetl.core as core  # Import lazily once for assertions

    expected_symbols = {
        # HTTP layer
        "UnifiedAPIClient": "bioetl.core.http",
        "APIClientFactory": "bioetl.core.http",
        "CircuitBreaker": "bioetl.core.http",
        "CircuitBreakerOpenError": "bioetl.core.http",
        "TokenBucketLimiter": "bioetl.core.http",
        "merge_http_configs": "bioetl.core.http",
        # Logging layer
        "UnifiedLogger": "bioetl.core.logging",
        "LogEvents": "bioetl.core.logging",
        # IO layer
        "DeterministicWriteArtifacts": "bioetl.core.io",
        "RunArtifacts": "bioetl.core.io",
        "WriteArtifacts": "bioetl.core.io",
        "WriteResult": "bioetl.core.io",
        "build_write_artifacts": "bioetl.core.io",
        "plan_run_artifacts": "bioetl.core.io",
        "write_dataset_atomic": "bioetl.core.io",
        "compute_hash": "bioetl.core.io",
        "hash_from_mapping": "bioetl.core.io",
        "ensure_columns": "bioetl.core.io",
        "ensure_hash_columns": "bioetl.core.io",
        "emit_qc_artifact": "bioetl.core.io",
        "prepare_dataframe": "bioetl.core.io",
        "serialise_metadata": "bioetl.core.io",
        "write_frame_like": "bioetl.core.io",
        "write_yaml_atomic": "bioetl.core.io",
        "QCUnits": "bioetl.core.io",
        # Schema layer
        "SchemaColumnFactory": "bioetl.core.schema",
        "IdentifierRule": "bioetl.core.schema",
        "IdentifierStats": "bioetl.core.schema",
        "normalize_identifier_columns": "bioetl.core.schema",
        "normalize_string_columns": "bioetl.core.schema",
        "StringRule": "bioetl.core.schema",
        "StringStats": "bioetl.core.schema",
        "format_failure_cases": "bioetl.core.schema",
        "summarize_schema_errors": "bioetl.core.schema",
        # Runtime layer
        "BioETLError": "bioetl.core.runtime",
        "CliCommandBase": "bioetl.core.runtime",
        "CliEntrypoint": "bioetl.core.runtime",
        "LoadMetaStore": "bioetl.core.runtime",
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
        "bioetl.core.runtime.base_pipeline_compat",
        "bioetl.core.runtime.base_source",
    ],
)
def test_removed_compat_modules_raise_import_error(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        import_module(module_name)

