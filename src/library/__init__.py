"""Public interface for the bioactivity ETL pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

# Only import CLI directly as it's needed for main entry point
# from library.cli import app, main

if TYPE_CHECKING:
    # Type-only imports to avoid circular dependencies
    from library.documents import (
        ALLOWED_SOURCES,
        DATE_TAG_FORMAT,
        DEFAULT_ENV_PREFIX,
        ConfigLoadError,
        DocumentConfig,
        DocumentHTTPGlobalSettings,
        DocumentHTTPRetrySettings,
        DocumentHTTPSettings,
        DocumentInputSettings,
        DocumentIOSettings,
        DocumentOutputSettings,
        DocumentRuntimeSettings,
        SourceToggle,
        load_document_config,
        write_document_outputs,
    )
    from library.etl.extract import fetch_bioactivity_data
    from library.etl.load import write_deterministic_csv, write_qc_artifacts
    from library.etl.run import run_pipeline
    from library.etl.transform import normalize_bioactivity_data
    from library.schemas import NormalizedBioactivitySchema, RawBioactivitySchema
    from library.scripts_base import (
        DeprecatedScriptWrapper,
        create_deprecated_script_wrapper,
    )
    from library.config import (
        APIClientConfig,
        Config,
        CorrelationSettings,
        CsvFormatSettings,
        HTTPGlobalSettings,
        HTTPSettings,
        HTTPSourceSettings,
        IOSettings,
        PaginationSettings,
        ParquetFormatSettings,
        PostprocessSettings,
        RateLimitSettings,
        RetrySettings,
        SourceSettings,
        TransformSettings,
        ValidationSettings,
    )
    from library.telemetry import (
        get_current_trace_id,
        setup_telemetry,
        traced_operation,
    )

# Utils are available via library.utils module

# CLISettings removed as it doesn't exist in library.config

__all__ = [
    "ALLOWED_SOURCES",
    "APIClientConfig",
    "Config",
    "ConfigLoadError",
    "CorrelationSettings",
    "CsvFormatSettings",
    "DATE_TAG_FORMAT",
    "DEFAULT_ENV_PREFIX",
    "DocumentConfig",
    "DocumentHTTPGlobalSettings",
    "DocumentHTTPRetrySettings",
    "DocumentHTTPSettings",
    "DocumentIOSettings",
    "DocumentInputSettings",
    "DocumentOutputSettings",
    "DocumentRuntimeSettings",
    "HTTPGlobalSettings",
    "HTTPSettings",
    "HTTPSourceSettings",
    "IOSettings",
    "NormalizedBioactivitySchema",
    "PaginationSettings",
    "ParquetFormatSettings",
    "PostprocessSettings",
    "RawBioactivitySchema",
    "RateLimitSettings",
    "RetrySettings",
    "SourceSettings",
    "SourceToggle",
    "TransformSettings",
    "ValidationSettings",
    "app",
    "create_deprecated_script_wrapper",
    "DeprecatedScriptWrapper",
    "fetch_bioactivity_data",
    "load_document_config",
    "main",
    "normalize_bioactivity_data",
    "run_pipeline",
    "write_deterministic_csv",
    "write_document_outputs",
    "write_qc_artifacts",
    "setup_telemetry",
    "traced_operation",
    "get_current_trace_id",
]


def __getattr__(name: str):
    """Lazy import for avoiding circular dependencies."""
    if name in {
        "APIClientConfig",
        "Config",
        "CorrelationSettings",
        "CsvFormatSettings",
        "HTTPGlobalSettings",
        "HTTPSettings",
        "HTTPSourceSettings",
        "IOSettings",
        "PaginationSettings",
        "ParquetFormatSettings",
        "PostprocessSettings",
        "RateLimitSettings",
        "RetrySettings",
        "SourceSettings",
        "TransformSettings",
        "ValidationSettings",
    }:
        from library.config import (
            APIClientConfig, Config, CorrelationSettings, CsvFormatSettings,
            HTTPGlobalSettings, HTTPSettings, HTTPSourceSettings, IOSettings,
            PaginationSettings, ParquetFormatSettings, PostprocessSettings,
            RateLimitSettings, RetrySettings, SourceSettings, TransformSettings,
            ValidationSettings
        )

        return globals()[name]

    elif name in {
        "ALLOWED_SOURCES",
        "DATE_TAG_FORMAT",
        "DEFAULT_ENV_PREFIX",
        "ConfigLoadError",
        "DocumentConfig",
        "DocumentHTTPGlobalSettings",
        "DocumentHTTPRetrySettings",
        "DocumentHTTPSettings",
        "DocumentIOSettings",
        "DocumentInputSettings",
        "DocumentOutputSettings",
        "DocumentRuntimeSettings",
        "SourceToggle",
        "load_document_config",
        "write_document_outputs",
    }:
        from library.documents import (
            ALLOWED_SOURCES, DATE_TAG_FORMAT, DEFAULT_ENV_PREFIX, ConfigLoadError,
            DocumentConfig, DocumentHTTPGlobalSettings, DocumentHTTPRetrySettings,
            DocumentHTTPSettings, DocumentIOSettings, DocumentInputSettings,
            DocumentOutputSettings, DocumentRuntimeSettings, SourceToggle,
            load_document_config, write_document_outputs
        )

        return globals()[name]

    elif name in {"fetch_bioactivity_data", "write_deterministic_csv", "write_qc_artifacts", "run_pipeline", "normalize_bioactivity_data"}:
        if name == "fetch_bioactivity_data":
            from library.etl.extract import fetch_bioactivity_data

            return fetch_bioactivity_data
        elif name in {"write_deterministic_csv", "write_qc_artifacts"}:
            from library.etl.load import write_deterministic_csv, write_qc_artifacts

            return globals()[name]
        elif name == "run_pipeline":
            from library.etl.run import run_pipeline

            return run_pipeline
        elif name == "normalize_bioactivity_data":
            from library.etl.transform import normalize_bioactivity_data

            return normalize_bioactivity_data

    elif name in {"NormalizedBioactivitySchema", "RawBioactivitySchema"}:
        from library.schemas import NormalizedBioactivitySchema, RawBioactivitySchema

        return globals()[name]

    elif name in {"DeprecatedScriptWrapper", "create_deprecated_script_wrapper"}:
        from library.scripts_base import (
            DeprecatedScriptWrapper,
            create_deprecated_script_wrapper,
        )

        return globals()[name]

    elif name in {"get_current_trace_id", "setup_telemetry", "traced_operation"}:
        from library.telemetry import (
            get_current_trace_id,
            setup_telemetry,
            traced_operation,
        )

        return globals()[name]

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


# CLISettings removed
# Utils are available via library.utils module
