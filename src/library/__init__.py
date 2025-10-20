"""Public interface for the bioactivity ETL pipeline."""

from __future__ import annotations

from library.cli import app as _app, main as _main
from library.config import (
    APIClientConfig,
    Config,
    CorrelationSettings,
    CsvFormatSettings,
    DeterminismSettings,
    HTTPGlobalSettings,
    HTTPSettings,
    HTTPSourceSettings,
    IOSettings,
    LoggingSettings,
    OutputSettings,
    PaginationSettings,
    ParquetFormatSettings,
    PostprocessSettings,
    QCStepSettings,
    QCValidationSettings,
    RateLimitSettings,
    RetrySettings,
    SortSettings,
    SourceSettings,
    TransformSettings,
    ValidationSettings,
)

try:  # pragma: no cover - optional wiring for partially generated configs
    # Import module and fetch attribute dynamically to satisfy type checkers
    import library.config as _config  # type: ignore[import-not-found]

    CLISettings = getattr(_config, "CLISettings", None)  # type: ignore[assignment]
except Exception:  # pragma: no cover - optional wiring for partially generated configs
    CLISettings = None  # type: ignore[assignment]
from library.documents import (
    ALLOWED_SOURCES,
    DATE_TAG_FORMAT,
    DEFAULT_ENV_PREFIX,
    ConfigLoadError,
    DocumentConfig,
    DocumentETLResult,
    DocumentHTTPError,
    DocumentHTTPGlobalSettings,
    DocumentHTTPRetrySettings,
    DocumentHTTPSettings,
    DocumentInputSettings,
    DocumentIOError,
    DocumentIOSettings,
    DocumentOutputSettings,
    DocumentPipelineError,
    DocumentQCError,
    DocumentRuntimeSettings,
    DocumentValidationError,
    SourceToggle,
    load_document_config,
    read_document_input,
    run_document_etl,
    write_document_outputs,
)
from library.etl.extract import fetch_bioactivity_data as _fetch_bioactivity_data
from library.etl.load import write_deterministic_csv as _write_deterministic_csv, write_qc_artifacts as _write_qc_artifacts
from library.etl.run import run_pipeline as _run_pipeline
from library.etl.transform import normalize_bioactivity_data as _normalize_bioactivity_data
from library.schemas import NormalizedBioactivitySchema as _NormalizedBioactivitySchema, RawBioactivitySchema as _RawBioactivitySchema
from library.scripts_base import DeprecatedScriptWrapper as _DeprecatedScriptWrapper, create_deprecated_script_wrapper as _create_deprecated_script_wrapper
from library.telemetry import get_current_trace_id as _get_current_trace_id, setup_telemetry as _setup_telemetry, traced_operation as _traced_operation
from library.utils import *  # noqa: F403
from library.utils import __all__ as _utils_all

_exports = [
    "ALLOWED_SOURCES",
    "APIClientConfig",
    "Config",
    "ConfigLoadError",
    "CorrelationSettings",
    "CsvFormatSettings",
    "DATE_TAG_FORMAT",
    "DEFAULT_ENV_PREFIX",
    "DeterminismSettings",
    "DocumentConfig",
    "DocumentETLResult",
    "DocumentHTTPError",
    "DocumentHTTPGlobalSettings",
    "DocumentHTTPRetrySettings",
    "DocumentHTTPSettings",
    "DocumentIOError",
    "DocumentIOSettings",
    "DocumentInputSettings",
    "DocumentOutputSettings",
    "DocumentPipelineError",
    "DocumentQCError",
    "DocumentRuntimeSettings",
    "DocumentValidationError",
    "HTTPGlobalSettings",
    "HTTPSettings",
    "HTTPSourceSettings",
    "IOSettings",
    "LoggingSettings",
    "NormalizedBioactivitySchema",
    "OutputSettings",
    "PaginationSettings",
    "ParquetFormatSettings",
    "PostprocessSettings",
    "QCStepSettings",
    "QCValidationSettings",
    "RateLimitSettings",
    "RawBioactivitySchema",
    "RetrySettings",
    "SortSettings",
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
    "read_document_input",
    "run_document_etl",
    "run_pipeline",
    "write_deterministic_csv",
    "write_document_outputs",
    "write_qc_artifacts",
    "setup_telemetry",
    "traced_operation",
    "get_current_trace_id",
]

if CLISettings is not None:
    _exports.append("CLISettings")

_exports += list(_utils_all)
__all__ = _exports
