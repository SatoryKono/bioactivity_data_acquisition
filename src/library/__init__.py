"""Public interface for the bioactivity ETL pipeline."""
from __future__ import annotations

from library.cli import app, main, ExitCode
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
    from library.config import CLISettings
except ImportError:  # pragma: no cover - optional wiring for partially generated configs
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
from library.etl.extract import fetch_bioactivity_data
from library.etl.load import write_deterministic_csv, write_qc_artifacts
from library.etl.run import run_pipeline
from library.etl.transform import normalize_bioactivity_data
from library.schemas import NormalizedBioactivitySchema, RawBioactivitySchema
from library.scripts_base import DeprecatedScriptWrapper, create_deprecated_script_wrapper
from library.utils import *  # noqa: F403
from library.utils import __all__ as _utils_all
from library.telemetry import setup_telemetry, traced_operation, get_current_trace_id

__all__ = [
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
    __all__.append("CLISettings")
__all__ += list(_utils_all)
