"""Public interface for the bioactivity ETL pipeline."""

from __future__ import annotations

from library.cli import app as _app, main as _main
import library.config as _config

try:  # pragma: no cover - optional wiring for partially generated configs
    # Import module and fetch attribute dynamically to satisfy type checkers
    import library.config as _config  # type: ignore[import-not-found]

    CLISettings = getattr(_config, "CLISettings", None)  # type: ignore[assignment]
except Exception:  # pragma: no cover - optional wiring for partially generated configs
    CLISettings = None  # type: ignore[assignment]
import library.documents as _documents
from library.etl.extract import fetch_bioactivity_data as _fetch_bioactivity_data
from library.etl.load import write_deterministic_csv as _write_deterministic_csv, write_qc_artifacts as _write_qc_artifacts
from library.etl.run import run_pipeline as _run_pipeline
from library.etl.transform import normalize_bioactivity_data as _normalize_bioactivity_data
import library.schemas as _schemas
import library.scripts_base as _scripts_base
import library.telemetry as _telemetry
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

# Re-export selected symbols to module namespace (avoid unused-import warnings)
app = _app
main = _main

# Config
APIClientConfig = _config.APIClientConfig
Config = _config.Config
CorrelationSettings = _config.CorrelationSettings
CsvFormatSettings = _config.CsvFormatSettings
DeterminismSettings = _config.DeterminismSettings
HTTPGlobalSettings = _config.HTTPGlobalSettings
HTTPSettings = _config.HTTPSettings
HTTPSourceSettings = _config.HTTPSourceSettings
IOSettings = _config.IOSettings
LoggingSettings = _config.LoggingSettings
OutputSettings = _config.OutputSettings
PaginationSettings = _config.PaginationSettings
ParquetFormatSettings = _config.ParquetFormatSettings
PostprocessSettings = _config.PostprocessSettings
QCStepSettings = _config.QCStepSettings
QCValidationSettings = _config.QCValidationSettings
RateLimitSettings = _config.RateLimitSettings
RetrySettings = _config.RetrySettings
SortSettings = _config.SortSettings
SourceSettings = _config.SourceSettings
TransformSettings = _config.TransformSettings
ValidationSettings = _config.ValidationSettings

# Documents
ALLOWED_SOURCES = _documents.ALLOWED_SOURCES
DATE_TAG_FORMAT = _documents.DATE_TAG_FORMAT
DEFAULT_ENV_PREFIX = _documents.DEFAULT_ENV_PREFIX
ConfigLoadError = _documents.ConfigLoadError
DocumentConfig = _documents.DocumentConfig
DocumentETLResult = _documents.DocumentETLResult
DocumentHTTPError = _documents.DocumentHTTPError
DocumentHTTPGlobalSettings = _documents.DocumentHTTPGlobalSettings
DocumentHTTPRetrySettings = _documents.DocumentHTTPRetrySettings
DocumentHTTPSettings = _documents.DocumentHTTPSettings
DocumentInputSettings = _documents.DocumentInputSettings
DocumentIOError = _documents.DocumentIOError
DocumentIOSettings = _documents.DocumentIOSettings
DocumentOutputSettings = _documents.DocumentOutputSettings
DocumentPipelineError = _documents.DocumentPipelineError
DocumentQCError = _documents.DocumentQCError
DocumentRuntimeSettings = _documents.DocumentRuntimeSettings
DocumentValidationError = _documents.DocumentValidationError
SourceToggle = _documents.SourceToggle
load_document_config = _documents.load_document_config
read_document_input = _documents.read_document_input
run_document_etl = _documents.run_document_etl
write_document_outputs = _documents.write_document_outputs

# ETL
fetch_bioactivity_data = _fetch_bioactivity_data
write_deterministic_csv = _write_deterministic_csv
write_qc_artifacts = _write_qc_artifacts
run_pipeline = _run_pipeline
normalize_bioactivity_data = _normalize_bioactivity_data

# Schemas
NormalizedBioactivitySchema = _schemas.NormalizedBioactivitySchema
RawBioactivitySchema = _schemas.RawBioactivitySchema

# Scripts base
DeprecatedScriptWrapper = _scripts_base.DeprecatedScriptWrapper
create_deprecated_script_wrapper = _scripts_base.create_deprecated_script_wrapper

# Telemetry
get_current_trace_id = _telemetry.get_current_trace_id
setup_telemetry = _telemetry.setup_telemetry
traced_operation = _telemetry.traced_operation
