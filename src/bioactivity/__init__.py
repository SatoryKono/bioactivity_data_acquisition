"""Public interface for the bioactivity ETL pipeline."""
from __future__ import annotations

from bioactivity.cli import app, main
from bioactivity.config import (
    APIClientConfig,
    Config,
    DeterminismSettings,
    HTTPSettings,
    IOSettings,
    OutputPaths,
    PaginationSettings,
    PostprocessSettings,
    ProjectSettings,
    RetrySettings,
    RuntimeSettings,
    SecretsSettings,
    SourceSettings,
    ValidationSettings,
)

try:  # pragma: no cover - optional wiring for partially generated configs
    from bioactivity.config import CLISettings
except ImportError:  # pragma: no cover - optional wiring for partially generated configs
    CLISettings = None  # type: ignore[assignment]
from bioactivity.etl.extract import fetch_bioactivity_data
from bioactivity.etl.load import write_deterministic_csv, write_qc_artifacts
from bioactivity.etl.run import run_pipeline
from bioactivity.etl.transform import normalize_bioactivity_data
from bioactivity.schemas import NormalizedBioactivitySchema, RawBioactivitySchema
from bioactivity.utils import *  # noqa: F403
from bioactivity.utils import __all__ as _utils_all

__all__ = [
    "APIClientConfig",
    "Config",
    "DeterminismSettings",
    "HTTPSettings",
    "IOSettings",
    "OutputPaths",
    "PaginationSettings",
    "PostprocessSettings",
    "ProjectSettings",
    "RetrySettings",
    "RuntimeSettings",
    "SecretsSettings",
    "SourceSettings",
    "ValidationSettings",
    "app",
    "fetch_bioactivity_data",
    "main",
    "normalize_bioactivity_data",
    "NormalizedBioactivitySchema",
    "RawBioactivitySchema",
    "run_pipeline",
    "write_deterministic_csv",
    "write_qc_artifacts",
]

if CLISettings is not None:
    __all__.append("CLISettings")
__all__ += list(_utils_all)
