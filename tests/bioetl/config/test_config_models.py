"""Unit tests for configuration models."""

from __future__ import annotations

import pytest

from pydantic import ValidationError

from bioetl.config.models import (
    DeterminismConfig,
    DeterminismSortingConfig,
    HTTPClientConfig,
)
from bioetl.config.models.io import IOConfig, IOInputConfig, IOOutputConfig
from bioetl.config.models.logging import LoggingConfig
from bioetl.config.models.runtime import RuntimeConfig
from bioetl.config.models.telemetry import TelemetryConfig


@pytest.mark.unit
def test_runtime_config_defaults() -> None:
    config = RuntimeConfig()

    assert config.parallelism == 4
    assert config.chunk_rows == 100_000
    assert config.dry_run is False
    assert config.seed == 42


@pytest.mark.unit
def test_io_config_roundtrip() -> None:
    config = IOConfig(
        input=IOInputConfig(format="parquet", encoding="utf-8", header=False, path="input.parquet"),
        output=IOOutputConfig(
            format="csv", partition_by=("year",), overwrite=False, path="output.csv"
        ),
    )

    assert config.input.format == "parquet"
    assert config.input.header is False
    assert config.input.path == "input.parquet"
    assert config.output.partition_by == ("year",)
    assert config.output.overwrite is False
    assert config.output.path == "output.csv"


@pytest.mark.unit
def test_logging_config_context_fields() -> None:
    config = LoggingConfig(level="DEBUG", context_fields=("pipeline", "run_id", "env"))

    assert config.level == "DEBUG"
    assert config.format == "json"
    assert "env" in config.context_fields


@pytest.mark.unit
def test_telemetry_config_sampling() -> None:
    config = TelemetryConfig(
        enabled=True, exporter="jaeger", endpoint="http://localhost:14268", sampling_ratio=0.5
    )

    assert config.enabled is True
    assert config.exporter == "jaeger"
    assert config.endpoint == "http://localhost:14268"
    assert pytest.approx(config.sampling_ratio) == 0.5


@pytest.mark.unit
def test_determinism_config_validates_sort_lengths() -> None:
    with pytest.raises(
        ValueError, match="determinism.sort.ascending must be empty or match determinism.sort.by length"
    ):
        DeterminismConfig(
            sort=DeterminismSortingConfig(by=["id"], ascending=[True, False])
        )


@pytest.mark.unit
def test_determinism_config_detects_duplicate_columns() -> None:
    with pytest.raises(ValueError, match="determinism.sort.by must not contain duplicate columns"):
        DeterminismConfig(sort=DeterminismSortingConfig(by=["id", "id"]))

    with pytest.raises(ValueError, match="determinism.column_order must not contain duplicate columns"):
        DeterminismConfig(column_order=("id", "id"))


@pytest.mark.unit
def test_http_client_config_defaults_and_extra_fields() -> None:
    config = HTTPClientConfig()

    assert config.timeout_sec == 60.0
    assert config.headers["User-Agent"] == "BioETL/1.0 (UnifiedAPIClient)"
    assert config.rate_limit.max_calls == 10

    with pytest.raises(ValidationError):
        HTTPClientConfig(unknown_option=True)  # type: ignore[arg-type]
