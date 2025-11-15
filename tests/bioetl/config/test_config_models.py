"""Unit tests for configuration models."""

from __future__ import annotations

import pytest

from bioetl.config.activity import ActivitySourceConfig
from bioetl.config.assay import AssaySourceConfig
from bioetl.config.document import DocumentSourceConfig
from bioetl.config.models.io import IOConfig, IOInputConfig, IOOutputConfig
from bioetl.config.models.logging import LoggingConfig
from bioetl.config.models.runtime import RuntimeConfig
from bioetl.config.models.source import SourceConfig, SourceParameters
from bioetl.config.models.telemetry import TelemetryConfig
from bioetl.config.target import TargetSourceConfig
from bioetl.config.testitem import TestItemSourceConfig


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
def test_target_source_config_from_source_config() -> None:
    source = SourceConfig(
        enabled=True,
        description="targets",
        batch_size=None,
        parameters={"select_fields": ["target_chembl_id", "pref_name"]},
    )

    result = TargetSourceConfig.from_source_config(source)

    assert result.enabled is True
    assert result.batch_size == 25
    assert result.description == "targets"
    assert list(result.parameters.select_fields or ()) == ["target_chembl_id", "pref_name"]


@pytest.mark.unit
def test_activity_source_config_respects_batch_size() -> None:
    source = SourceConfig(
        enabled=True,
        batch_size=15,
        parameters={"select_fields": ["activity_id"]},
    )

    result = ActivitySourceConfig.from_source_config(source)

    assert result.batch_size == 15
    assert list(result.parameters.select_fields or ()) == ["activity_id"]


@pytest.mark.unit
def test_document_source_config_defaults_batch_size() -> None:
    source = SourceConfig(
        enabled=True,
        parameters={},
    )

    result = DocumentSourceConfig.from_source_config(source)

    assert result.batch_size == 25
    assert result.parameters.select_fields is None


@pytest.mark.unit
def test_document_source_config_clamps_batch_size_on_init() -> None:
    config = DocumentSourceConfig(batch_size=100)

    assert config.batch_size == 25


@pytest.mark.unit
def test_target_source_config_enforces_batch_limit() -> None:
    config = TargetSourceConfig(batch_size=None)

    assert config.batch_size == 25


@pytest.mark.unit
def test_assay_source_config_applies_max_url_length() -> None:
    source = SourceConfig(
        enabled=True,
        batch_size=30,
        parameters={"max_url_length": 1500, "handshake_enabled": False},
    )

    result = AssaySourceConfig.from_source_config(source)

    assert result.batch_size == 25
    assert result.max_url_length == 1500
    assert result.parameters.handshake_enabled is False


@pytest.mark.unit
def test_testitem_source_config_uses_custom_page_size() -> None:
    source = SourceConfig(
        enabled=True,
        batch_size=None,
        parameters={"batch_size": 120, "select_fields": ["chembl_id"]},
    )

    result = TestItemSourceConfig.from_source_config(source)

    assert result.page_size == 120
    assert list(result.parameters.select_fields or ()) == ["chembl_id"]


@pytest.mark.unit
def test_testitem_source_config_prefers_batch_size_field() -> None:
    source = SourceConfig(
        enabled=True,
        batch_size=180,
        parameters={"batch_size": 50},
    )

    result = TestItemSourceConfig.from_source_config(source)

    assert result.page_size == 180


@pytest.mark.unit
def test_source_parameters_contains_key_delegates_to_model_dump() -> None:
    params = SourceParameters(alpha=1, beta="value")

    assert SourceParameters.contains_key(params, "alpha") is True
    assert SourceParameters.contains_key(params, "missing") is False
    assert SourceParameters.contains_key(params, 123) is False


@pytest.mark.unit
def test_source_config_contains_uses_static_helper() -> None:
    config = SourceConfig(enabled=False, description="demo")

    assert "description" in config
    assert "enabled" in config
    assert "missing" not in config
    assert 42 not in config
