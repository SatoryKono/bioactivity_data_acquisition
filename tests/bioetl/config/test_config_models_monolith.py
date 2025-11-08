"""Unit tests for legacy monolithic configuration models."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture(scope="module")
def monolith_module() -> ModuleType:
    """Load the legacy configuration models module from its file path."""

    module_name = "bioetl.config._models_monolith"
    module_path = Path(__file__).resolve().parents[3] / "src" / "bioetl" / "config" / "models.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load legacy configuration models module.")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.unit
def test_retry_rate_limit_defaults(monolith_module: ModuleType) -> None:
    """Ensure retry and rate limit configs expose deterministic defaults."""

    retry_config = monolith_module.RetryConfig()
    rate_limit_config = monolith_module.RateLimitConfig()

    assert retry_config.total == 5
    assert retry_config.backoff_multiplier == pytest.approx(2.0)
    assert retry_config.statuses == (408, 429, 500, 502, 503, 504)
    assert rate_limit_config.max_calls == 10
    assert rate_limit_config.period == pytest.approx(1.0)


@pytest.mark.unit
def test_http_client_config_customization(monolith_module: ModuleType) -> None:
    """Validate HTTP client configuration composition."""

    client_config = monolith_module.HTTPClientConfig(
        timeout_sec=30.0,
        connect_timeout_sec=5.0,
        headers={"User-Agent": "custom-agent/0.1"},
    )

    assert client_config.timeout_sec == pytest.approx(30.0)
    assert client_config.connect_timeout_sec == pytest.approx(5.0)
    assert client_config.headers["User-Agent"] == "custom-agent/0.1"
    assert client_config.rate_limit.max_calls == 10
    assert client_config.circuit_breaker.failure_threshold == 5


@pytest.mark.unit
def test_determinism_config_validator(monolith_module: ModuleType) -> None:
    """Cover the determinism validator positive and negative paths."""

    determinism_config = monolith_module.DeterminismConfig(
        sort=monolith_module.DeterminismSortingConfig(by=("id",), ascending=()),
        column_order=("a", "b"),
    )

    assert determinism_config.enabled is True
    assert list(determinism_config.sort.by) == ["id"]

    with pytest.raises(ValueError, match="determinism.sort.by must not contain duplicate columns"):
        monolith_module.DeterminismConfig(
            sort=monolith_module.DeterminismSortingConfig(by=("id", "id"))
        )

    with pytest.raises(
        ValueError,
        match="determinism.column_order must not contain duplicate columns",
    ):
        monolith_module.DeterminismConfig(column_order=("id", "id"))

    with pytest.raises(
        ValueError,
        match="determinism.sort.ascending must be empty or match determinism.sort.by length",
    ):
        monolith_module.DeterminismConfig(
            sort=monolith_module.DeterminismSortingConfig(by=("id",), ascending=(True, False))
        )


@pytest.mark.unit
def test_pipeline_config_column_order_requires_schema(monolith_module: ModuleType) -> None:
    """Ensure pipeline config enforces schema when column order is provided."""

    with pytest.raises(
        ValueError,
        match="determinism.column_order requires validation.schema_out to be set",
    ):
        monolith_module.PipelineConfig(
            version=1,
            pipeline=monolith_module.PipelineMetadata(name="test", version="0.1.0"),
            http=monolith_module.HTTPConfig(default=monolith_module.HTTPClientConfig()),
            determinism=monolith_module.DeterminismConfig(column_order=("id",)),
        )

    pipeline_config = monolith_module.PipelineConfig(
        version=1,
        pipeline=monolith_module.PipelineMetadata(name="test", version="0.1.0"),
        http=monolith_module.HTTPConfig(default=monolith_module.HTTPClientConfig()),
        determinism=monolith_module.DeterminismConfig(column_order=("id",)),
        validation=monolith_module.ValidationConfig(schema_out="bioetl.schemas.activity.ActivitySchema"),
    )

    assert pipeline_config.version == 1
    assert pipeline_config.validation.schema_out == "bioetl.schemas.activity.ActivitySchema"
