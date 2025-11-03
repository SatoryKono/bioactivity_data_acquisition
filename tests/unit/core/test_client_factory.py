"""Tests for :mod:`bioetl.core.client_factory`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest

from bioetl.config import PipelineConfig
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.pipelines.activity import ActivityPipeline
from bioetl.pipelines.assay import AssayPipeline
from bioetl.pipelines.document import DocumentPipeline
from bioetl.pipelines.testitem import TestItemPipeline


def _build_pipeline_config(tmp_path: Path) -> PipelineConfig:
    cache_dir = tmp_path / "cache"
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    cache_dir.mkdir()
    input_root.mkdir()
    output_root.mkdir()

    config_data: dict[str, Any] = {
        "version": 1,
        "pipeline": {"name": "test", "entity": "test"},
        "http": {
            "global": {
                "timeout_sec": 120.0,
                "retries": {
                    "total": 7,
                    "backoff_multiplier": 2.5,
                    "backoff_max": 240.0,
                    "statuses": [500, 502, 503],
                },
                "rate_limit": {"max_calls": 5, "period": 30.0},
                "headers": {"X-Global": "global"},
            },
            "chembl_profile": {
                "timeout_sec": 45.0,
                "connect_timeout_sec": 5.0,
                "read_timeout_sec": 25.0,
                "retries": {
                    "total": 9,
                    "backoff_multiplier": 1.0,
                    "backoff_max": 90.0,
                    "statuses": [429, 500],
                },
                "rate_limit": {"max_calls": 2, "period": 5.0},
                "rate_limit_jitter": False,
                "headers": {"X-Profile": "chembl"},
            },
        },
        "sources": {
            "chembl": {
                "enabled": True,
                "base_url": "https://example.org/api",
                "batch_size": 20,
                "max_url_length": 1900,
                "http_profile": "chembl_profile",
                "headers": {"Authorization": "Bearer token", "X-Source": "chembl"},
            }
        },
        "fallbacks": {
            "enabled": False,
            "strategies": ["partial_retry"],
            "partial_retry_max": 4,
            "circuit_breaker": {"failure_threshold": 6, "timeout_sec": 180.0},
        },
        "cache": {
            "enabled": True,
            "directory": str(cache_dir),
            "ttl": 86400,
            "release_scoped": True,
            "maxsize": 256,
        },
        "paths": {
            "input_root": str(input_root),
            "output_root": str(output_root),
        },
        "determinism": {},
    }

    return PipelineConfig.model_validate(config_data)


def test_factory_merges_http_profiles(tmp_path: Path) -> None:
    """The factory should merge global/profile/source headers and policies."""

    config = _build_pipeline_config(tmp_path)
    factory = APIClientFactory.from_pipeline_config(config)
    source_config = ensure_target_source_config(config.sources["chembl"], defaults={})

    api_config = factory.create("chembl", source_config)

    assert api_config.retry_total == 9
    assert api_config.retry_backoff_factor == 1.0
    assert api_config.rate_limit_max_calls == 2
    assert api_config.rate_limit_period == 5.0
    assert api_config.rate_limit_jitter is False
    assert api_config.timeout_connect == 5.0
    assert api_config.timeout_read == 25.0
    assert api_config.headers["X-Global"] == "global"
    assert api_config.headers["X-Profile"] == "chembl"
    assert api_config.headers["Authorization"] == "Bearer token"
    assert api_config.headers["X-Source"] == "chembl"
    assert api_config.cb_failure_threshold == 6
    assert api_config.cb_timeout == 180.0
    assert api_config.fallback_enabled is False
    assert api_config.fallback_strategies == ["partial_retry"]
    assert api_config.partial_retry_max == 4


def test_ensure_target_source_config_defaults() -> None:
    """Defaults should be applied when the source definition is missing."""

    defaults = {"base_url": "https://fallback", "batch_size": 25}
    resolved = ensure_target_source_config(None, defaults=defaults)

    assert resolved.base_url == "https://fallback"
    assert resolved.batch_size == 25


@pytest.mark.parametrize(
    ("pipeline_cls", "module_path"),
    [
        (ActivityPipeline, "bioetl.pipelines.activity"),
        (AssayPipeline, "bioetl.pipelines.assay"),
        (DocumentPipeline, "bioetl.pipelines.document"),
        (TestItemPipeline, "bioetl.pipelines.testitem"),
    ],
)
def test_pipeline_constructors_use_factory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    pipeline_cls: type[Any],
    module_path: str,
) -> None:
    """Pipelines should construct :class:`UnifiedAPIClient` via the factory."""

    config = _build_pipeline_config(tmp_path)
    module = importlib.import_module(module_path)
    base_module = importlib.import_module("bioetl.pipelines.base")
    chembl_module = importlib.import_module("bioetl.core.chembl")
    captured: list[Any] = []

    class DummyClient:
        def __init__(self, api_config):
            captured.append(api_config)
            self.config = api_config

        def request_json(self, url, params=None, method="GET", **kwargs):
            return {"chembl_db_version": "CHEMBL_TEST"}

    original_builder = chembl_module.build_chembl_client_context

    def recording_helper(self, *, defaults=None, batch_size_cap=None):  # type: ignore[override]
        context = original_builder(
            self.config,
            defaults=defaults,
            batch_size_cap=batch_size_cap,
        )
        return chembl_module.ChemblClientContext(
            client=DummyClient(context.client.config),
            source_config=context.source_config,
            batch_size=context.batch_size,
            max_url_length=context.max_url_length,
            base_url=context.base_url,
        )

    monkeypatch.setattr(base_module.PipelineBase, "_init_chembl_client", recording_helper)

    if hasattr(module, "UnifiedAPIClient"):
        monkeypatch.setattr(module, "UnifiedAPIClient", DummyClient)

    pipeline_cls(config, "run-id")

    assert captured, "UnifiedAPIClient was not instantiated"
    api_config = captured[0]
    assert api_config.retry_total == 9
    assert api_config.rate_limit_max_calls == 2
    assert api_config.headers["X-Global"] == "global"
    assert api_config.headers["X-Profile"] == "chembl"
    assert api_config.headers["Authorization"] == "Bearer token"
