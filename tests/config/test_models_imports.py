"""Tests for the deprecated config.models compatibility shim."""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.mark.unit
def test_legacy_models_reexports_pipeline_config() -> None:
    """Ensure the deprecated module re-exports the canonical classes."""
    sys.modules.pop("bioetl.config.models", None)

    with pytest.warns(DeprecationWarning):
        legacy_module = importlib.import_module("bioetl.config.models")

    from bioetl.config.models.base import PipelineConfig, PipelineMetadata

    assert legacy_module.PipelineConfig is PipelineConfig
    assert legacy_module.PipelineMetadata is PipelineMetadata
