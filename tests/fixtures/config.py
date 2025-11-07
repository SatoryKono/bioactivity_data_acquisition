"""Configuration-related pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.config import PipelineConfig
from bioetl.config.models.base import PipelineMetadata
from bioetl.config.models.cli import CLIConfig
from bioetl.config.models.determinism import (
    DeterminismConfig,
    DeterminismHashingConfig,
    DeterminismSortingConfig,
)
from bioetl.config.models.http import HTTPClientConfig, HTTPConfig, RetryConfig
from bioetl.config.models.paths import MaterializationConfig
from bioetl.config.models.postprocess import PostprocessConfig
from bioetl.config.models.source import SourceConfig
from bioetl.config.models.validation import ValidationConfig


__all__ = ["pipeline_config_fixture"]


@pytest.fixture  # type: ignore[misc]
def pipeline_config_fixture(tmp_output_dir: Path) -> PipelineConfig:
    """Sample PipelineConfig for testing."""
    return PipelineConfig(  # type: ignore[call-arg]
        version=1,
        pipeline=PipelineMetadata(  # type: ignore[call-arg]
            name="activity_chembl",
            version="1.0.0",
            description="Test activity pipeline",
        ),
        http=HTTPConfig(
            default=HTTPClientConfig(
                timeout_sec=30.0,
                connect_timeout_sec=10.0,
                read_timeout_sec=30.0,
                retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
            ),
        ),
        materialization=MaterializationConfig(root=str(tmp_output_dir)),
        determinism=DeterminismConfig(  # type: ignore[call-arg]
            sort=DeterminismSortingConfig(
                by=[],
                ascending=[],
            ),
            hashing=DeterminismHashingConfig(
                business_key_fields=(),
            ),
        ),
        validation=ValidationConfig(
            schema_out=None,
            strict=True,
            coerce=True,
        ),
        postprocess=PostprocessConfig(),
        sources={
            "chembl": SourceConfig(  # type: ignore[call-arg,dict-item]
                enabled=True,
                parameters={"base_url": "https://www.ebi.ac.uk/chembl/api/data"},
            ),
        },
        cli=CLIConfig(date_tag="20240101"),  # type: ignore[attr-defined]
    )
