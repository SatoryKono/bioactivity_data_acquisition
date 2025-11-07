"""Integration tests verifying pipeline configuration overlays."""

from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.config.loader import load_config


PIPELINE_CONFIGS = [
    Path("configs/pipelines/activity/activity_chembl.yaml"),
    Path("configs/pipelines/assay/assay_chembl.yaml"),
    Path("configs/pipelines/document/document_chembl.yaml"),
    Path("configs/pipelines/target/target_chembl.yaml"),
    Path("configs/pipelines/testitem/testitem_chembl.yaml"),
]


@pytest.mark.integration
@pytest.mark.parametrize("config_path", PIPELINE_CONFIGS, ids=[path.stem for path in PIPELINE_CONFIGS])
def test_pipeline_config_profile_merge(config_path: Path) -> None:
    config = load_config(config_path, include_default_profiles=False)

    # Runtime defaults propagated from base profile
    assert config.runtime.parallelism >= 1
    assert config.io.output.format in {"parquet", "csv"}

    # Logging / telemetry sections present with defaults
    assert config.logging.level
    assert config.telemetry.sampling_ratio > 0.0

    # Hashing columns configured explicitly
    hashing = config.determinism.hashing
    assert hashing.business_key_column
    assert hashing.row_hash_column

    # Chembl based pipelines should expose source overrides
    chembl_source = config.sources.get("chembl")
    assert chembl_source is not None
    assert "base_url" in chembl_source.parameters

