"""Tests for configuration loader."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from bioetl.config import PipelineConfig, load_config, parse_cli_overrides


def test_load_base_config():
    """Test loading base configuration."""
    config = load_config(Path("configs/base.yaml"))
    assert config.version == 1
    assert config.pipeline.name == "base"
    assert config.http["global"].timeout_sec == 60.0


def test_inheritance():
    """Test configuration inheritance."""
    config = load_config(Path("configs/profiles/dev.yaml"))
    assert config.pipeline.name == "dev"
    assert config.http["global"].retries.total == 2  # From dev
    assert config.cache.enabled is True


def test_cli_overrides(tmp_path):
    """Test CLI overrides."""
    # Create test config
    config_file = tmp_path / "test.yaml"
    config_file.write_text(
        """
version: 1
pipeline:
  name: test
  entity: test
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
    rate_limit:
      max_calls: 5
      period: 15.0
cache:
  enabled: true
  directory: "data/cache"
  ttl: 86400
  release_scoped: true
paths:
  input_root: "data/input"
  output_root: "data/output"
determinism:
  sort:
    by: []
    ascending: []
  column_order: []
postprocess: {}
qc:
  enabled: true
  severity_threshold: warning
cli: {}
"""
    )

    overrides = {"http": {"global": {"timeout_sec": 30.0}}}
    config = load_config(config_file, overrides=overrides)
    assert config.http["global"].timeout_sec == 30.0


def test_env_overrides(tmp_path):
    """Test environment variable overrides."""
    config_file = tmp_path / "test.yaml"
    config_file.write_text(
        """
version: 1
pipeline:
  name: test
  entity: test
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
    rate_limit:
      max_calls: 5
      period: 15.0
cache:
  enabled: true
  directory: "data/cache"
  ttl: 86400
  release_scoped: true
paths:
  input_root: "data/input"
  output_root: "data/output"
determinism:
  sort:
    by: []
    ascending: []
  column_order: []
postprocess: {}
qc:
  enabled: true
  severity_threshold: warning
cli: {}
"""
    )

    os.environ["BIOETL_HTTP__GLOBAL__TIMEOUT_SEC"] = "45.0"

    try:
        config = load_config(config_file)
        assert config.http["global"].timeout_sec == 45.0
    finally:
        del os.environ["BIOETL_HTTP__GLOBAL__TIMEOUT_SEC"]


def test_config_hash_stability(tmp_path):
    """Test config_hash stability."""
    config_file = tmp_path / "test.yaml"
    config_file.write_text(
        """
version: 1
pipeline:
  name: test
  entity: test
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
    rate_limit:
      max_calls: 5
      period: 15.0
cache:
  enabled: true
  directory: "data/cache"
  ttl: 86400
  release_scoped: true
paths:
  input_root: "data/input"
  output_root: "data/output"
determinism:
  sort:
    by: []
    ascending: []
  column_order: []
postprocess: {}
qc:
  enabled: true
  severity_threshold: warning
cli: {}
"""
    )

    config1 = load_config(config_file)
    config2 = load_config(config_file)
    assert config1.config_hash == config2.config_hash


def test_parse_cli_overrides():
    """Test parsing CLI overrides."""
    overrides = ["http.global.timeout_sec=45", "sources.chembl.batch_size=20"]
    result = parse_cli_overrides(overrides)
    assert result["http"]["global"]["timeout_sec"] == 45
    assert result["sources"]["chembl"]["batch_size"] == 20


def test_invalid_config():
    """Test invalid configuration raises error."""
    with pytest.raises(Exception):
        load_config(Path("nonexistent.yaml"))


def test_activity_batch_size_enforced(tmp_path):
    """Ensure chembl batch size cannot exceed documented maximum."""

    config_file = tmp_path / "activity.yaml"
    config_file.write_text(
        """
version: 1
pipeline:
  name: activity
  entity: activity
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
    rate_limit:
      max_calls: 5
      period: 15.0
cache:
  enabled: true
  directory: "data/cache"
  ttl: 86400
  release_scoped: true
paths:
  input_root: "data/input"
  output_root: "data/output"
determinism:
  sort:
    by: []
    ascending: []
  column_order: []
sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    batch_size: 99
"""
    )

    with pytest.raises(ValueError, match="batch_size"):
        load_config(config_file)


def test_circular_extends(tmp_path):
    """Test circular extends detection."""
    base_file = tmp_path / "base.yaml"
    child_file = tmp_path / "child.yaml"

    base_file.write_text(
        """
extends: child.yaml
pipeline:
  name: base
"""
    )
    child_file.write_text(
        """
extends: base.yaml
pipeline:
  name: child
"""
    )

    with pytest.raises(ValueError, match="Circular extends"):
        load_config(child_file)

