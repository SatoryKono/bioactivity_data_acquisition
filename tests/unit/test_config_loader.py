"""Tests for configuration loader."""

import os
from pathlib import Path
from textwrap import dedent, indent

import pytest
from pydantic import ValidationError

from bioetl.config import load_config, parse_cli_overrides


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


@pytest.mark.parametrize(
    ("profile", "expected_ttl", "expected_enabled", "expected_release_scoped"),
    [
        ("dev", 3600, True, False),
        ("prod", 86400, True, True),
        ("test", 0, False, False),
    ],
)
def test_profile_configs_smoke(profile, expected_ttl, expected_enabled, expected_release_scoped):
    """Smoke test for loading profile configs with cache include overrides."""

    config = load_config(Path(f"configs/profiles/{profile}.yaml"))

    assert config.pipeline.name == profile
    assert config.cache.enabled is expected_enabled
    assert config.cache.ttl == expected_ttl
    assert config.cache.release_scoped is expected_release_scoped


def test_multiple_extends_and_overrides(tmp_path):
    """Multiple extends blocks should merge with overrides correctly."""

    base_file = tmp_path / "base.yaml"
    base_file.write_text(
        dedent(
            """
            version: 1
            pipeline:
              name: base
              entity: base
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
            postprocess: {{}}
            qc:
              enabled: true
              severity_threshold: warning
            cli: {{}}
            """
        )
    )

    include_file = tmp_path / "chembl.yaml"
    include_file.write_text(
        dedent(
            """
            sources:
              chembl:
                enabled: true
                base_url: "https://www.ebi.ac.uk/chembl/api/data"
                batch_size: 20
                max_url_length: 2000
                headers:
                  Accept: "application/json"
                  User-Agent: "bioetl-chembl-default/1.0"
                http:
                  rate_limit_jitter: true
            """
        )
    )

    child_file = tmp_path / "child.yaml"
    child_file.write_text(
        dedent(
            """
            extends:
              - base.yaml
              - chembl.yaml

            pipeline:
              name: chembl-child
              entity: child

            sources:
              chembl:
                batch_size: 15
                headers:
                  User-Agent: "custom-agent/1.0"
            """
        )
    )

    config = load_config(child_file)

    chembl = config.sources["chembl"]
    assert chembl.base_url == "https://www.ebi.ac.uk/chembl/api/data"
    assert chembl.batch_size == 15
    assert chembl.max_url_length == 2000
    assert chembl.headers["Accept"] == "application/json"
    assert chembl.headers["User-Agent"] == "custom-agent/1.0"
    assert chembl.rate_limit_jitter is True


def test_source_rate_limit_jitter_override(tmp_path):
    """Nested ``http.rate_limit_jitter`` should be recognized for sources."""

    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        dedent(
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
            sources:
              chembl:
                base_url: "https://example.org"
                http:
                  rate_limit_jitter: false
            cache:
              enabled: true
              directory: data/cache
              ttl: 1
              release_scoped: false
            paths:
              input_root: data/input
              output_root: data/output
            determinism:
              sort:
                by: []
                ascending: []
              column_order: []
            postprocess: {{}}
            qc:
              enabled: true
              severity_threshold: warning
            cli: {{}}
            """
        )
    )

    config = load_config(config_file)

    chembl = config.sources["chembl"]
    assert chembl.rate_limit_jitter is False


def _write_minimal_config(tmp_path, sources_block: str) -> Path:
    """Helper to create a minimal config file with custom sources."""

    config_file = tmp_path / "config.yaml"
    config_template = dedent(
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
        sources:
        __SOURCES_BLOCK__
        cache:
          enabled: true
          directory: data/cache
          ttl: 1
          release_scoped: false
        paths:
          input_root: data/input
          output_root: data/output
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
    indented_sources_block = indent(sources_block.strip(), "  ")
    config_file.write_text(
        config_template.replace("__SOURCES_BLOCK__", indented_sources_block)
    )
    return config_file


def test_source_api_key_requires_env_variable(tmp_path, monkeypatch):
    """Environment references for API keys should fail fast when missing."""

    config_file = _write_minimal_config(
        tmp_path,
        dedent(
            """
              iuphar:
                base_url: "https://example.org"
                api_key: "${IUPHAR_API_KEY}"
            """
        ),
    )
    monkeypatch.delenv("IUPHAR_API_KEY", raising=False)

    with pytest.raises(ValidationError, match="IUPHAR_API_KEY"):
        load_config(config_file)


def test_source_header_requires_env_variable(tmp_path, monkeypatch):
    """Environment references in headers should fail fast when missing."""

    config_file = _write_minimal_config(
        tmp_path,
        dedent(
            """
              iuphar:
                base_url: "https://example.org"
                headers:
                  x-api-key: "env:IUPHAR_API_KEY"
            """
        ),
    )
    monkeypatch.delenv("IUPHAR_API_KEY", raising=False)

    with pytest.raises(ValidationError, match="IUPHAR_API_KEY"):
        load_config(config_file)


def test_pipeline_include_merges_determinism_defaults():
    """Pipeline configs should inherit determinism defaults from shared include."""

    config = load_config(Path("configs/pipelines/target.yaml"))

    determinism = config.determinism
    assert determinism.hash_algorithm == "sha256"
    assert determinism.float_precision == 6
    assert determinism.datetime_format == "iso8601"
    assert determinism.sort.by == ["target_chembl_id"]
    assert determinism.column_order[0] == "target_chembl_id"


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
postprocess: {{}}
qc:
  enabled: true
  severity_threshold: warning
cli: {{}}
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
postprocess: {{}}
qc:
  enabled: true
  severity_threshold: warning
cli: {{}}
"""
    )

    os.environ["BIOETL_HTTP__GLOBAL__TIMEOUT_SEC"] = "45.0"

    try:
        config = load_config(config_file)
        assert config.http["global"].timeout_sec == 45.0
    finally:
        del os.environ["BIOETL_HTTP__GLOBAL__TIMEOUT_SEC"]


def test_target_source_env_resolution(tmp_path, monkeypatch):
    """Environment placeholders in source secrets should be resolved."""

    config_file = tmp_path / "target.yaml"
    config_file.write_text(
        """
version: 1
pipeline:
  name: target
  entity: target
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
postprocess: {{}}
qc:
  enabled: true
  severity_threshold: warning
sources:
  chembl:
    enabled: true
    base_url: "https://api.example.com"
    api_key: "${TEST_TARGET_API_KEY}"
    headers:
      Authorization: "env:TEST_TARGET_AUTH"
""",
    )

    monkeypatch.setenv("TEST_TARGET_API_KEY", "secret-token")
    monkeypatch.setenv("TEST_TARGET_AUTH", "Bearer secret-token")

    config = load_config(config_file)
    chembl_source = config.sources["chembl"]

    assert chembl_source.api_key == "secret-token"
    assert chembl_source.headers["Authorization"] == "Bearer secret-token"


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
postprocess: {{}}
qc:
  enabled: true
  severity_threshold: warning
cli: {{}}
"""
    )

    config1 = load_config(config_file)
    config2 = load_config(config_file)
    assert config1.config_hash == config2.config_hash


def test_chembl_batch_size_limit():
    """Chembl batch size must not exceed documented maximum."""

    with pytest.raises(ValidationError):
        load_config(
            Path("configs/pipelines/assay.yaml"),
            overrides={"sources": {"chembl": {"batch_size": 30}}},
        )


def test_parse_cli_overrides():
    """Test parsing CLI overrides."""
    overrides = ["http.global.timeout_sec=45", "sources.chembl.batch_size=20"]
    result = parse_cli_overrides(overrides)
    assert result["http"]["global"]["timeout_sec"] == 45
    assert result["sources"]["chembl"]["batch_size"] == 20


def test_invalid_config():
    """Test invalid configuration raises error."""
    with pytest.raises(FileNotFoundError):
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

