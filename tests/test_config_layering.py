from __future__ import annotations

from pathlib import Path

import pytest

from library.config import Config


def write_config(tmp_path: Path) -> Path:
    output_dir = tmp_path / "artifacts"
    data_path = output_dir / "bio.csv"
    qc_path = output_dir / "qc.csv"
    corr_path = output_dir / "corr.csv"
    config_text = f"""
http:
  global:
    timeout: 12
    retries:
      max_tries: 2
      backoff_multiplier: 1.5
    headers:
      User-Agent: unit-test
sources:
  chembl:
    name: chembl
    endpoint: activity
    params:
      format: json
    pagination:
      page_param: page
      size: 50
    http:
      base_url: https://example.org/api
  crossref:
    name: crossref
    http:
      base_url: https://api.crossref.org/works
io:
  output:
    data_path: {data_path}
    qc_report_path: {qc_path}
    correlation_path: {corr_path}
logging:
  level: INFO
validation:
  strict: true
"""
    path = tmp_path / "config.yaml"
    path.write_text(config_text, encoding="utf-8")
    return path


def test_config_loads_and_applies_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = write_config(tmp_path)
    monkeypatch.setenv("BIOACTIVITY__HTTP__GLOBAL__TIMEOUT", "30")
    monkeypatch.setenv("BIOACTIVITY__SOURCES__CHEMBL__HTTP__HEADERS__authorization", "Token test")

    overrides = {"sources.chembl.pagination.size": "100"}
    config = Config.load(config_path, overrides=overrides)

    assert config.http.global_.timeout == 30
    chembl_source = config.sources["chembl"]
    client_cfg = chembl_source.to_client_config(config.http.global_)
    assert client_cfg.page_size == 100
    assert client_cfg.headers["authorization"] == "Token test"
    assert client_cfg.headers["User-Agent"] == "unit-test"


def test_cli_override_updates_timeout(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    config = Config.load(config_path, overrides={"sources.chembl.http.timeout": "60"})

    chembl_cfg = config.sources["chembl"].to_client_config(config.http.global_)
    assert chembl_cfg.timeout == 60
    assert chembl_cfg.retries.max_tries == 2
