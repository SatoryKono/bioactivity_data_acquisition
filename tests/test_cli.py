"""Tests for the Typer-based CLI."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import responses
import yaml
from typer.testing import CliRunner

from library.cli import app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def sample_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    output_path = tmp_path / "bioactivities.csv"
    qc_path = tmp_path / "qc.csv"
    corr_path = tmp_path / "corr.csv"

    config_path.write_text(
        yaml.safe_dump(
            {
                "http": {
                    "global": {
                        "timeout": 5,
                        "retries": {"max_tries": 2, "backoff_multiplier": 1.0},
                        "headers": {"User-Agent": "pytest"},
                    }
                },
                "sources": {
                    "chembl": {
                        "name": "chembl",
                        "endpoint": "activities",
                        "pagination": {
                            "page_param": "page",
                            "size_param": "page_size",
                            "size": 50,
                            "max_pages": 1,
                        },
                        "http": {
                            "base_url": "https://example.com",
                            "headers": {"Accept": "application/json"},
                        },
                    }
                },
                "io": {
                    "output": {
                        "data_path": str(output_path),
                        "qc_report_path": str(qc_path),
                        "correlation_path": str(corr_path),
                        "format": "csv",
                        "csv": {"encoding": "utf-8", "float_format": "%.6f"},
                    }
                },
                "determinism": {
                    "sort": {
                        "by": ["compound_id", "target"],
                        "ascending": [True, True],
                        "na_position": "last",
                    },
                    "column_order": [
                        "compound_id",
                        "target",
                        "activity_value",
                        "activity_unit",
                        "source",
                        "retrieved_at",
                        "smiles",
                    ],
                },
                "transforms": {
                    "unit_conversion": {"nM": 1.0, "uM": 1000.0, "pM": 0.001}
                },
                "logging": {"level": "INFO"},
                "validation": {
                    "strict": True,
                    "qc": {
                        "max_missing_fraction": 1.0,
                        "max_duplicate_fraction": 1.0,
                    },
                },
                "postprocess": {"qc": {"enabled": True}, "correlation": {"enabled": True}},
            }
        ),
        encoding="utf-8",
    )
    return config_path


@responses.activate
def test_cli_pipeline_command(runner: CliRunner, sample_config: Path) -> None:
    """Test that the CLI pipeline command works correctly."""
    responses.add(
        responses.GET,
        "https://example.com/activities",
        json={
            "results": [
                {
                    "compound_id": "CHEMBL1",
                    "target_pref_name": "Protein X",
                    "activity_value": 1.0,
                    "activity_units": "uM",
                    "source": "chembl",
                    "retrieved_at": "2024-01-01T00:00:00Z",
                    "smiles": "C1=CC=CC=C1",
                }
            ]
        },
    )

    result = runner.invoke(app, ["pipeline", "--config", str(sample_config)])
    assert result.exit_code == 0
    
    # Check that output files were created
    output_path = sample_config.parent / "bioactivities.csv"
    qc_path = sample_config.parent / "qc.csv"
    corr_path = sample_config.parent / "corr.csv"
    
    assert output_path.exists()
    frame = pd.read_csv(output_path)
    assert list(frame.columns) == [
        "compound_id",
        "target",
        "activity_value",
        "activity_unit",
        "source",
        "retrieved_at",
        "smiles",
    ]
    assert frame.loc[0, "activity_unit"] == "nM"
    assert frame.loc[0, "activity_value"] == pytest.approx(1000.0, rel=1e-6)
    assert qc_path.exists()
    assert corr_path.exists()


def test_cli_invalid_override_format(runner: CliRunner, sample_config: Path) -> None:
    """Invalid override syntax should surface a helpful error."""

    result = runner.invoke(
        app, ["pipeline", "--config", str(sample_config), "--set", "not-a-valid-override"]
    )

    assert result.exit_code == 2
    assert "Overrides must be in KEY=VALUE format" in result.stderr


@pytest.fixture()
def health_config(tmp_path: Path) -> Path:
    """Create a test configuration file with two enabled sources for health checking."""
    config_path = tmp_path / "health_config.yaml"
    
    config_data = {
        "http": {
            "global": {
                "timeout_sec": 30.0,
                "retries": {
                    "total": 5,
                    "backoff_multiplier": 2.0
                },
                "headers": {
                    "User-Agent": "bioactivity-data-acquisition/0.1.0"
                }
            }
        },
        "sources": {
            "chembl": {
                "name": "chembl",
                "enabled": True,
                "endpoint": "document",
                "params": {
                    "document_type": "article"
                },
                "pagination": {
                    "page_param": "page",
                    "size_param": "page_size",
                    "size": 200,
                    "max_pages": 10
                },
                "http": {
                    "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                    "timeout_sec": 60.0,
                    "headers": {
                        "Accept": "application/json"
                    }
                }
            },
            "crossref": {
                "name": "crossref",
                "enabled": True,
                "params": {
                    "query": "chembl",
                    "select": "DOI,title"
                },
                "pagination": {
                    "page_param": "cursor",
                    "size_param": "rows",
                    "size": 100,
                    "max_pages": 5
                },
                "http": {
                    "base_url": "https://api.crossref.org/works",
                    "timeout_sec": 30.0,
                    "headers": {
                        "Accept": "application/json"
                    }
                }
            },
            "disabled_source": {
                "name": "disabled_source",
                "enabled": False,
                "http": {
                    "base_url": "https://example.com/api",
                    "timeout_sec": 30.0,
                    "headers": {
                        "Accept": "application/json"
                    }
                }
            }
        },
        "io": {
            "input": {},
            "output": {
                "data_path": str(tmp_path / "bioactivities.csv"),
                "qc_report_path": str(tmp_path / "qc.csv"),
                "correlation_path": str(tmp_path / "corr.csv")
            }
        },
        "logging": {
            "level": "INFO"
        }
    }
    
    with config_path.open("w", encoding="utf-8") as f:
        yaml.dump(config_data, f)
    
    return config_path


@responses.activate
def test_health_command_with_enabled_sources(runner: CliRunner, health_config: Path) -> None:
    """Test that the health command works with enabled sources."""
    # Mock responses for health checks - use the actual URLs that will be constructed
    responses.add(
        responses.HEAD,
        "https://www.ebi.ac.uk/chembl/api/data/health",
        status=200
    )
    responses.add(
        responses.HEAD,
        "https://api.crossref.org/works/health",
        status=200
    )
    
    result = runner.invoke(app, ["health", "--config", str(health_config)])
    
    # Should succeed and show health status for enabled sources
    assert result.exit_code == 0
    assert "API Health Status" in result.stdout
    assert "chembl" in result.stdout
    assert "crossref" in result.stdout
    # Disabled source should not appear
    assert "disabled_source" not in result.stdout


@responses.activate
def test_health_command_json_output(runner: CliRunner, health_config: Path) -> None:
    """Test that the health command works with JSON output."""
    # Mock responses for health checks
    responses.add(
        responses.HEAD,
        "https://www.ebi.ac.uk/chembl/api/data/health",
        status=200
    )
    responses.add(
        responses.HEAD,
        "https://api.crossref.org/works/health",
        status=200
    )
    
    result = runner.invoke(app, ["health", "--config", str(health_config), "--json"])
    
    # Should succeed and output JSON
    assert result.exit_code == 0
    import json
    # Extract JSON from output (skip the "Checking API health..." message)
    json_start = result.stdout.find('{')
    json_output = result.stdout[json_start:]
    output = json.loads(json_output)
    assert "total_apis" in output
    assert "healthy_apis" in output
    assert "apis" in output
    assert len(output["apis"]) == 2  # Only enabled sources


def test_health_command_no_enabled_sources(runner: CliRunner, tmp_path: Path) -> None:
    """Test that the health command fails when no sources are enabled."""
    config_path = tmp_path / "no_sources_config.yaml"
    
    config_data = {
        "http": {
            "global": {
                "timeout_sec": 30.0,
                "retries": {"total": 5},
                "headers": {"User-Agent": "test"}
            }
        },
            "sources": {
                "chembl": {
                    "name": "chembl",
                    "enabled": False,
                    "http": {
                        "base_url": "https://example.com/api",
                        "timeout_sec": 30.0,
                        "headers": {"Accept": "application/json"}
                    }
                }
            },
        "io": {
            "input": {},
            "output": {
                "data_path": str(tmp_path / "bioactivities.csv"),
                "qc_report_path": str(tmp_path / "qc.csv"),
                "correlation_path": str(tmp_path / "corr.csv")
            }
        },
        "logging": {"level": "INFO"}
    }
    
    with config_path.open("w", encoding="utf-8") as f:
        yaml.dump(config_data, f)
    
    result = runner.invoke(app, ["health", "--config", str(config_path)])
    
    # Should fail with appropriate error message
    assert result.exit_code == 1
    assert "No enabled API clients found for health checking" in result.stderr