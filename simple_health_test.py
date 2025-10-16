#!/usr/bin/env python3
"""Simple health test without mocks."""

import tempfile
import yaml
from pathlib import Path
from typer.testing import CliRunner

from library.cli import app

def test_health_simple():
    """Simple health test."""
    runner = CliRunner()
    
    # Create a simple config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
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
                    "enabled": True,
                    "http": {
                        "base_url": "https://httpbin.org",
                        "timeout_sec": 30.0,
                        "headers": {"Accept": "application/json"}
                    }
                }
            },
            "io": {
                "input": {},
                "output": {
                    "data_path": "/tmp/bioactivities.csv",
                    "qc_report_path": "/tmp/qc.csv",
                    "correlation_path": "/tmp/corr.csv"
                }
            },
            "logging": {"level": "INFO"}
        }
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        result = runner.invoke(app, ["health", "--config", config_path])
        print(f"Exit code: {result.exit_code}")
        if result.stdout:
            print(f"Output: {result.stdout.encode('utf-8', errors='replace').decode('utf-8')}")
        if result.stderr:
            print(f"Error: {result.stderr.encode('utf-8', errors='replace').decode('utf-8')}")
    finally:
        Path(config_path).unlink()

if __name__ == "__main__":
    test_health_simple()
