from __future__ import annotations

from pathlib import Path

import pandas as pd
import responses
import yaml
from typer.testing import CliRunner

from library import cli


@responses.activate
def test_cli_pipeline_command(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    output_path = tmp_path / "bioactivities.csv"
    qc_path = tmp_path / "qc.csv"
    corr_path = tmp_path / "corr.csv"
    config_path.write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "name": "chembl",
                        "base_url": "https://example.com",
                        "activities_endpoint": "/activities",
                        "page_size": 2,
                    }
                ],
                "output": {
                    "output_path": str(output_path),
                    "qc_report_path": str(qc_path),
                    "correlation_path": str(corr_path),
                },
                "retries": {"max_tries": 2},
                "log_level": "INFO",
                "strict_validation": True,
            }
        ),
        encoding="utf-8",
    )

    responses.add(
        responses.GET,
        "https://example.com/activities",
        json={
            "activities": [
                {
                    "assay_id": 1,
                    "molecule_chembl_id": "CHEMBL1",
                    "standard_value": 1.0,
                    "standard_units": "nM",
                    "activity_comment": None,
                }
            ],
            "next_page": False,
        },
    )

    runner = CliRunner()
    result = runner.invoke(cli.app, ["pipeline", "--config", str(config_path)])
    assert result.exit_code == 0
    assert output_path.exists()
    frame = pd.read_csv(output_path)
    assert frame.iloc[0]["standard_units"] == "nM"
    assert qc_path.exists()
    assert corr_path.exists()
