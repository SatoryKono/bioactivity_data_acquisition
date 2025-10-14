from __future__ import annotations

from pathlib import Path

import pandas as pd
import responses

from library.pipeline import run_pipeline


@responses.activate
def test_run_pipeline_returns_transformed_frame(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    output_path = tmp_path / "bioactivities.csv"
    qc_path = tmp_path / "qc.csv"
    corr_path = tmp_path / "corr.csv"
    config_path.write_text(
        f"""
        sources:
          - name: chembl
            base_url: "https://example.com"
            activities_endpoint: "/activities"
            page_size: 2
        output:
          output_path: "{output_path}"
          qc_report_path: "{qc_path}"
          correlation_path: "{corr_path}"
        retries:
          max_tries: 2
        log_level: INFO
        strict_validation: true
        """,
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

    result = run_pipeline(config_path)
    assert isinstance(result, pd.DataFrame)
    assert result.iloc[0]["standard_units"] == "nM"
    assert output_path.exists()
