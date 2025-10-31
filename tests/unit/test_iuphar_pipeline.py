from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.pipelines.iuphar import GtpIupharPipeline


class DummyIupharClient:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, Any] | None]] = []

    def request_json(self, path: str, params: dict[str, Any] | None = None):  # noqa: D401 - test stub
        self.requests.append((path, params))
        if path == "/targets":
            return [
                {
                    "targetId": 1,
                    "name": "Test Target",
                    "abbreviation": "TT",
                    "synonyms": "Alpha|Beta",
                    "annotationStatus": "CURATED",
                    "geneSymbol": "TTG",
                    "familyIds": [11],
                },
            ]
        if path == "/targets/families":
            return [
                {"familyId": 1, "name": "GPCRs", "parentFamilyIds": []},
                {"familyId": 10, "name": "Class A GPCRs", "parentFamilyIds": [1]},
                {"familyId": 11, "name": "Adenosine receptors", "parentFamilyIds": [10]},
            ]
        raise AssertionError(f"Unexpected path {path}")


@pytest.fixture
def iuphar_config() -> Any:
    return load_config("configs/pipelines/iuphar.yaml")


def test_iuphar_pipeline_transform_produces_outputs(monkeypatch: pytest.MonkeyPatch, iuphar_config):
    run_id = "unit-test"
    pipeline = GtpIupharPipeline(iuphar_config, run_id)

    dummy_client = DummyIupharClient()
    pipeline.iuphar_client = dummy_client
    pipeline.iuphar_service.client = dummy_client

    outputs: dict[str, pd.DataFrame] = {}

    def _capture_materialization(
        classification_df: pd.DataFrame,
        gold_df: pd.DataFrame,
        *,
        format: str = "parquet",
        output_directory: Path | None = None,
    ) -> dict[str, Path]:
        outputs["classification"] = classification_df
        outputs["gold"] = gold_df
        return {}

    monkeypatch.setattr(
        pipeline.materialization_manager,
        "materialize_iuphar",
        lambda classification_df, gold_df, *, format="parquet", output_directory=None: _capture_materialization(
            classification_df,
            gold_df,
            format=format,
            output_directory=output_directory,
        ),
    )

    extracted = pipeline.extract()
    assert not extracted.empty
    assert "iuphar_target_id" in extracted.columns

    transformed = pipeline.transform(extracted)

    assert "classification_path" in transformed.columns
    assert transformed.loc[0, "iuphar_class"] == "Class A GPCRs"
    assert pipeline.qc_metrics["enrichment_success.iuphar"] == pytest.approx(1.0)
    assert "classification" in outputs and not outputs["classification"].empty
    assert "gold" in outputs and not outputs["gold"].empty
    assert "gtp_iuphar_classification" in pipeline.additional_tables

    validated = pipeline.validate(transformed)
    assert not validated.empty
    assert validated.loc[0, "iuphar_target_id"] == 1
