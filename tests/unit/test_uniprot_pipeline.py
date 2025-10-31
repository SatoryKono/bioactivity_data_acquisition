from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.sources.uniprot import UniProtEnrichmentResult
from bioetl.sources.uniprot.pipeline import UniProtPipeline


@pytest.fixture()
def uniprot_config():
    return load_config("configs/pipelines/uniprot.yaml")


def test_transform_uses_service(monkeypatch, uniprot_config) -> None:
    pipeline = UniProtPipeline(uniprot_config, "test-run")

    monkeypatch.setattr(
        "bioetl.sources.uniprot.pipeline.finalize_output_dataset",
        lambda df, **_: df.copy(),
    )

    enrichment_result = UniProtEnrichmentResult(
        dataframe=pd.DataFrame({"uniprot_accession": ["P12345"]}),
        silver=pd.DataFrame({"canonical_accession": ["P12345"]}),
        components=pd.DataFrame(),
        metrics={"enrichment_success.uniprot": 1.0},
        missing_mappings=[
            {
                "stage": "uniprot",
                "target_id": None,
                "accession": "P12345",
                "resolution": "direct",
                "status": "resolved",
            }
        ],
        validation_issues=[],
    )

    service_mock = MagicMock()
    service_mock.enrich_targets.return_value = enrichment_result
    pipeline.normalizer = service_mock

    df = pd.DataFrame({"uniprot_accession": ["P12345"]})
    transformed = pipeline.transform(df)

    service_mock.enrich_targets.assert_called_once()
    assert "uniprot_accession" in transformed.columns
    assert not pipeline.qc_missing_mappings.empty
    assert pipeline.qc_metrics["enrichment_success.uniprot"] == pytest.approx(1.0)
    assert "uniprot_entries" in pipeline.additional_tables
