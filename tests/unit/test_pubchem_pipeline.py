"""Integration tests for the modular PubChem pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
from unittest.mock import MagicMock

import pandas as pd

from bioetl.config.loader import load_config
from bioetl.sources.pubchem.pipeline import PubChemPipeline


class StubPubChemClient:
    """Collects batch enrichment requests for verification."""

    def __init__(self, records: Iterable[dict[str, object]]):
        self.records = list(records)
        self.calls: list[list[str]] = []

    def enrich_batch(self, inchikeys: list[str]) -> list[dict[str, object]]:
        self.calls.append(inchikeys)
        return self.records


def _prepare_pipeline(monkeypatch, tmp_path: Path, client: StubPubChemClient | None) -> PubChemPipeline:
    config = load_config("configs/pipelines/pubchem.yaml").model_copy(deep=True)
    lookup_path = tmp_path / "lookup.csv"
    pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEMBL1"],
            "standard_inchi_key": ["ABCDEFGHIJKLMN-OPQRSTUVWX-Y"],
        }
    ).to_csv(lookup_path, index=False)
    config.postprocess.enrichment["pubchem_lookup_input"] = str(lookup_path)

    if client is not None:
        api_client = MagicMock()
        monkeypatch.setattr(
            "bioetl.sources.pubchem.pipeline.PubChemClient.from_config",
            lambda cfg: (client, api_client),
        )
    else:
        monkeypatch.setattr(
            "bioetl.sources.pubchem.pipeline.PubChemClient.from_config",
            lambda cfg: (None, None),
        )

    pipeline = PubChemPipeline(config, run_id="pubchem-test")
    pipeline.normalizer._timestamp_factory = lambda: "2024-01-01T00:00:00+00:00"
    return pipeline


def test_pubchem_pipeline_enriches_and_exports(monkeypatch, tmp_path):
    client = StubPubChemClient(
        [
            {
                "CID": 123,
                "MolecularWeight": 321.5,
                "_source_identifier": "ABCDEFGHIJKLMN-OPQRSTUVWX-Y",
                "_cid_source": "inchikey",
                "_enrichment_attempt": 1,
                "_fallback_used": False,
                "InChIKey": "ABCDEFGHIJKLMN-OPQRSTUVWX-Y",
            }
        ]
    )

    pipeline = _prepare_pipeline(monkeypatch, tmp_path, client)

    extracted = pipeline.extract()
    transformed = pipeline.transform(extracted)
    validated = pipeline.validate(transformed)
    output_path = tmp_path / "pubchem_output.csv"
    artifacts = pipeline.export(validated, output_path)

    assert int(transformed.loc[transformed.index[0], "pubchem_cid"]) == 123
    assert float(transformed.loc[transformed.index[0], "pubchem_molecular_weight"]) == 321.5
    assert output_path.exists()
    assert artifacts.dataset.exists()
    assert client.calls == [["ABCDEFGHIJKLMN-OPQRSTUVWX-Y"]]


def test_pubchem_pipeline_handles_missing_client(monkeypatch, tmp_path):
    pipeline = _prepare_pipeline(monkeypatch, tmp_path, client=None)

    extracted = pipeline.extract()
    transformed = pipeline.transform(extracted)

    assert "pubchem_cid" in transformed.columns
    assert transformed["pubchem_cid"].isna().all()
    metrics = pipeline.qc_summary_data.get("metrics", {})
    assert metrics.get("pubchem.enrichment_rate", {}).get("count") == 0
