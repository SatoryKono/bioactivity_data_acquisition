"""End-to-end checks for the PubChem enrichment pipeline."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.config.paths import get_config_path
from bioetl.sources.pubchem.pipeline import PubChemClient, PubChemPipeline


@pytest.fixture
def pubchem_config():
    """Load the canonical PubChem pipeline configuration."""

    return load_config(get_config_path("pipelines/pubchem.yaml"))


@pytest.fixture
def patch_pubchem_client(monkeypatch: pytest.MonkeyPatch):
    """Helper to replace ``PubChemClient.from_config`` within a test."""

    def _apply(client, api_client) -> None:
        monkeypatch.setattr(PubChemClient, "from_config", lambda config: (client, api_client))

    return _apply


def test_extract_reads_lookup_table(
    tmp_path: Path, pubchem_config, patch_pubchem_client
) -> None:
    """The extract stage reads the lookup table and preserves identifiers."""

    patch_pubchem_client(None, None)
    pipeline = PubChemPipeline(pubchem_config, "pytest-run")

    lookup = pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "standard_inchi_key": ["XCGT", "NNNN"],
        }
    )
    input_path = tmp_path / "lookup.csv"
    lookup.to_csv(input_path, index=False)

    extracted = pipeline.extract(input_file=input_path)

    assert list(extracted.columns) == ["molecule_chembl_id", "standard_inchi_key"]
    assert extracted.loc[0, "molecule_chembl_id"] == "CHEMBL1"
    assert extracted.loc[1, "standard_inchi_key"] == "NNNN"


def test_transform_enriches_with_stub_client(pubchem_config, patch_pubchem_client) -> None:
    """Transform stage enriches and records QC metrics when the client responds."""

    class StubClient:
        def __init__(self, records: list[dict[str, object]]):
            self.records = records
            self.calls: list[list[str]] = []

        def enrich_batch(self, inchikeys: list[str]) -> list[dict[str, object]]:
            self.calls.append(list(inchikeys))
            return self.records

    stub_records = [
        {
            "CID": 12345,
            "MolecularFormula": "H2O",
            "MolecularWeight": 18.0,
            "SMILES": "O",
            "InChIKey": "XCGT",
            "_source_identifier": "XCGT",
            "_cid_source": "inchikey",
            "_enrichment_attempt": 1,
            "_fallback_used": False,
        }
    ]
    stub_client = StubClient(stub_records)

    patch_pubchem_client(stub_client, SimpleNamespace(close=lambda: None))

    pipeline = PubChemPipeline(pubchem_config, "pytest-run")

    df = pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEMBL1"],
            "standard_inchi_key": ["Xcgt"],
        }
    )

    transformed = pipeline.transform(df)

    assert stub_client.calls == [["XCGT"]]
    assert int(transformed.loc[0, "pubchem_cid"]) == 12345
    assert transformed.loc[0, "pubchem_lookup_inchikey"] == "XCGT"

    metrics = pipeline.qc_summary_data.get("metrics", {})
    assert metrics["pubchem.enrichment_rate"]["count"] == 1
    assert metrics["pubchem.inchikey_coverage"]["passed"] is True

    validated = pipeline.validate(transformed)
    assert not validated.empty

    assert pipeline.export_metadata is not None
    assert "pubchem_lookup_inchikey" in pipeline.export_metadata.column_order
