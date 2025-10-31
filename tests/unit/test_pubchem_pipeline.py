from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from bioetl.config.loader import load_config
from bioetl.pipelines.pubchem import PubChemPipeline


def _build_pubchem_pipeline(tmp_path, monkeypatch, *, adapter: MagicMock | None):
    config = load_config("configs/pipelines/pubchem.yaml").model_copy(deep=True)
    lookup_path = tmp_path / "lookup.csv"
    pd.DataFrame(
        {
            "molecule_chembl_id": ["CHEMBL1"],
            "standard_inchi_key": ["ABCDEFGHIJKLMN-OPQRSTUVWX-Y"],
        }
    ).to_csv(lookup_path, index=False)
    config.postprocess.enrichment["pubchem_lookup_input"] = str(lookup_path)

    if adapter is not None:
        monkeypatch.setattr(PubChemPipeline, "_create_pubchem_adapter", lambda self: adapter)
    else:
        monkeypatch.setattr(PubChemPipeline, "_create_pubchem_adapter", lambda self: None)

    return PubChemPipeline(config, run_id="pubchem-test")


def test_pubchem_pipeline_enriches_and_exports(monkeypatch, tmp_path):
    mock_adapter = MagicMock()

    def _enrich(df, inchi_key_col="standard_inchi_key"):
        enriched = df.copy()
        enriched["pubchem_cid"] = [123]
        enriched["pubchem_molecular_weight"] = [321.5]
        enriched["pubchem_enrichment_attempt"] = [1]
        enriched["pubchem_lookup_inchikey"] = df[inchi_key_col]
        enriched["pubchem_enriched_at"] = ["2024-01-01T00:00:00+00:00"]
        enriched["pubchem_cid_source"] = ["inchikey"]
        enriched["pubchem_fallback_used"] = [False]
        return enriched

    mock_adapter.enrich_with_pubchem.side_effect = _enrich
    mock_adapter.api_client = MagicMock()

    pipeline = _build_pubchem_pipeline(tmp_path, monkeypatch, adapter=mock_adapter)

    extracted = pipeline.extract()
    assert not extracted.empty

    transformed = pipeline.transform(extracted)
    assert int(transformed.loc[transformed.index[0], "pubchem_cid"]) == 123

    validated = pipeline.validate(transformed)
    output_path = tmp_path / "pubchem_output.csv"
    artifacts = pipeline.export(validated, output_path)

    assert output_path.exists()
    assert mock_adapter.enrich_with_pubchem.called
    assert artifacts.dataset.exists()


def test_pubchem_pipeline_handles_missing_adapter(monkeypatch, tmp_path):
    pipeline = _build_pubchem_pipeline(tmp_path, monkeypatch, adapter=None)

    extracted = pipeline.extract()
    transformed = pipeline.transform(extracted)

    assert "pubchem_cid" in transformed.columns
    assert transformed["pubchem_cid"].isna().all()

    validated = pipeline.validate(transformed)
    metrics = pipeline.qc_summary_data.get("metrics", {})
    assert metrics.get("pubchem.enrichment_rate", {}).get("count") == 0

