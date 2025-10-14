from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from library.io.normalize import normalise_doi
from scripts.fetch_publications import app


runner = CliRunner()


def test_cli_pipeline(tmp_path, monkeypatch) -> None:
    input_df = pd.DataFrame(
        [
            {"document_chembl_id": "CHEMBL1", "doi": "10.1000/chembl1", "pmid": "111"},
            {"document_chembl_id": "CHEMBL2", "doi": "https://doi.org/10.1000/chembl2", "pmid": "222"},
        ]
    )
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    input_df.to_csv(input_path, index=False)

    chembl_payloads = {
        "CHEMBL1": {
            "documents": [
                {
                    "document_chembl_id": "CHEMBL1",
                    "doi": "10.1000/CHEMBL1",
                    "pubmed_id": "111",
                    "title": "ChEMBL one",
                }
            ]
        },
        "CHEMBL2": {
            "documents": [
                {
                    "document_chembl_id": "CHEMBL2",
                    "doi": "10.1000/chembl2",
                    "pubmed_id": "222",
                    "title": "ChEMBL two",
                }
            ]
        },
    }

    crossref_payloads = {
        "10.1000/chembl1": {"message": {"DOI": "10.1000/chembl1", "title": ["Crossref one"]}},
        "10.1000/chembl2": {"message": {"DOI": "10.1000/chembl2", "title": ["Crossref two"]}},
    }

    pubmed_payloads = {
        "111": {
            "result": {
                "uids": ["111"],
                "111": {"uid": "111", "title": "PubMed one", "elocationid": "10.1000/chembl1"},
            }
        },
        "222": {
            "result": {
                "uids": ["222"],
                "222": {"uid": "222", "title": "PubMed two", "elocationid": "10.1000/chembl2"},
            }
        },
    }

    from library.clients.chembl import ChEMBLClient
    from library.clients.crossref import CrossrefClient
    from library.clients.pubmed import PubMedClient

    monkeypatch.setattr(ChEMBLClient, "fetch_document", lambda self, doc_id: chembl_payloads[doc_id])
    monkeypatch.setattr(CrossrefClient, "fetch_by_doi", lambda self, doi: crossref_payloads[normalise_doi(doi)])
    monkeypatch.setattr(PubMedClient, "fetch_by_pmid", lambda self, pmid: pubmed_payloads[pmid])

    log_dir = tmp_path / "logs"
    result = runner.invoke(
        app,
        [str(input_path), str(output_path), "--run-id", "test", "--log-dir", str(log_dir)],
    )

    assert result.exit_code == 0, result.output
    df = pd.read_csv(output_path)
    assert list(df.columns) == [
        "document_chembl_id",
        "doi_key",
        "pmid",
        "chembl_title",
        "chembl_doi",
        "crossref_title",
        "pubmed_title",
    ]
    assert df.loc[0, "chembl_title"] == "ChEMBL one"
    assert df.loc[1, "crossref_title"] == "Crossref two"
    assert df.loc[0, "pubmed_title"] == "PubMed one"

    error_files = list(Path(log_dir).glob("*.error"))
    assert not error_files
