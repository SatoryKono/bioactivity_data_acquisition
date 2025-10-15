from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from scripts.fetch_publications import app
from library.clients.chembl import ChemblClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semscholar import SemanticScholarClient


runner = CliRunner()


def test_cli_end_to_end(tmp_path, monkeypatch):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"

    pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL123"],
            "doi": ["10.1000/abc"],
            "pmid": ["12345"],
        }
    ).to_csv(input_path, index=False)

    monkeypatch.setattr(ChemblClient, "fetch_by_doc_id", lambda self, doc_id: {"document_chembl_id": doc_id, "doi": "10.1000/abc", "pmid": "12345"})
    monkeypatch.setattr(PubMedClient, "fetch_batch", lambda self, pmids: {"12345": {"pmid": "12345", "doi": "10.1000/abc", "title": "PubMed title"}})
    monkeypatch.setattr(SemanticScholarClient, "fetch_batch", lambda self, pmids: {"12345": {"pmid": "12345", "doi": "10.1000/abc", "title": "S2 title"}})
    monkeypatch.setattr(CrossrefClient, "fetch_by_doi", lambda self, doi: {"doi": doi, "title": "Crossref title"})
    monkeypatch.setattr(CrossrefClient, "fetch_by_pmid", lambda self, pmid: {})
    monkeypatch.setattr(OpenAlexClient, "fetch_by_doi", lambda self, doi: {"doi": doi, "title": "OpenAlex title"})
    monkeypatch.setattr(OpenAlexClient, "fetch_by_pmid", lambda self, pmid: {})

    result = runner.invoke(app, ["--input", str(input_path), "--output", str(output_path), "--error-dir", str(tmp_path / "errors")])
    assert result.exit_code == 0, result.stdout

    output_df = pd.read_csv(output_path)
    assert output_df.loc[0, "chembl_document_chembl_id"] == "CHEMBL123"
    assert output_df.loc[0, "doi_key"] == "10.1000/abc"
    assert "pubmed_title" in output_df.columns
