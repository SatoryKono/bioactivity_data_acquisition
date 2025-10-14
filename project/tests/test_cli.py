"""Integration tests for the Typer CLI."""

from __future__ import annotations

import csv
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import responses
from library.io.normalize import PUBLICATION_COLUMNS
from scripts.fetch_publications import app
from typer.testing import CliRunner


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def sample_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
logging:
  level: INFO
etl:
  strict_validation: true
  batch_size: 10
  global_rps: 5
sources:
  chembl:
    base_url: https://chembl.test/
  pubmed:
    base_url: https://pubmed.test/
  semscholar:
    base_url: https://semscholar.test/
  crossref:
    base_url: https://crossref.test/
  openalex:
    base_url: https://openalex.test/
        """.strip(),
        encoding="utf-8",
    )
    return config_path


@pytest.fixture()
def sample_input(tmp_path: Path) -> Path:
    csv_path = tmp_path / "queries.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["document_chembl_id", "doi", "pmid"])
        writer.writerow(["CHEMBL25", "", ""])
    return csv_path


@pytest.fixture()
def mocked_endpoints() -> Iterable[responses._recorder.Recorder]:  # type: ignore[name-defined]
    with responses.RequestsMock() as rsps:
        rsps.add(
            "GET",
            "https://chembl.test/document/CHEMBL25.json",
            json={
                "document_chembl_id": "CHEMBL25",
                "title": "Study of GPCR",
                "doi": "10.1000/xyz",
                "pubmed_id": "12345",
                "journal": "Nature",
                "year": "2023",
            },
            status=200,
        )
        rsps.add(
            "GET",
            "https://pubmed.test/esummary.fcgi",
            match=[responses.matchers.query_param_matcher({"db": "pubmed", "id": "12345", "retmode": "json"})],
            json={
                "result": {
                    "uids": ["12345"],
                    "12345": {
                        "uid": "12345",
                        "title": "PubMed Study",
                        "fulljournalname": "Nature",
                        "pubdate": "2023-01-01",
                        "articleids": [
                            {"idtype": "doi", "value": "10.1000/xyz"},
                        ],
                    },
                }
            },
            status=200,
        )
        rsps.add(
            "POST",
            "https://semscholar.test/paper/batch",
            json=[
                {
                    "paperId": "PMID12345",
                    "externalIds": {"PMID": "12345", "DOI": "10.1000/xyz"},
                    "title": "Semantic Scholar Study",
                    "year": 2023,
                }
            ],
            status=200,
        )
        rsps.add(
            "GET",
            "https://crossref.test/works/10.1000%2Fxyz",
            json={
                "message": {
                    "title": ["Crossref Study"],
                    "issued": {"date-parts": [[2023, 1, 1]]},
                    "publisher": "Crossref Publisher",
                    "type": "journal-article",
                    "DOI": "10.1000/xyz",
                }
            },
            status=200,
        )
        rsps.add(
            "GET",
            "https://openalex.test/works/https://doi.org/10.1000%2Fxyz",
            json={
                "display_name": "OpenAlex Study",
                "publication_year": 2023,
                "ids": {
                    "doi": "https://doi.org/10.1000/xyz",
                    "pmid": "https://pubmed.ncbi.nlm.nih.gov/12345/",
                },
            },
            status=200,
        )
        yield rsps


def test_run_command_creates_output_file(
    runner: CliRunner,
    sample_config: Path,
    sample_input: Path,
    tmp_path: Path,
    mocked_endpoints: Any,
) -> None:
    output_path = tmp_path / "output.csv"
    result = runner.invoke(
        app,
        [
            "run",
            "--config",
            str(sample_config),
            "--input",
            str(sample_input),
            "--output",
            str(output_path),
        ],
    )
    assert result.exit_code == 0, result.stdout
    frame = pd.read_csv(output_path)
    assert list(frame.columns) == PUBLICATION_COLUMNS
    assert frame.loc[0, "chembl.document_chembl_id"] == "CHEMBL25"
    assert frame.loc[0, "doi_key"] == "10.1000/xyz"
    assert frame.loc[0, "pmid"] == "12345"


def test_extract_command_skips_writing_output(
    runner: CliRunner,
    sample_config: Path,
    sample_input: Path,
    mocked_endpoints: Any,
) -> None:
    result = runner.invoke(
        app,
        [
            "extract",
            "--config",
            str(sample_config),
            "--input",
            str(sample_input),
        ],
    )
    assert result.exit_code == 0, result.stdout
