"""Smoke tests for the publications CLI."""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest
import yaml
from scripts.fetch_publications import app
from typer.testing import CliRunner


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def stub_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Chembl:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def fetch_document(self, doc_id: str) -> dict[str, object]:
            return {
                "documents": [
                    {
                        "document_chembl_id": doc_id,
                        "doi": f"10.1000/{doc_id.lower()}",
                        "pubmed_id": "111",
                        "title": f"Chembl {doc_id}",
                    }
                ]
            }

    class _Crossref:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def fetch_by_doi(self, doi: str) -> dict[str, object]:
            return {
                "message": {
                    "items": [
                        {
                            "DOI": doi,
                            "title": [f"Crossref {doi}"],
                        }
                    ]
                }
            }

    class _Pubmed:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def fetch_by_pmid(self, pmid: str) -> dict[str, object]:
            return {
                "result": {
                    pmid: {
                        "pmid": pmid,
                        "title": f"PubMed {pmid}",
                    }
                }
            }

    monkeypatch.setattr("scripts.fetch_publications.ChEMBLClient", _Chembl)
    monkeypatch.setattr("scripts.fetch_publications.CrossrefClient", _Crossref)
    monkeypatch.setattr("scripts.fetch_publications.PubMedClient", _Pubmed)


@pytest.fixture()
def sample_config(tmp_path: Path) -> Path:
    output_dir = tmp_path / "artifacts"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "logging": {"level": "INFO"},
                "determinism": {
                    "sort": {
                        "by": ["document_chembl_id", "doi_key", "pmid"],
                        "ascending": [True, True, True],
                        "na_position": "last",
                    },
                    "column_order": [
                        "document_chembl_id",
                        "doi_key",
                        "pmid",
                        "chembl_title",
                        "chembl_doi",
                        "crossref_title",
                        "pubmed_title",
                    ],
                },
                "io": {
                    "output": {
                        "data_path": str(output_dir / "publications.csv"),
                        "qc_report_path": str(output_dir / "qc.csv"),
                        "correlation_path": str(output_dir / "corr.csv"),
                        "format": "csv",
                        "csv": {"encoding": "utf-8", "float_format": "%.2f"},
                    }
                },
                "validation": {
                    "strict": True,
                    "qc": {"max_missing_fraction": 1.0, "max_duplicate_fraction": 1.0},
                },
                "postprocess": {"qc": {"enabled": True}, "correlation": {"enabled": True}},
            }
        ),
        encoding="utf-8",
    )
    return config_path


@pytest.fixture()
def sample_input(tmp_path: Path) -> Path:
    csv_path = tmp_path / "queries.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["document_chembl_id"])
        writer.writerow(["CHEMBL25"])
    return csv_path


def test_run_command_creates_configured_outputs(
    runner: CliRunner,
    sample_config: Path,
    sample_input: Path,
) -> None:
    result = runner.invoke(
        app,
        [
            "run",
            "--config",
            str(sample_config),
            "--input",
            str(sample_input),
        ],
    )
    assert result.exit_code == 0, result.stdout

    config_payload = yaml.safe_load(sample_config.read_text(encoding="utf-8"))
    output_path = Path(config_payload["io"]["output"]["data_path"]).resolve()
    qc_path = Path(config_payload["io"]["output"]["qc_report_path"]).resolve()
    corr_path = Path(config_payload["io"]["output"]["correlation_path"]).resolve()

    assert output_path.exists()
    frame = pd.read_csv(output_path)
    assert list(frame.columns) == [
        "document_chembl_id",
        "doi_key",
        "pmid",
        "chembl_title",
        "chembl_doi",
        "crossref_title",
        "pubmed_title",
    ]

    assert qc_path.exists()
    qc_frame = pd.read_csv(qc_path)
    assert "qc_passed" in qc_frame["metric"].values

    assert corr_path.exists()
    assert corr_path.read_text(encoding="utf-8").strip() == ""
