"""Tests for document pipeline schema validation and QC policy enforcement."""
from pathlib import Path

import pandas as pd
import pandera.errors as pa_errors
import pytest

from bioetl.config.loader import load_config
from bioetl.config.models import QCThresholdConfig
from bioetl.pipelines.document import DocumentPipeline


@pytest.fixture
def document_config() -> Path:
    """Return path to document pipeline configuration."""
    return Path("configs/pipelines/document.yaml")


def disable_adapters(config) -> None:
    """Disable external adapters to avoid network calls during tests."""
    for source_name in ["pubmed", "crossref", "openalex", "semantic_scholar"]:
        if source_name in config.sources:
            config.sources[source_name].enabled = False


def test_input_schema_violation_raises(document_config, tmp_path, monkeypatch):
    """Invalid document identifiers should raise a schema error during extraction."""
    config = load_config(document_config)
    disable_adapters(config)
    monkeypatch.setattr(
        DocumentPipeline,
        "_get_chembl_release",
        lambda self: None,
    )
    pipeline = DocumentPipeline(config, run_id="test")

    bad_df = pd.DataFrame({"document_chembl_id": ["INVALID1", "XYZ"]})
    input_path = tmp_path / "documents.csv"
    bad_df.to_csv(input_path, index=False)

    with pytest.raises((pa_errors.SchemaError, pa_errors.SchemaErrors)):
        pipeline.extract(input_file=input_path)


def test_qc_threshold_violation_triggers_failure(document_config, tmp_path, monkeypatch):
    """QC threshold violations with error severity should fail the pipeline run."""
    config = load_config(document_config)
    disable_adapters(config)
    config.qc.thresholds["doi_coverage"] = QCThresholdConfig(min=1.0, severity="error")
    monkeypatch.setattr(
        DocumentPipeline,
        "_get_chembl_release",
        lambda self: None,
    )

    pipeline = DocumentPipeline(config, run_id="qc-test")

    df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "title": ["Title A", "Title B"],
            "abstract": ["Abstract A", "Abstract B"],
            "authors": ["Author A", "Author B"],
            "journal": ["Journal A", "Journal B"],
            "year": [2020, 2021],
            "doi": [pd.NA, pd.NA],
        }
    )
    input_path = tmp_path / "documents.csv"
    df.to_csv(input_path, index=False)

    output_path = tmp_path / "documents"

    with pytest.raises(RuntimeError):
        pipeline.run(output_path=output_path, input_file=input_path)
