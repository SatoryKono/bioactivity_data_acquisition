import sys
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from bioetl.config.loader import load_config
from bioetl.pipelines.document import DocumentPipeline
from bioetl.schemas.document import DocumentNormalizedSchema


class _DummyTTLCache(dict):
    def __init__(self, maxsize, ttl):  # noqa: D401 - simple test stub
        super().__init__()
        self.maxsize = maxsize
        self.ttl = ttl


sys.modules.setdefault("cachetools", SimpleNamespace(TTLCache=_DummyTTLCache))

class _DummySchema:
    __fields__: dict[str, object] = {}


testitem_stub = ModuleType("bioetl.schemas.testitem")
setattr(testitem_stub, "TestItemSchema", _DummySchema)
sys.modules.setdefault("bioetl.schemas.testitem", testitem_stub)


@pytest.fixture
def document_config():
    """Load document pipeline configuration for tests."""
    return load_config("configs/pipelines/document.yaml")


@pytest.fixture
def document_pipeline(document_config, monkeypatch):
    """Instantiate a document pipeline with network calls stubbed out."""

    monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
    pipeline = DocumentPipeline(document_config, run_id="test-run")
    return pipeline


def _build_document_frame(**overrides) -> pd.DataFrame:
    """Create a minimal DataFrame matching DocumentSchema."""

    schema_columns = list(DocumentNormalizedSchema.to_schema().columns.keys())
    if not schema_columns:
        raise AssertionError("Document schema columns are not defined")

    row = {column: None for column in schema_columns}
    row.update(
        {
            "document_chembl_id": "CHEMBL1",
            "hash_business_key": "0" * 64,
            "hash_row": "1" * 64,
            "pipeline_version": "1.0.0",
            "source_system": "chembl",
            "chembl_release": "ChEMBL_TEST",
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "index": 0,
            "conflict_doi": False,
            "conflict_pmid": False,
            "title_source": "chembl",
            "journal": "",
            "authors": "",
            "title": "Test Title",
            "doi_clean": None,
            "pmid": 0,
        }
    )
    row.update(overrides)

    df = pd.DataFrame([row], columns=schema_columns)
    schema = DocumentNormalizedSchema.to_schema()

    for name, column in schema.columns.items():
        if df.at[0, name] is not None:
            continue

        dtype = str(column.dtype)
        if dtype.startswith("int"):
            df[name] = pd.Series([0], dtype="int64")
        elif dtype.startswith("bool"):
            df[name] = pd.Series([False], dtype="bool")

    return df.convert_dtypes()


def test_extract_raw_schema_violation(tmp_path, document_pipeline, monkeypatch):
    """Extraction should fail when raw schema constraints are violated."""

    csv_path = tmp_path / "documents.csv"
    csv_path.write_text("document_chembl_id\nCHEMBL1\n", encoding="utf-8")

    monkeypatch.setattr(document_pipeline, "_fetch_documents", lambda ids: [{"title": "No identifier"}])

    with pytest.raises(SchemaErrors):
        document_pipeline.extract(input_file=csv_path)


def test_validate_enforces_qc_thresholds(document_pipeline, monkeypatch):
    """QC threshold breaches with error severity should raise."""

    monkeypatch.setattr(
        DocumentNormalizedSchema,
        "validate",
        classmethod(lambda cls, df, lazy=True: df),
    )

    document_pipeline.config.qc.thresholds = {
        "doi_coverage": {"min": 0.9, "severity": "error"},
        "title_fallback_rate": {"max": 0.0, "severity": "error"},
    }

    df = _build_document_frame(title_source="fallback", conflict_doi=True)

    with pytest.raises(ValueError) as exc:
        document_pipeline.validate(df)

    message = str(exc.value)
    assert "doi_coverage" in message
    assert "title_fallback_rate" in message
    assert document_pipeline.qc_metrics["doi_coverage"] == pytest.approx(0.0)
    assert document_pipeline.qc_metrics["title_fallback_rate"] == pytest.approx(1.0)


def test_qc_threshold_severity_policy(document_pipeline, monkeypatch):
    """Threshold breaches below severity threshold should not raise errors."""

    monkeypatch.setattr(
        DocumentNormalizedSchema,
        "validate",
        classmethod(lambda cls, df, lazy=True: df),
    )

    document_pipeline.config.qc.severity_threshold = "error"
    document_pipeline.config.qc.thresholds = {
        "title_fallback_rate": {"max": 0.0, "severity": "warning"}
    }

    df = _build_document_frame(title_source="fallback")

    validated = document_pipeline.validate(df)

    assert not validated.empty
    assert document_pipeline.qc_metrics["title_fallback_rate"] == pytest.approx(1.0)
    assert any(
        issue["metric"] == "title_fallback_rate"
        and issue["severity"] == "warning"
        and issue["passed"] is False
        for issue in document_pipeline.validation_issues
    )
