import sys
from collections.abc import Sequence
from types import ModuleType

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from bioetl.config.loader import load_config
from bioetl.pipelines.document import DocumentPipeline
from bioetl.schemas.document import DocumentNormalizedSchema, DocumentRawSchema, DocumentSchema


class _DummySchema:
    __fields__: dict[str, object] = {}


testitem_stub = ModuleType("bioetl.schemas.testitem")
testitem_stub.TestItemSchema = _DummySchema
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

    row = dict.fromkeys(schema_columns)
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


def test_validate_handles_nullable_integer_columns(document_pipeline):
    """Validation should succeed when optional integer columns contain ``pd.NA``."""

    df = _build_document_frame()

    for column in DocumentPipeline._INTEGER_COLUMNS:
        if column in df.columns:
            df[column] = pd.Series([pd.NA], dtype="Int64")

    document_pipeline.config.qc.severity_threshold = "error"
    document_pipeline.config.qc.thresholds = {}

    validated = document_pipeline.validate(df)

    for column in DocumentPipeline._INTEGER_COLUMNS:
        if column in validated.columns:
            assert str(validated[column].dtype) == "Int64"
            assert validated[column].isna().all()


def test_extract_raw_schema_violation(tmp_path, document_pipeline, monkeypatch):
    """Extraction should fail when raw schema constraints are violated."""

    csv_path = tmp_path / "documents.csv"
    csv_path.write_text("document_chembl_id\nCHEMBL1\n", encoding="utf-8")

    monkeypatch.setattr(document_pipeline, "_fetch_documents", lambda ids: [{"title": "No identifier"}])

    with pytest.raises(SchemaErrors):
        document_pipeline.extract(input_file=csv_path)


def test_raw_validation_reorders_columns(document_pipeline):
    """Raw validation should reorder columns to satisfy Pandera when API payloads shuffle fields."""

    unsorted_columns = [
        "abstract",
        "authors",
        "chembl_release",
        "contact",
        "doc_type",
        "document_chembl_id",
        "doi",
        "doi_chembl",
        "first_page",
        "issue",
        "journal",
        "journal_full_title",
        "last_page",
        "patent_id",
        "pubmed_id",
        "src_id",
        "title",
        "volume",
        "year",
        "classification",
        "document_contains_external_links",
        "is_experimental_doc",
        "journal_abbrev",
        "month",
        "source",
    ]

    record = dict.fromkeys(unsorted_columns)
    record.update(
        {
            "document_chembl_id": "CHEMBL999",
            "abstract": "Example abstract",
            "source": "ChEMBL",
        }
    )

    raw_df = pd.DataFrame([record], columns=unsorted_columns)

    validated = document_pipeline._validate_raw_dataframe(raw_df)

    schema_columns = list(DocumentRawSchema.to_schema().columns.keys())
    assert list(validated.columns[: len(schema_columns)]) == schema_columns

    extras = [
        "chembl_release",
        "contact",
        "doc_type",
        "doi_chembl",
        "journal_full_title",
        "patent_id",
        "src_id",
    ]
    assert list(validated.columns[len(schema_columns) :]) == extras
    assert validated.loc[0, "document_chembl_id"] == "CHEMBL999"


def test_transform_empty_dataframe_includes_all_columns(document_pipeline):
    """Transform should return an empty frame with full schema when no rows are present."""

    empty_df = pd.DataFrame(columns=["document_chembl_id"])

    transformed = document_pipeline.transform(empty_df)

    expected_columns = DocumentSchema.get_column_order()

    assert transformed.empty
    assert list(transformed.columns) == expected_columns


def test_enrich_skips_when_no_external_adapters(document_pipeline, monkeypatch):
    """Enrichment should be skipped gracefully when no external adapters are configured."""

    chembl_df = _build_document_frame()
    document_pipeline.external_adapters.clear()
    monkeypatch.setattr(document_pipeline, "_prepare_enrichment_adapters", lambda: None)

    result = document_pipeline._enrich_with_external_sources(chembl_df)

    assert result.dataframe is chembl_df
    assert result.status == "skipped"
    assert result.errors == {}
    assert result.requested_count == 0


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


def test_enrichment_fallback_recovers_missing_ids(document_pipeline, monkeypatch):
    """Pipeline-level fallback should restore full coverage when available."""

    class PubmedAdapter:
        def __init__(self) -> None:
            self.process_calls: list[tuple[str, ...]] = []
            self.fallback_calls: list[tuple[str, ...]] = []

        def process(self, ids: Sequence[str]) -> pd.DataFrame:
            self.process_calls.append(tuple(ids))
            return pd.DataFrame({"pubmed_pmid": [int(ids[0])]})

        def process_with_fallback(self, ids: Sequence[str]) -> pd.DataFrame:
            self.fallback_calls.append(tuple(ids))
            return pd.DataFrame({"pubmed_pmid": [int(ids[0])]})

    adapter = PubmedAdapter()
    pipeline = document_pipeline
    monkeypatch.setattr(pipeline, "_prepare_enrichment_adapters", lambda: None)
    pipeline.external_adapters = {"pubmed": adapter}

    chembl_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "chembl_pmid": ["1", "2"],
        }
    )

    result = pipeline._enrich_with_external_sources(chembl_df)

    assert adapter.fallback_calls == [("2",)]
    assert result.errors == {}
    assert result.requested_count == 2
    assert result.matched_count == 2
    assert result.missing_ids == {}
    pubmed_metrics = pipeline.qc_enrichment_metrics.loc[
        pipeline.qc_enrichment_metrics["source"] == "pubmed"
    ].iloc[0]
    assert pubmed_metrics["coverage"] == pytest.approx(1.0)
    assert bool(pubmed_metrics["fallback_attempted"]) is True
    assert pipeline.enrichment_missing_sources == []


def test_enrichment_fallback_reports_missing_when_unresolved(document_pipeline, monkeypatch):
    """Missing identifiers should be surfaced as adapter errors when fallback fails."""

    class PubmedAdapter:
        def __init__(self) -> None:
            self.process_calls: list[tuple[str, ...]] = []
            self.fallback_calls: list[tuple[str, ...]] = []

        def process(self, ids: Sequence[str]) -> pd.DataFrame:
            self.process_calls.append(tuple(ids))
            return pd.DataFrame({"pubmed_pmid": [int(ids[0])]})

        def process_with_fallback(self, ids: Sequence[str]) -> pd.DataFrame:
            self.fallback_calls.append(tuple(ids))
            return pd.DataFrame()

    adapter = PubmedAdapter()
    pipeline = document_pipeline
    monkeypatch.setattr(pipeline, "_prepare_enrichment_adapters", lambda: None)
    pipeline.external_adapters = {"pubmed": adapter}

    chembl_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "chembl_pmid": ["1", "2"],
        }
    )

    result = pipeline._enrich_with_external_sources(chembl_df)

    assert adapter.fallback_calls == [("2",)]
    assert "pubmed" in result.errors
    assert "missing_identifiers" in result.errors["pubmed"]
    assert result.missing_ids == {"pubmed": ["2"]}
    pubmed_metrics = pipeline.qc_enrichment_metrics.loc[
        pipeline.qc_enrichment_metrics["source"] == "pubmed"
    ].iloc[0]
    assert pubmed_metrics["coverage"] == pytest.approx(0.5)
    assert pubmed_metrics["missing_count"] == 1
    assert bool(pubmed_metrics["fallback_attempted"]) is True
    assert pipeline.enrichment_missing_sources == ["pubmed"]


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


def test_external_adapter_cache_timeout_overrides(document_config, monkeypatch):
    """Source-level cache and timeout overrides should update APIConfig."""

    source_cfg = document_config.sources["pubmed"]
    source_cfg.cache_enabled = False
    source_cfg.cache_ttl = 42
    source_cfg.cache_maxsize = 2048
    source_cfg.timeout_sec = 12.5

    monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")

    pipeline = DocumentPipeline(document_config, run_id="test-run")
    api_config = pipeline.external_adapters["pubmed"].api_config

    assert api_config.cache_enabled is False
    assert api_config.cache_ttl == 42
    assert api_config.cache_maxsize == 2048
    assert api_config.timeout_connect == pytest.approx(12.5)
    assert api_config.timeout_read == pytest.approx(12.5)
