"""Shared pytest fixtures and configuration."""

import os
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

try:
    from faker import Faker
except ModuleNotFoundError:  # pragma: no cover - import guard
    import random

    class Faker:  # type: ignore[override]
        """Minimal Faker fallback used when the dependency is absent."""

        @classmethod
        def seed(cls, value: int) -> None:
            random.seed(value)

        def name(self) -> str:
            return f"Test User {random.randint(0, 9999)}"

# Add src to path so imports work
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Configure Faker for consistent test data
fake = Faker()
Faker.seed(42)  # For reproducible test data


class _DummyTTLCache(dict):
    """Minimal TTLCache stub used when ``cachetools`` is unavailable."""

    def __init__(self, maxsize, ttl):  # noqa: D401 - simple stub
        super().__init__()
        self.maxsize = maxsize
        self.ttl = ttl


def _register_cachetools_stub() -> None:
    """Ensure a ``cachetools`` stub with ``TTLCache`` is available for tests."""

    module = sys.modules.get("cachetools")
    if module is None:
        stub = ModuleType("cachetools")
        stub.TTLCache = _DummyTTLCache
        sys.modules["cachetools"] = stub
        return

    if not hasattr(module, "TTLCache"):
        module.TTLCache = _DummyTTLCache


_register_cachetools_stub()


def pytest_addoption(parser: pytest.Parser) -> None:  # pragma: no cover - test helper
    """Provide no-op coverage options when pytest-cov is unavailable."""

    for args in (
        ("--cov", {"action": "append", "default": []}),
        ("--cov-report", {"action": "append", "default": []}),
        ("--cov-fail-under", {"action": "store", "default": None}),
    ):
        name, kwargs = args
        try:
            parser.addoption(name, **kwargs)
        except ValueError:
            # Option already provided by an installed plugin.
            continue


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_chembl_data() -> pd.DataFrame:
    """Sample ChEMBL data for testing."""
    return pd.DataFrame(
        {
            "assay_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "assay_type": ["B", "F", "B"],
            "assay_category": ["SINGLE", "SINGLE", "SINGLE"],
            "assay_cell_type": [None, "Hepatocyte", None],
            "assay_organism": ["Homo sapiens", "Mus musculus", "Homo sapiens"],
            "assay_tax_id": [9606, 10090, 9606],
            "assay_test_type": ["ADME", "BINDING", "BINDING"],
            "assay_tissue": ["Brain", "Liver", "Heart"],
            "assay_parameters_json": ["[]", "[]", "[]"],
            "assay_strain": [None, "C57BL/6", None],
            "assay_subcellular_fraction": [None, "Membrane", None],
            "confidence_score": [9, 8, 7],
            "document_chembl_id": ["CHEMBL_DOC1", "CHEMBL_DOC2", "CHEMBL_DOC3"],
            "src_id": [1, 2, 3],
            "src_assay_id": ["SRC1", "SRC2", "SRC3"],
            "target_chembl_id": ["CHEMBL_TGT1", "CHEMBL_TGT2", "CHEMBL_TGT3"],
            "pref_name": ["Target A", "Target B", "Target C"],
            "organism": ["Homo sapiens", "Mus musculus", "Homo sapiens"],
            "target_type": ["SINGLE PROTEIN", "SINGLE PROTEIN", "SINGLE PROTEIN"],
            "species_group_flag": [0, 0, 0],
            "tax_id": [9606, 10090, 9606],
            "component_count": [1, 1, 1],
            "assay_param_type": ["IC50", "EC50", "IC50"],
            "assay_param_relation": ["=", "=", "="],
            "assay_param_value": [100.0, 200.0, 150.0],
            "assay_param_units": ["nM", "nM", "nM"],
            "assay_param_text_value": [None, None, None],
            "assay_param_standard_type": ["IC50", "EC50", "IC50"],
            "assay_param_standard_value": [100.0, 200.0, 150.0],
            "assay_param_standard_units": ["nM", "nM", "nM"],
            "assay_class_id": [101, 102, 103],
            "assay_class_bao_id": ["BAO_0000001", "BAO_0000002", "BAO_0000003"],
            "assay_class_type": ["primary", "secondary", "primary"],
            "assay_class_l1": ["L1", "L1", "L1"],
            "assay_class_l2": ["L2", "L2", "L2"],
            "assay_class_l3": ["L3", "L3", "L3"],
            "assay_class_description": [
                "Example class A",
                "Example class B",
                "Example class C",
            ],
            "variant_id": [1, 2, 3],
            "variant_base_accession": ["P12345", "P23456", "P34567"],
            "variant_mutation": ["A10V", "T25M", "G100D"],
            "variant_sequence": ["MSSSS", "MSTTT", "MSGGG"],
            "variant_accession_reported": ["Q11111", "Q22222", "Q33333"],
        }
    )


@pytest.fixture
def frozen_time(monkeypatch: pytest.MonkeyPatch):
    """Freeze ``datetime.now`` for deterministic metadata generation."""

    from datetime import datetime, timezone

    frozen = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None):  # type: ignore[override]
            if tz is None:
                return frozen.replace(tzinfo=None)
            return frozen.astimezone(tz)

        @classmethod
        def utcnow(cls):  # type: ignore[override]
            return frozen.astimezone(timezone.utc).replace(tzinfo=None)

    monkeypatch.setattr("bioetl.core.output_writer.datetime", _FrozenDateTime)
    monkeypatch.setattr("bioetl.pipelines.base.datetime", _FrozenDateTime)
    return frozen


@pytest.fixture
def sample_activity_data() -> pd.DataFrame:
    """Sample activity data for testing."""
    return pd.DataFrame(
        {
            "activity_id": [1, 2, 3],
            "assay_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "molecule_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL102"],
            "target_chembl_id": ["CHEMBL_TGT1", "CHEMBL_TGT2", "CHEMBL_TGT3"],
            "document_chembl_id": ["CHEMBL_DOC1", "CHEMBL_DOC2", "CHEMBL_DOC3"],
            "standard_type": ["IC50", "EC50", "IC50"],
            "standard_relation": ["=", "=", "="],
            "standard_value": [100.0, 200.0, 150.0],
            "standard_units": ["nM", "nM", "nM"],
            "standard_flag": [0, 0, 0],
            "pchembl_value": [6.0, 5.7, 5.8],
            "lower_bound": [90.0, 180.0, 140.0],
            "upper_bound": [110.0, 220.0, 160.0],
            "is_censored": [False, False, False],
            "activity_comment": [
                "Test comment 1",
                "Test comment 2",
                "Test comment 3",
            ],
            "data_validity_comment": [None, "Potential issue", None],
            "bao_endpoint": ["BAO_0000004", "BAO_0000004", "BAO_0000004"],
            "bao_format": ["BAO_0000218", "BAO_0000208", "BAO_0000218"],
            "bao_label": ["response", "inhibition", "response"],
            "potential_duplicate": [0, 0, 1],
            "uo_units": ["UO_0000065", "UO_0000065", "UO_0000065"],
            "qudt_units": ["QUDT_0000065", "QUDT_0000065", "QUDT_0000065"],
            "src_id": [1, 1, 1],
            "canonical_smiles": [
                "C1=CC=CC=C1",
                "C1=CC(=CC=C1)O",
                "C1=CC(=O)NC=C1",
            ],
            "target_organism": [
                "Homo sapiens",
                "Mus musculus",
                "Rattus norvegicus",
            ],
            "target_tax_id": [9606, 10090, 10116],
            "compound_key": ["CHEMBL100|CHEMBL1", "CHEMBL101|CHEMBL2", "CHEMBL102|CHEMBL3"],
            "is_citation": [True, False, False],
            "high_citation_rate": [False, False, True],
        }
    )


@pytest.fixture
def sample_document_data() -> pd.DataFrame:
    """Sample document data for testing."""
    return pd.DataFrame(
        {
            "document_chembl_id": [
                "CHEMBL1137491",
                "CHEMBL1155082",
                "CHEMBL4000255",
            ],
            "pmid": [17827018, 18578478, 28337320],
            "pmid_source": ["chembl", "pubmed", "chembl"],
            "doi_clean": [
                "10.1016/j.bmc.2007.08.038",
                "10.1021/jm800092x",
                "10.1021/acsmedchemlett.6b00465",
            ],
            "doi_clean_source": ["crossref", "openalex", "chembl"],
            "title": [
                "Click chemistry based solid phase supported synthesis",
                "Chemo-enzymatic synthesis of glutamate analogues",
                "Discovery of ABBV-075 (mivebresib)",
            ],
            "title_source": ["chembl", "pubmed", "semantic_scholar"],
            "abstract": ["Abstract 1", "Abstract 2", "Abstract 3"],
            "abstract_source": ["chembl", "pubmed", "pubmed"],
            "journal": [
                "J. Med. Chem.",
                "J. Med. Chem.",
                "ACS Med. Chem. Lett.",
            ],
            "journal_source": ["chembl", "chembl", "crossref"],
            "year": [2008, 2008, 2017],
            "year_source": ["chembl", "chembl", "chembl"],
            "authors": ["Author 1", "Author 2", "Author 3"],
            "authors_source": ["chembl", "chembl", "chembl"],
            "is_oa": [True, False, True],
            "is_oa_source": ["openalex", "openalex", "semantic_scholar"],
            "oa_status": ["gold", "closed", "bronze"],
            "oa_status_source": ["openalex", "openalex", "openalex"],
            "mesh_terms": [
                "term1;term2",
                "term3;term4",
                "term5;term6",
            ],
            "mesh_terms_source": ["pubmed", "pubmed", "pubmed"],
            "chembl_doi": [
                "10.1016/j.bmc.2007.08.038",
                None,
                "10.1021/acsmedchemlett.6b00465",
            ],
            "pubmed_article_title": [
                "Click chemistry",
                "Chemo-enzymatic synthesis",
                "Discovery of ABBV-075",
            ],
            "pubmed_authors": ["Author 1", "Author 2", "Author 3"],
            "pubmed_journal": [
                "J. Med. Chem.",
                "J. Med. Chem.",
                "ACS Med. Chem. Lett.",
            ],
            "crossref_error": [None, None, None],
            "openalex_error": [None, None, None],
            "pubmed_error": [None, None, None],
            "semantic_scholar_error": [None, None, None],
        }
    )


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    from bioetl.config import Config

    return Config(
        version=1,
        pipeline={"name": "test", "entity": "test"},
        http={
            "global": {
                "timeout_sec": 60.0,
                "retries": {"total": 3, "backoff_multiplier": 2.0, "backoff_max": 120.0},
                "rate_limit": {"max_calls": 5, "period": 15.0},
            }
        },
        cache={"enabled": True, "directory": "data/cache", "ttl": 86400, "release_scoped": True},
        paths={"input_root": "data/input", "output_root": "data/output"},
        determinism={"sort": {"by": [], "ascending": []}, "column_order": []},
        postprocess={},
        qc={"enabled": True, "severity_threshold": "warning"},
        cli={},
    )


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables after each test."""
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_requests(monkeypatch):
    """Mock requests library for API testing."""
    from unittest.mock import Mock

    import requests

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": []}
    mock_response.text = '{"data": []}'

    mock_get = Mock(return_value=mock_response)
    mock_post = Mock(return_value=mock_response)

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)

    return {"get": mock_get, "post": mock_post}

