"""Tests for identifier normalizer behaviors."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


class _DummyTTLCache(dict):
    """Minimal TTLCache stub used to avoid optional dependency in tests."""

    def __init__(self, maxsize, ttl):  # noqa: D401 - simple stub
        super().__init__()
        self.maxsize = maxsize
        self.ttl = ttl


# Ensure the API client can import cachetools even if it's missing in test envs
sys.modules.setdefault("cachetools", SimpleNamespace(TTLCache=_DummyTTLCache))

from bioetl.normalizers.identifier import IdentifierNormalizer
from bioetl.config.loader import load_config  # noqa: E402  (after cachetools stub)
from bioetl.pipelines.document import DocumentPipeline  # noqa: E402


@pytest.fixture
def normalizer() -> IdentifierNormalizer:
    """Provide a fresh instance for each test."""

    return IdentifierNormalizer()


@pytest.mark.parametrize(
    "value, expected",
    [
        ("CHEMBL123", "CHEMBL123"),
        (" chembl123 ", "CHEMBL123"),
        ("10.1234/Example", "10.1234/example"),
        (" 123456 ", "123456"),
        ("p12345", "P12345"),
        ("q9h2x3-2", "Q9H2X3-2"),
        ("a0a024r161", "A0A024R161"),
        ("InChIKey=ABCDEFJKLMNopqrstuvw", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_cases(normalizer: IdentifierNormalizer, value: str | None, expected: str | None) -> None:
    """Normalize should return canonical form for known identifiers or ``None``."""

    if value is None:
        result = normalizer.normalize(value)  # type: ignore[arg-type]
    else:
        result = normalizer.normalize(value)
    assert result == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("CHEMBL123", True),
        ("10.1234/example", True),
        ("987654", True),
        ("Q9H2X3", True),
        ("Q9H2X3-2", True),
        ("A0A024R161", True),
        ("W1234567890", True),
        ("0000-0002-1825-0097", True),
        ("ABCDEFGHIJKLMN-OPQRSTUVWX-Y", True),
        ("InChIKey=ABCDEFJKLMNopqrstuvw", False),
        ("CHEMBL", False),
        ("10.123/example", False),
        ("", False),
        ("https://openalex.org/W1234567890", False),
    ],
)
def test_validate_patterns(normalizer: IdentifierNormalizer, value: str, expected: bool) -> None:
    """Validate should accept only canonical patterns."""

    assert normalizer.validate(value) is expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("0000-0002-1825-0097", "0000-0002-1825-0097"),
        ("https://orcid.org/0000-0002-1825-0097", "0000-0002-1825-0097"),
        ("0000-0002-1825-009x", "0000-0002-1825-009X"),
        ("0000-0002-1825-009", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_orcid(normalizer: IdentifierNormalizer, value: str | None, expected: str | None) -> None:
    """ORCID normalization should strip URLs and enforce checksum pattern."""

    if value is None:
        result = normalizer.normalize_orcid(value)  # type: ignore[arg-type]
    else:
        result = normalizer.normalize_orcid(value)
    assert result == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("https://openalex.org/W1234567890", "W1234567890"),
        ("W1234567890", "W1234567890"),
        (" https://openalex.org/W7654321 ", "W7654321"),
        ("https://openalex.org/w1234", None),
        ("X1234", None),
        (None, None),
    ],
)
def test_normalize_openalex(normalizer: IdentifierNormalizer, value: str | None, expected: str | None) -> None:
    """OpenAlex IDs should return the short canonical identifier when valid."""

    if value is None:
        result = normalizer.normalize_openalex_id(value)  # type: ignore[arg-type]
    else:
        result = normalizer.normalize_openalex_id(value)
    assert result == expected


@pytest.fixture(scope="module")
def document_config():
    """Load the document pipeline configuration once for these tests."""

    return load_config("configs/pipelines/document.yaml")


@pytest.fixture
def document_pipeline(document_config, monkeypatch):
    """Instantiate a DocumentPipeline with external dependencies patched."""

    monkeypatch.setattr(DocumentPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")
    return DocumentPipeline(document_config, run_id="test-normalizer")


def test_prepare_input_ids_filters_non_canonical(document_pipeline):
    """Pipeline should accept only canonical CHEMBL identifiers after normalization."""

    df = pd.DataFrame(
        {
            "document_chembl_id": [
                " chembl123 ",
                "CHEMBL123",  # duplicate after normalization
                "CHEMBL999",  # valid second identifier
                "CHEMBLINVALID",  # invalid format
                "   ",  # whitespace only
                None,
            ]
        }
    )

    valid_ids, rejected = document_pipeline._prepare_input_ids(df)

    assert valid_ids == ["CHEMBL123", "CHEMBL999"]
    assert {item["document_chembl_id"]: item["reason"] for item in rejected} == {
        "CHEMBLINVALID": "invalid_format",
        "": "missing",
        "   ": "missing",
    }
