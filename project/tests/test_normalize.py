from project.library.io.normalize import coerce_text, normalise_doi, to_lc_stripped
from project.library.utils.joins import merge_records


def test_normalise_doi_variants():
    assert normalise_doi("10.1000/XYZ ") == "10.1000/xyz"
    assert normalise_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    assert normalise_doi(None) is None


def test_to_lc_stripped():
    assert to_lc_stripped(" HeLLo ") == "hello"
    assert to_lc_stripped(123) == "123"
    assert to_lc_stripped(None) is None


def test_merge_records_prefers_first_value():
    base = {"key": "value1"}
    merged = merge_records(base, [{"key": "value1"}, {"key": "value2"}])
    assert merged["key"] == "value1"
    assert merged["key__alternatives"] == ["value1", "value2"]


def test_coerce_text_handles_none():
    assert coerce_text(None) is None
    assert coerce_text("  ") is None
    assert coerce_text(5) == "5"

