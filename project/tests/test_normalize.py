from project.library.io.normalize import coerce_text, normalise_doi, to_lc_stripped
from project.library.utils.joins import merge_records


def test_normalise_doi_variants() -> None:
    assert normalise_doi("10.1000/XYZ ") == "10.1000/xyz"
    assert normalise_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    assert normalise_doi("10.1000/xyz") == "10.1000/xyz"
    assert normalise_doi("not a doi") is None
    assert normalise_doi(None) is None


def test_to_lc_stripped() -> None:
    assert to_lc_stripped(" HeLLo ") == "hello"
    assert to_lc_stripped("  ABC ") == "abc"
    assert to_lc_stripped("123") == "123"
    assert to_lc_stripped(None) is None
    assert to_lc_stripped("   ") is None


def test_coerce_text() -> None:
    assert coerce_text(123) == "123"
    assert coerce_text(" value ") == "value"
    assert coerce_text(5) == "5"
    assert coerce_text(None) is None
    assert coerce_text("  ") is None


def test_merge_records_prefers_first_value() -> None:
    base: dict[str, object] = {"key": "value1"}
    merged = merge_records(base, [{"key": "value1"}, {"key": "value2"}])  # type: ignore
    assert merged["key"] == "value1"
    assert merged["key__alternatives"] == ["value1", "value2"]
