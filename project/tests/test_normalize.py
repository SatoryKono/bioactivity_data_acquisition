from library.io.normalize import coerce_text, normalise_doi, to_lc_stripped


def test_normalise_doi_variants():
    assert normalise_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    assert normalise_doi("10.1000/xyz") == "10.1000/xyz"
    assert normalise_doi("not a doi") is None


def test_to_lc_stripped():
    assert to_lc_stripped("  ABC ") == "abc"
    assert to_lc_stripped(None) is None
    assert to_lc_stripped("   ") is None


def test_coerce_text():
    assert coerce_text(123) == "123"
    assert coerce_text(" value ") == "value"
    assert coerce_text(None) is None
