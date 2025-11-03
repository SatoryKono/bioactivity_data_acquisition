from types import SimpleNamespace

import pytest

from bioetl.sources.chembl.testitem.request import TestItemRequestBuilder


class StubAPIClient:
    def __init__(self, base_url: str = "https://api.example") -> None:
        self.config = SimpleNamespace(base_url=base_url)


def test_build_filter_params_and_url() -> None:
    api_client = StubAPIClient()
    builder = TestItemRequestBuilder(api_client=api_client, batch_size=50, max_url_length=None)

    params = builder.build_filter_params(["CHEMBL1", "CHEMBL2"])
    assert params["molecule_chembl_id__in"] == "CHEMBL1,CHEMBL2"
    assert params["limit"] == "2"

    url = builder.build_url(["CHEMBL1", "CHEMBL2"])
    assert url.startswith("https://api.example/molecule.json?")
    assert "molecule_chembl_id__in=CHEMBL1%2CCHEMBL2" in url


@pytest.mark.parametrize(
    "ids,expected_batches",
    [
        ([], []),
        (["CHEMBL1"], [["CHEMBL1"]]),
        (["CHEMBL1", "CHEMBL2", "CHEMBL3"], [["CHEMBL1", "CHEMBL2"], ["CHEMBL3"]]),
    ],
)
def test_iter_batches_respects_batch_size(ids: list[str], expected_batches: list[list[str]]) -> None:
    api_client = StubAPIClient()
    builder = TestItemRequestBuilder(api_client=api_client, batch_size=2, max_url_length=None)

    assert builder.iter_batches(ids) == expected_batches


def test_iter_batches_respects_url_limit() -> None:
    api_client = StubAPIClient()
    builder = TestItemRequestBuilder(api_client=api_client, batch_size=10, max_url_length=70)
    ids = [f"CHEMBL{i}" for i in range(1, 6)]

    batches = builder.iter_batches(ids)

    # ensure splitting due to URL limit rather than batch size
    assert len(batches) > 1
    for batch in batches:
        url = builder.build_url(batch)
        assert len(url) <= 70 or len(batch) == 1
