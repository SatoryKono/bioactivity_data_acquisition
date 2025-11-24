from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
import pytest

from infrastructure.clients.common import BaseEntityFetcher


class DummyFetcher(BaseEntityFetcher):
    def __init__(self, total: int, *, default_chunk_size: int = 2):
        super().__init__(default_chunk_size=default_chunk_size)
        self._total = total

    def fetch_page(self, url: str, params: Mapping[str, object] | None = None):  # type: ignore[override]
        offset = int(params.get("offset", 0)) if params else 0
        limit = int(params.get("limit", 0)) if params else 0
        end = min(self._total, offset + limit if limit else self._total)
        records = [{"id": index, "value": str(index)} for index in range(offset, end)]
        return {"items": records}


def test_chunked_fetch_iterates_pages():
    fetcher = DummyFetcher(total=5, default_chunk_size=2)

    records = list(fetcher.chunked_fetch("http://example.test"))

    assert [record["id"] for record in records] == [0, 1, 2, 3, 4]


def test_records_to_dataframe_casting():
    fetcher = BaseEntityFetcher(default_chunk_size=1)
    records = [
        {"activity_id": "10", "score": "3.5"},
        {"activity_id": "11", "score": None},
    ]
    field_mapping = {"id": "activity_id", "score": "score"}
    dtype_map = {"id": int, "score": float}

    frame = fetcher.records_to_dataframe(records, field_mapping, dtype_map=dtype_map)

    assert list(frame.columns) == ["id", "score"]
    assert frame["id"].tolist() == [10, 11]
    assert frame.loc[0, "score"] == pytest.approx(3.5)
    assert pd.isna(frame.loc[1, "score"])
    assert isinstance(frame, pd.DataFrame)
