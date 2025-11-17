"""Tests for the unified pipeline base and mixins."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.pipelines.unified_base import UnifiedPipelineBase


class DummyUnifiedPipeline(UnifiedPipelineBase):
    """Minimal pipeline implementation for exercising the mixins."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._output_column_order = ("identifier", "value", "note")
        self._extract_df = pd.DataFrame({"identifier": ["a"], "value": [1]})
        self.observed_pages: list[tuple[int, dict[str, object]]] = []

    def build_descriptor(self):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        return self._extract_df.copy()

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - compatibility hook
        return self.extract()

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        return pd.DataFrame({"identifier": list(ids), "value": list(range(len(ids)))})

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        df["note"] = "ok"
        return df

    def on_page(self, index: int, meta: dict[str, object]) -> None:
        self.observed_pages.append((index, dict(meta)))


@pytest.fixture
def unified_pipeline(pipeline_config_fixture, run_id) -> DummyUnifiedPipeline:
    return DummyUnifiedPipeline(pipeline_config_fixture, run_id)


def test_perform_handshake_caches_result(unified_pipeline: DummyUnifiedPipeline) -> None:
    client = MagicMock()
    client.handshake.return_value = {"chembl_release": "33", "api_version": "1"}

    first = unified_pipeline.perform_handshake(client, "/status.json")
    second = unified_pipeline.perform_handshake(client, "/status.json")

    assert first == second
    client.handshake.assert_called_once()


def test_iterate_pages_invokes_on_page(unified_pipeline: DummyUnifiedPipeline) -> None:
    client = MagicMock()

    first_response = MagicMock()
    first_response.json.return_value = {
        "results": [{"identifier": "a"}],
        "page_meta": {"next": "/next"},
    }
    second_response = MagicMock()
    second_response.json.return_value = {
        "results": [{"identifier": "b"}],
        "page_meta": {"next": None},
    }

    client.get.side_effect = [first_response, second_response]

    pages = list(
        unified_pipeline.iterate_pages(
            client,
            "/items",
            params={"dummy": "1"},
            page_size=1,
            items_key="results",
        )
    )

    assert len(pages) == 2
    assert pages[0][1][0]["identifier"] == "a"
    assert unified_pipeline.observed_pages[0][0] == 0
    assert unified_pipeline.observed_pages[-1][0] == 1


def test_run_batched_extraction_bridge(unified_pipeline: DummyUnifiedPipeline) -> None:
    ids = ["id-1", "id-2"]

    def fetch(batch: Sequence[str], context):  # type: ignore[no-untyped-def]
        return [{"identifier": value, "value": 1} for value in batch]

    dataframe, stats = unified_pipeline.run_batched_extraction(
        ids,
        id_column="identifier",
        fetcher=fetch,
        batch_size=2,
    )

    assert dataframe.shape[0] == 2
    assert stats.requested == 2


def test_transform_pipeline_flow(unified_pipeline: DummyUnifiedPipeline) -> None:
    df = pd.DataFrame({"value": [2], "identifier": ["item"]})
    transformed = unified_pipeline.transform(df)
    assert "note" in transformed.columns
    assert transformed.loc[0, "note"] == "ok"


def test_save_results_writes_dataset(
    unified_pipeline: DummyUnifiedPipeline, tmp_path: Path
) -> None:
    result = unified_pipeline.save_results(pd.DataFrame(), tmp_path)
    assert result.dataset.exists()


def test_unified_pipeline_run_produces_artifacts(
    unified_pipeline: DummyUnifiedPipeline, tmp_output_dir: Path
) -> None:
    result = unified_pipeline.run(tmp_output_dir)
    assert result.write_result.dataset.exists()
