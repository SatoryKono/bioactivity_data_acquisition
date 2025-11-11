"""Tests for the shared batched extraction helper."""

from __future__ import annotations

from typing import Any, Sequence

import pandas as pd
import pytest

from bioetl.pipelines.chembl_base import BatchExtractionContext, ChemblPipelineBase


class DummyChemblPipeline(ChemblPipelineBase):
    """Minimal pipeline implementation exposing ``run_batched_extraction`` for tests."""

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError


@pytest.fixture
def dummy_pipeline(pipeline_config_fixture, run_id) -> DummyChemblPipeline:
    pipeline = DummyChemblPipeline(config=pipeline_config_fixture, run_id=run_id)
    pipeline._batched_stats = None  # type: ignore[attr-defined]
    return pipeline


def test_run_batched_extraction_deduplicates_and_sorts(dummy_pipeline: DummyChemblPipeline) -> None:
    ids = ["b", "a", "b", "", "c", "a"]
    batches: list[tuple[str, ...]] = []

    def fetch(batch_ids: Sequence[str], context: BatchExtractionContext) -> Sequence[dict[str, Any]]:
        batches.append(tuple(batch_ids))
        return [{"identifier": value} for value in batch_ids]

    dataframe, stats = dummy_pipeline.run_batched_extraction(
        ids,
        id_column="identifier",
        fetcher=fetch,
        batch_size=2,
        select_fields=["identifier"],
        metadata_filters={"note": "basic"},
        chembl_release="test-release",
        stats_attribute="_batched_stats",
    )

    assert dataframe["identifier"].tolist() == ["a", "b", "c"]
    assert batches == [("a", "b"), ("c",)]
    assert stats.requested == 3
    assert stats.batches == 2
    assert getattr(dummy_pipeline, "_batched_stats")["rows"] == 3

    recorded_filters = dummy_pipeline._extract_metadata.get("filters")  # type: ignore[reportPrivateUsage]
    assert recorded_filters is not None
    assert recorded_filters["batch_size"] == 2
    assert recorded_filters["select_fields"] == ["identifier"]
    assert recorded_filters["note"] == "basic"


def test_run_batched_extraction_respects_limit(dummy_pipeline: DummyChemblPipeline) -> None:
    ids = ["x1", "x2", "x3"]

    def fetch(batch_ids: Sequence[str], context: BatchExtractionContext) -> Sequence[dict[str, Any]]:
        return [{"identifier": value} for value in batch_ids]

    dataframe, stats = dummy_pipeline.run_batched_extraction(
        ids,
        id_column="identifier",
        fetcher=fetch,
        batch_size=3,
        limit=1,
        chembl_release="v1",
    )

    assert dataframe.shape[0] == 1
    assert stats.rows == 1
    assert stats.requested == 1


def test_run_batched_extraction_delegated_mode_updates_stats(
    dummy_pipeline: DummyChemblPipeline,
) -> None:
    summary = {"batches": 5, "api_calls": 3, "cache_hits": 2, "extra_field": "value"}

    def delegated_fetch(
        canonical_ids: Sequence[str],
        context: BatchExtractionContext,
    ) -> tuple[Sequence[dict[str, Any]], dict[str, Any]]:
        payload = [{"identifier": value} for value in canonical_ids]
        return payload, summary

    dataframe, stats = dummy_pipeline.run_batched_extraction(
        ["x", "y"],
        id_column="identifier",
        fetcher=delegated_fetch,
        batch_size=2,
        fetch_mode="delegated",
        stats_attribute="_batched_stats",
    )

    assert dataframe["identifier"].tolist() == ["x", "y"]
    assert stats.batches == 5
    assert stats.api_calls == 3
    assert stats.cache_hits == 2
    stored = getattr(dummy_pipeline, "_batched_stats")
    assert stored["cache_hits"] == 2
    assert stored["extra_field"] == "value"
