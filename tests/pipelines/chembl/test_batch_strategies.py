from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import pytest
from structlog.stdlib import BoundLogger

from infrastructure.chembl import (
    BatchExtractionContext,
    BatchExtractionStats,
    ChemblPipelineBase,
)
from infrastructure.config.models.models import PipelineConfig
from application.pipelines.specs.mixins.descriptor_builder import (
    DefaultFetchStrategy,
    DelegatedFetchStrategy,
    DescriptorStrategyFactory,
    SimpleNormalizationStrategy,
)
from infrastructure.logging import UnifiedLogger


class StrategyTestPipeline(ChemblPipelineBase):
    """Pipeline stub exposing ``run_batched_extraction`` for strategy tests."""

    id_column = "identifier"

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
        return df


@pytest.fixture()
def strategy_pipeline(pipeline_config_fixture: PipelineConfig, run_id: str) -> StrategyTestPipeline:
    pipeline = StrategyTestPipeline(config=pipeline_config_fixture, run_id=run_id)
    pipeline._batched_stats = None  # type: ignore[attr-defined]
    return pipeline


@pytest.fixture()
def bound_log() -> BoundLogger:
    return UnifiedLogger.get(__name__).bind(component="test")


def test_simple_normalization_strategy_sorts_and_limits() -> None:
    strategy = SimpleNormalizationStrategy()
    ids = ["b", "a", "b", "", None]
    result = strategy.normalize(ids, limit=2, id_normalizer=None, sort_key=None)

    assert result.unique_ids == ("a", "b")
    assert result.metadata == {"a": None, "b": None}
    assert isinstance(result.stats, BatchExtractionStats)
    assert result.stats.requested == 2


def test_default_fetch_strategy_batches_records(
    strategy_pipeline: StrategyTestPipeline,
    bound_log: BoundLogger,
) -> None:
    batches: list[tuple[str, ...]] = []
    contexts: list[BatchExtractionContext] = []

    def fetcher(batch: Sequence[str], context):  # type: ignore[no-untyped-def]
        batches.append(tuple(batch))
        contexts.append(context)
        assert isinstance(context, BatchExtractionContext)
        assert context.batch_size == 2
        return [{"identifier": value} for value in batch]

    factory = DescriptorStrategyFactory()
    plan = factory.build_plan(
        pipeline=strategy_pipeline,
        ids=["b", "a", "c"],
        id_column="identifier",
        select_fields=("identifier",),
        fetcher=fetcher,
        fetch_mode="default",
        limit=None,
        batch_size=2,
        chunk_size=None,
        max_batch_size=10,
        id_normalizer=None,
        sort_key=None,
        transform_item=None,
        finalize=None,
        finalize_context=None,
        empty_frame_factory=None,
        stats_attribute="_batched_stats",
        log=bound_log,
        started_at=0.0,
    )

    assert isinstance(plan.fetch_strategy, DefaultFetchStrategy)
    assert isinstance(plan.context, BatchExtractionContext)
    assert plan.context.ids == ("a", "b", "c")
    assert plan.context.chunk_size == 2

    dataframe, stats = plan.execute()

    assert batches == [("a", "b"), ("c",)]
    assert dataframe["identifier"].tolist() == ["a", "b", "c"]
    assert stats.requested == 3
    assert stats.batches == 2
    assert strategy_pipeline._batched_stats["rows"] == 3  # type: ignore[index]
    assert contexts and all(context is plan.context for context in contexts)


def test_delegated_fetch_strategy_merges_stats(
    strategy_pipeline: StrategyTestPipeline,
    bound_log: BoundLogger,
) -> None:
    summary = BatchExtractionStats(requested=2)
    summary.batches = 5
    summary.api_calls = 3
    summary.cache_hits = 1
    summary.set_extra(extra_flag="yes")

    def delegated_fetch(ids: Sequence[str], context):  # type: ignore[no-untyped-def]
        payload = [{"identifier": value} for value in ids]
        return payload, summary

    factory = DescriptorStrategyFactory()
    plan = factory.build_plan(
        pipeline=strategy_pipeline,
        ids=["c", "a"],
        id_column="identifier",
        select_fields=("identifier",),
        fetcher=delegated_fetch,
        fetch_mode="delegated",
        limit=None,
        batch_size=2,
        chunk_size=None,
        max_batch_size=25,
        id_normalizer=None,
        sort_key=None,
        transform_item=None,
        finalize=None,
        finalize_context=None,
        empty_frame_factory=None,
        stats_attribute="_batched_stats",
        log=bound_log,
        started_at=0.0,
    )

    assert isinstance(plan.fetch_strategy, DelegatedFetchStrategy)
    assert plan.context.ids == ("a", "c")

    dataframe, stats = plan.execute()

    assert dataframe["identifier"].tolist() == ["a", "c"]
    assert stats.batches == 5
    assert stats.api_calls == 3
    assert stats.cache_hits == 1
    assert stats.extra["extra_flag"] == "yes"
    assert strategy_pipeline._batched_stats["cache_hits"] == 1  # type: ignore[index]
