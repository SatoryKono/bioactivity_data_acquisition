"""Tests for the shared descriptor-driven extraction helpers."""

from __future__ import annotations

from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
from structlog.stdlib import BoundLogger

from infrastructure.chembl import (
    BatchExtractionStats,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from infrastructure.config.models.models import PipelineConfig


class DescriptorTestPipeline(ChemblPipelineBase):
    """Minimal pipeline exposing ``run_descriptor_extraction`` for tests."""

    id_column = "test_id"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config=config, run_id=run_id)
        self.resolved_release = "chembl-99"
        self.resolve_invocations = 0

    def build_descriptor(self):  # pragma: no cover - not used directly
        raise NotImplementedError

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
        return df

    def resolve_chembl_release(
        self,
        chembl_client: object,
        log: BoundLogger,
        entity_client: object | None = None,
    ) -> tuple[str | None, dict[str, Any]]:
        self.resolve_invocations += 1
        return self.resolved_release, {"api_version": "v1"}


class _IteratorStub:
    """Provide ``iterate_by_ids`` so descriptor contexts can be built generically."""

    def iterate_by_ids(
        self,
        ids: Sequence[str],
        *,
        select_fields: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        return [{DescriptorTestPipeline.id_column: identifier} for identifier in ids]


@pytest.fixture()
def descriptor_pipeline(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> DescriptorTestPipeline:
    config = pipeline_config_fixture.model_copy(deep=True)
    config.pipeline = config.pipeline.model_copy(update={"name": "dummy_chembl"})
    return DescriptorTestPipeline(config=config, run_id=run_id)


def _build_basic_descriptor(
    chembl_client: object,
) -> ChemblExtractionDescriptor[DescriptorTestPipeline]:
    def build_context(
        pipeline: DescriptorTestPipeline,
        source_config: Any,
        log: BoundLogger,
    ) -> ChemblExtractionContext:
        context = ChemblExtractionContext(
            source_config=source_config,
            iterator=_IteratorStub(),
            chembl_client=chembl_client,
        )
        context.select_fields = ("test_id", "value")
        return context

    return ChemblExtractionDescriptor[DescriptorTestPipeline](
        name="dummy_descriptor",
        source_name="chembl",
        source_config_factory=lambda cfg: cfg,
        build_context=build_context,
        id_column="test_id",
        summary_event="dummy.summary",
    )


def test_run_descriptor_extraction_passes_shared_arguments(
    descriptor_pipeline: DescriptorTestPipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_descriptor_extraction must centralise shared orchestration details."""

    source_config = SimpleNamespace(batch_size=10)
    chembl_client = object()
    descriptor = _build_basic_descriptor(chembl_client)

    captured: dict[str, Any] = {}

    def stub_run_batched_extraction(
        self: DescriptorTestPipeline,
        ids: Sequence[str],
        **kwargs: Any,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        captured["ids"] = tuple(ids)
        captured["kwargs"] = dict(kwargs)
        stats = BatchExtractionStats(requested=len(ids))
        stats.rows = len(ids)
        frame = pd.DataFrame({descriptor.id_column: list(ids)})
        return frame, stats

    monkeypatch.setattr(
        DescriptorTestPipeline,
        "run_batched_extraction",
        stub_run_batched_extraction,
    )

    def fetcher_factory(
        context: ChemblExtractionContext,
        log: BoundLogger,
    ) -> Any:
        def fetch(batch_ids: Sequence[str], batch_context: Any) -> list[dict[str, Any]]:
            return [{descriptor.id_column: identifier} for identifier in batch_ids]

        return fetch

    dataframe, stats = descriptor_pipeline.run_descriptor_extraction(
        descriptor,
        ids=["b", "a", "b"],
        source_config=source_config,
        summary_event="dummy.summary",
        fetcher_factory=fetcher_factory,
        metadata_filters={"mode": "ids"},
        batch_size=5,
        max_batch_size=25,
    )

    assert descriptor_pipeline.resolve_invocations == 1
    assert descriptor_pipeline.chembl_release == descriptor_pipeline.resolved_release

    assert captured["ids"] == ("b", "a", "b")
    kwargs = captured["kwargs"]
    assert kwargs["id_column"] == descriptor.id_column
    assert kwargs["metadata_filters"] == {"mode": "ids"}
    assert kwargs["chembl_release"] == descriptor_pipeline.resolved_release
    assert kwargs["select_fields"] == ("test_id", "value")
    assert callable(kwargs["fetcher"])

    assert dataframe[descriptor.id_column].tolist() == ["b", "a", "b"]
    assert stats.requested == 3
    assert stats.rows == 3


def test_run_descriptor_extraction_dry_run_short_circuits(
    descriptor_pipeline: DescriptorTestPipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dry-run mode must not call ``run_batched_extraction`` and should use handler."""

    descriptor_pipeline.config.cli.dry_run = True
    source_config = SimpleNamespace(batch_size=25)
    chembl_client = object()
    base_descriptor = _build_basic_descriptor(chembl_client)

    dry_run_called = {"flag": False}

    def dry_run_handler(
        pipeline: DescriptorTestPipeline,
        context: ChemblExtractionContext,
        log: BoundLogger,
    ) -> pd.DataFrame:
        dry_run_called["flag"] = True
        return pd.DataFrame({DescriptorTestPipeline.id_column: pd.Series(dtype="string")})

    descriptor = ChemblExtractionDescriptor[DescriptorTestPipeline](
        name="dummy_descriptor",
        source_name="chembl",
        source_config_factory=lambda cfg: cfg,
        build_context=base_descriptor.build_context,
        id_column="test_id",
        summary_event="dummy.summary",
        dry_run_handler=dry_run_handler,
    )

    def fail_run_batched(*args: Any, **kwargs: Any) -> None:  # pragma: no cover - guard
        raise AssertionError("run_batched_extraction must not be called in dry-run mode")

    monkeypatch.setattr(
        DescriptorTestPipeline,
        "run_batched_extraction",
        fail_run_batched,
    )

    dataframe, stats = descriptor_pipeline.run_descriptor_extraction(
        descriptor,
        ids=["x", "y", "x"],
        source_config=source_config,
        summary_event="dummy.summary",
        dry_run_event="dummy.dry_run",
        dry_run_handler=dry_run_handler,
    )

    assert dry_run_called["flag"] is True
    assert stats.requested == 3
    assert dataframe.empty
    assert descriptor_pipeline.resolve_invocations == 1
    assert descriptor_pipeline.chembl_release == descriptor_pipeline.resolved_release
