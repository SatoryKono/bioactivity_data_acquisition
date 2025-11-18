"""Tests for the descriptor-driven extraction template."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from bioetl.config.models.models import PipelineConfig
from bioetl.chembl.common.descriptor import (
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)


@dataclass
class _DummySourceConfig:
    batch_size: int
    parameters: dict[str, Any]


class _DummyIterator:
    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records
        self.calls: list[dict[str, Any]] = []

    def iterate_all(
        self,
        *,
        limit: int | None = None,
        page_size: int | None = None,
        select_fields: list[str] | None = None,
    ):
        self.calls.append(
            {
                "limit": limit,
                "page_size": page_size,
                "select_fields": list(select_fields) if select_fields else None,
            }
        )
        yield from self._records


class _DummyChemblPipeline(ChemblPipelineBase):
    actor = "dummy_chembl"

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_by_ids(self, ids: list[str]) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
        return df


def test_run_extract_all_descriptor_applies_hooks(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """The shared template should respect must-have fields and post processors."""

    pipeline = _DummyChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

    records = [{"id": 2, "value": "b"}, {"id": 1, "value": "a"}]
    iterator = _DummyIterator(records)

    def build_context(
        _current_pipeline: _DummyChemblPipeline,
        _: _DummySourceConfig,
        __: Any,
    ) -> ChemblExtractionContext:
        return ChemblExtractionContext(
            source_config=_DummySourceConfig(batch_size=5, parameters={}),
            iterator=iterator,
            select_fields=["value"],
            chembl_release="test-release",
        )

    def post_process(
        _: _DummyChemblPipeline,
        df: pd.DataFrame,
        __: ChemblExtractionContext,
        ___: Any,
    ) -> pd.DataFrame:
        result = df.copy()
        result["extra"] = "ok"
        return result

    descriptor = ChemblExtractionDescriptor(
        name="dummy",
        source_name="chembl",
        source_config_factory=lambda _: _DummySourceConfig(batch_size=5, parameters={}),
        build_context=build_context,
        id_column="id",
        summary_event="dummy.extract_summary",
        must_have_fields=("id", "required_id"),
        default_select_fields=("id", "value"),
        post_processors=(post_process,),
        sort_by=("id",),
        empty_frame_factory=lambda *_: pd.DataFrame({"id": pd.Series(dtype="Int64")}),
    )

    dataframe = pipeline.run_extract_all(descriptor)

    assert list(dataframe["id"]) == [1, 2]
    assert list(dataframe["extra"]) == ["ok", "ok"]

    call = iterator.calls[0]
    assert call["select_fields"] == ["value", "id", "required_id"]

    metadata = pipeline._extract_metadata  # noqa: SLF001 - accessing for verification
    assert metadata["chembl_release"] == "test-release"
    assert metadata["filters"]["select_fields"] == ["value", "id", "required_id"]


def test_run_extract_all_dry_run_handler_skips_iteration(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """Dry-run mode should short-circuit iteration via descriptor handler."""

    pipeline_config_fixture.cli.dry_run = True  # type: ignore[attr-defined]
    pipeline = _DummyChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

    iterator = _DummyIterator(records=[{"id": 1}])

    def build_context(
        _: _DummyChemblPipeline,
        __: _DummySourceConfig,
        ___: Any,
    ) -> ChemblExtractionContext:
        return ChemblExtractionContext(
            source_config=_DummySourceConfig(batch_size=10, parameters={}),
            iterator=iterator,
            select_fields=["id"],
            chembl_release="dry-run-release",
        )

    dry_run_calls: list[tuple[_DummyChemblPipeline, ChemblExtractionContext]] = []

    def dry_run_handler(
        current_pipeline: _DummyChemblPipeline,
        context: ChemblExtractionContext,
        __: Any,
        ___: float,
    ) -> pd.DataFrame:
        dry_run_calls.append((current_pipeline, context))
        return pd.DataFrame({"id": [99], "value": ["dry"]})

    descriptor = ChemblExtractionDescriptor(
        name="dummy",
        source_name="chembl",
        source_config_factory=lambda _: _DummySourceConfig(batch_size=10, parameters={}),
        build_context=build_context,
        id_column="id",
        summary_event="dummy.extract_summary",
        dry_run_handler=dry_run_handler,
    )

    dataframe = pipeline.run_extract_all(descriptor)

    assert list(dataframe["id"]) == [99]
    assert dry_run_calls and dry_run_calls[0][0] is pipeline
    assert dry_run_calls[0][1].chembl_release == "dry-run-release"
    assert iterator.calls == []

    metadata = pipeline._extract_metadata  # noqa: SLF001 - accessing for verification
    assert metadata["chembl_release"] == "dry-run-release"


def test_run_extract_all_post_processors_run_in_order(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """Multiple post processors must be executed sequentially."""

    pipeline = _DummyChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

    records = [{"id": 1, "value": "a"}]
    iterator = _DummyIterator(records)

    def build_context(
        _: _DummyChemblPipeline,
        __: _DummySourceConfig,
        ___: Any,
    ) -> ChemblExtractionContext:
        return ChemblExtractionContext(
            source_config=_DummySourceConfig(batch_size=5, parameters={}),
            iterator=iterator,
            select_fields=["id"],
            chembl_release="post-release",
        )

    called: list[str] = []

    def first_processor(
        _: _DummyChemblPipeline,
        df: pd.DataFrame,
        __: ChemblExtractionContext,
        ___: Any,
    ) -> pd.DataFrame:
        called.append("first")
        result = df.copy()
        result["value"] = result["value"].str.upper()
        return result

    def second_processor(
        _: _DummyChemblPipeline,
        df: pd.DataFrame,
        __: ChemblExtractionContext,
        ___: Any,
    ) -> pd.DataFrame:
        called.append("second")
        result = df.copy()
        result["extra"] = "done"
        return result

    descriptor = ChemblExtractionDescriptor(
        name="dummy",
        source_name="chembl",
        source_config_factory=lambda _: _DummySourceConfig(batch_size=5, parameters={}),
        build_context=build_context,
        id_column="id",
        summary_event="dummy.extract_summary",
        post_processors=(first_processor, second_processor),
    )

    dataframe = pipeline.run_extract_all(descriptor)

    assert called == ["first", "second"]
    assert list(dataframe["value"]) == ["A"]
    assert list(dataframe["extra"]) == ["done"]
