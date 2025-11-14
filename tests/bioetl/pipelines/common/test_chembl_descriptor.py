"""Tests for the descriptor-driven extraction template."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.chembl.common.descriptor import (
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
        current_pipeline: _DummyChemblPipeline,
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
