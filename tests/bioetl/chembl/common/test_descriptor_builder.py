"""Tests for the declarative Chembl descriptor builder mixin."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.chembl.common.descriptor import (
    ChemblContextSpec,
    ChemblDescriptorBuilderMixin,
    ChemblDescriptorSpec,
    ChemblExtractionContext,
    ChemblPipelineBase,
)
from bioetl.config.models.models import PipelineConfig
from bioetl.config.target import TargetSourceConfig


class _ProbePipeline(
    ChemblDescriptorBuilderMixin["_ProbePipeline"],
    ChemblPipelineBase,
):  # noqa: UP037
    """Minimal pipeline harnessing the descriptor builder mixin for tests."""

    actor = "probe"
    id_column = "probe_id"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self.override_release = "override-release"
        self.release_resolver_calls = 0
        self.after_build_calls = 0
        self.probe_flag = "flagged"

    def extract_by_ids(self, _: Any) -> pd.DataFrame:
        return pd.DataFrame()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - minimal stub
        return df

    def descriptor_spec(self) -> ChemblDescriptorSpec[_ProbePipeline]:
        def summary_extra(
            _: _ProbePipeline,
            __: pd.DataFrame,
            ___: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            return {"after_build_calls": self.after_build_calls}

        context_spec = ChemblContextSpec(
            entity_name="probe_entity",
            release_resolver=self._release_resolver,
            select_fields_resolver=lambda pipeline, _: ("foo", "bar"),
            extra_filters_factory=lambda __, pipeline: {"flag": pipeline.probe_flag},
            client_registry_name=lambda pipeline: f"{pipeline.actor}_client",
            chembl_release_override=lambda pipeline: pipeline.override_release,
            page_size_resolver=lambda _: 13,
            after_build=self._after_build,
        )

        return ChemblDescriptorSpec(
            name="probe_descriptor",
            source_name="chembl",
            source_config_factory=TargetSourceConfig.from_source_config,
            context=context_spec,
            id_column="probe_id",
            summary_event="probe.summary",
            summary_extra=summary_extra,
        )

    def _release_resolver(
        self,
        _: _ProbePipeline,
        __: Any,
        ___: BoundLogger,
        ____: Any | None,
    ) -> str:
        self.release_resolver_calls += 1
        return "resolved"

    def _after_build(
        self,
        _: _ProbePipeline,
        context: ChemblExtractionContext,
        source_config: TargetSourceConfig,
        __: BoundLogger,
    ) -> ChemblExtractionContext:
        self.after_build_calls += 1
        context.metadata["batch_size"] = source_config.batch_size
        return context


def test_descriptor_builder_uses_spec(
    monkeypatch: Any,
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """Ensure the mixin wires context spec settings into the descriptor."""

    pipeline = _ProbePipeline(pipeline_config_fixture, run_id)
    captured: dict[str, Any] = {}

    def fake_build_standard(
        pipeline_obj: ChemblPipelineBase,
        entity_name: str,
        source_config: Any,
        log: BoundLogger,
        **kwargs: Any,
    ) -> ChemblExtractionContext:
        captured["pipeline"] = pipeline_obj
        captured["entity_name"] = entity_name
        captured["kwargs"] = kwargs
        return ChemblExtractionContext(
            source_config,
            iterator="iter",
            chembl_client="client",
            select_fields=("foo",),
            page_size=5,
            chembl_release="release",
        )

    monkeypatch.setattr(
        "bioetl.chembl.common.descriptor.build_standard_chembl_context",
        fake_build_standard,
    )

    descriptor = pipeline.build_descriptor()
    source_config = TargetSourceConfig()
    log = cast(
        BoundLogger,
        SimpleNamespace(
            debug=lambda *args, **kwargs: None,
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        ),
    )
    context = descriptor.build_context(pipeline, source_config, log)

    assert captured["entity_name"] == "probe_entity"
    assert captured["kwargs"]["client_registry_name"] == "probe_client"
    assert captured["kwargs"]["chembl_release_override"] == "override-release"
    assert captured["kwargs"]["page_size_resolver"](source_config) == 13

    extra_filters = captured["kwargs"]["extra_filters_factory"](source_config, pipeline)
    assert extra_filters == {"flag": "flagged"}

    release_value = captured["kwargs"]["release_resolver"](pipeline, object(), log, None)
    assert release_value == "resolved"
    assert pipeline.release_resolver_calls == 1
    assert pipeline.after_build_calls == 1
    assert context.metadata["batch_size"] == source_config.batch_size

    empty_frame = descriptor.empty_frame_factory(pipeline, context)
    assert empty_frame.columns.tolist() == ["probe_id"]
    assert empty_frame["probe_id"].dtype.name == "string"

    summary = descriptor.summary_extra(pipeline, pd.DataFrame(), context)
    assert summary["after_build_calls"] == pipeline.after_build_calls
    assert descriptor.summary_event == "probe.summary"
