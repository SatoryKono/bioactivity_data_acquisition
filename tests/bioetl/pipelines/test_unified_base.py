"""Tests for the unified pipeline base and mixins."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.core.pipeline import PipelineExtractionMode, RunResult
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
from bioetl.pipelines.chembl.assay.run import ChemblAssayPipeline
from bioetl.pipelines.chembl.document.run import ChemblDocumentPipeline
from bioetl.pipelines.chembl.target.run import ChemblTargetPipeline
from bioetl.pipelines.chembl.testitem.run import TestItemChemblPipeline
from bioetl.pipelines.unified_base import ChemblPipelineContract, UnifiedPipelineBase


class DummyUnifiedPipeline(UnifiedPipelineBase):
    """Minimal pipeline implementation for exercising the mixins."""

    actor = "dummy_unified"
    id_column = "identifier"

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._output_column_order = ("identifier", "value", "note")
        self._extract_df = pd.DataFrame({"identifier": ["a"], "value": [1]})
        self.observed_pages: list[tuple[int, dict[str, object]]] = []

    def build_descriptor(self):  # pragma: no cover - not used in tests
        raise NotImplementedError

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = PipelineExtractionMode.AUTO,
        ids: Sequence[str] | None = None,
        **legacy_kwargs: object,
    ) -> pd.DataFrame:
        _ = (mode, ids)
        return self._extract_df.copy()

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - compatibility hook
        return self._extract_df.copy()

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

    first = unified_pipeline.perform_handshake(client, "/status")
    second = unified_pipeline.perform_handshake(client, "/status")

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


def test_stage_logger_records_duration_and_emits_events(
    unified_pipeline: DummyUnifiedPipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = MagicMock()
    unified_pipeline.logger_for = MagicMock(return_value=logger)  # type: ignore[assignment]

    counter = iter([1.0, 1.25])

    def fake_perf_counter() -> float:
        return next(counter)

    monkeypatch.setattr("time.perf_counter", fake_perf_counter)  # type: ignore[arg-type]

    with unified_pipeline.stage_logger("extract", rows=3) as log:
        assert log is logger
        log.info("custom_event")

    assert "extract" in unified_pipeline._stage_durations_ms
    assert unified_pipeline._stage_durations_ms["extract"] == pytest.approx(250.0)

    unified_pipeline.logger_for.assert_called_once_with(stage="extract", component=None)
    logger.info.assert_any_call("stage_started", rows=3)
    logger.info.assert_any_call("stage_completed", duration_ms=pytest.approx(250.0), rows=3)


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


def test_transform_lifecycle_invokes_hooks_in_order(
    pipeline_config_fixture,
    run_id,
) -> None:
    class TrackingPipeline(DummyUnifiedPipeline):
        actor = "tracking_pipeline"

        def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            super().__init__(*args, **kwargs)
            self.hooks: list[str] = []

        def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
            self.hooks.append("pre")
            df = df.copy()
            df["pre_marker"] = "seen"
            return df

        def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
            self.hooks.append("domain")
            return super().domain_enrich(df)

        def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
            self.hooks.append("post")
            df = df.copy()
            df["post_marker"] = "done"
            return df

    tracking_pipeline = TrackingPipeline(pipeline_config_fixture, run_id)

    source_df = pd.DataFrame(
        {
            "identifier": ["alpha"],
            "value": [42],
            "note": [pd.NA],
        }
    )

    result = tracking_pipeline.transform(source_df)

    assert tracking_pipeline.hooks == ["pre", "domain", "post"]
    assert "post_marker" in result.columns
    assert result.loc[0, "post_marker"] == "done"
    assert "pre_marker" not in source_df.columns


def test_save_results_writes_dataset(
    unified_pipeline: DummyUnifiedPipeline, tmp_path: Path
) -> None:
    result = unified_pipeline.save_results(pd.DataFrame(), tmp_path)
    assert result.write_result.dataset.exists()


def test_save_results_coerces_none_flags(
    unified_pipeline: DummyUnifiedPipeline, tmp_path: Path
) -> None:
    result = unified_pipeline.save_results(
        pd.DataFrame(),
        tmp_path,
        include_correlation=None,  # type: ignore[arg-type]
        include_qc_metrics=None,  # type: ignore[arg-type]
    )

    assert isinstance(result, RunResult)
    assert result.write_result.dataset.exists()


def test_unified_pipeline_run_produces_artifacts(
    unified_pipeline: DummyUnifiedPipeline, tmp_output_dir: Path
) -> None:
    result = unified_pipeline.run(tmp_output_dir)
    assert result.write_result.dataset.exists()


def test_unified_pipeline_run_tracks_lifecycle(
    pipeline_config_fixture, run_id, tmp_output_dir: Path
) -> None:
    class LifecyclePipeline(DummyUnifiedPipeline):
        def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            super().__init__(*args, **kwargs)
            self.lifecycle: list[str] = []
            self.finalized: RunResult | None = None

        def prepare_run(self) -> None:
            self.lifecycle.append("prepare")

        def finalize_run(self, result: RunResult | None) -> None:
            self.lifecycle.append("finalize")
            self.finalized = result

    pipeline = LifecyclePipeline(pipeline_config_fixture, run_id)

    result = pipeline.run(tmp_output_dir)

    assert result.records == 1
    assert {"extract", "transform", "validate", "write"}.issubset(
        result.stage_durations_ms.keys()
    )
    assert pipeline.lifecycle == ["prepare", "finalize"]
    assert pipeline.finalized is result


def test_unified_pipeline_satisfies_contract(
    unified_pipeline: DummyUnifiedPipeline,
) -> None:
    assert isinstance(unified_pipeline, ChemblPipelineContract)


def test_unified_pipeline_run_signature_matches_contract() -> None:
    sig = inspect.signature(UnifiedPipelineBase.run)
    assert sig.return_annotation in {RunResult, "RunResult"}

    param_names = list(sig.parameters.keys())
    assert param_names == [
        "self",
        "output_dir",
        "extended",
        "include_correlation",
        "include_qc_metrics",
        "qc_reports",
        "qc_thresholds",
        "fail_on_qc_violation",
    ]

    kwonly_params = [sig.parameters[name] for name in param_names[2:]]
    for param in kwonly_params:
        assert param.kind is inspect.Parameter.KEYWORD_ONLY

    assert sig.parameters["extended"].default is False
    assert sig.parameters["include_correlation"].default is False
    assert sig.parameters["include_qc_metrics"].default is False
    assert sig.parameters["qc_reports"].default is None
    assert sig.parameters["qc_thresholds"].default is None
    assert sig.parameters["fail_on_qc_violation"].default is False


def test_all_chembl_pipelines_use_unified_base() -> None:
    pipelines = [
        ChemblActivityPipeline,
        ChemblAssayPipeline,
        ChemblDocumentPipeline,
        ChemblTargetPipeline,
        TestItemChemblPipeline,
    ]

    for pipeline_cls in pipelines:
        assert issubclass(pipeline_cls, UnifiedPipelineBase)
