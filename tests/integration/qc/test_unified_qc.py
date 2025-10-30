"""Integration tests validating shared QC helpers across pipelines."""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd

if "cachetools" not in sys.modules:
    cachetools_module = types.ModuleType("cachetools")

    class _StubTTLCache(dict):
        def __init__(self, maxsize: int, ttl: int, *args: object, **kwargs: object) -> None:
            super().__init__()

    cachetools_module.TTLCache = _StubTTLCache
    sys.modules["cachetools"] = cachetools_module

from bioetl.pipelines.activity import ActivityPipeline
from bioetl.pipelines.base import PipelineBase
from bioetl.pipelines.document import DocumentPipeline
from bioetl.pipelines.target import TargetPipeline


def _make_pipeline_stub(pipeline_cls: type[PipelineBase], name: str) -> PipelineBase:
    """Instantiate a pipeline without running subclass initialisers."""

    determinism = types.SimpleNamespace(
        column_order=[],
        float_precision=9,
        datetime_format="iso8601",
        sort=types.SimpleNamespace(by=[], ascending=[]),
    )
    sources = {
        "chembl": {
            "base_url": f"https://{name}.example/api",
            "stage": "primary",
        }
    }
    config = types.SimpleNamespace(
        pipeline=types.SimpleNamespace(name=name),
        determinism=determinism,
        config_hash=f"{name}-hash",
        sources=sources,
    )
    pipeline = pipeline_cls.__new__(pipeline_cls)  # type: ignore[call-arg]
    PipelineBase.__init__(pipeline, config, run_id="test")
    return pipeline


def test_qc_summary_format_consistent_across_pipelines() -> None:
    """QC summary helpers should yield consistent payloads for all pipelines."""

    pipelines = [
        _make_pipeline_stub(TargetPipeline, "target"),
        _make_pipeline_stub(ActivityPipeline, "activity"),
        _make_pipeline_stub(DocumentPipeline, "document"),
    ]

    for pipeline in pipelines:
        pipeline.set_qc_metrics({"rows.total": {"value": 3, "passed": True}})
        pipeline.add_qc_summary_section("row_counts", {"dataset": 3})
        pipeline.add_qc_summary_section("datasets", {"dataset": {"rows": 3}})
        pipeline.record_validation_issue({"severity": "warning"})
        pipeline.refresh_validation_issue_summary()

    reference = pipelines[0].qc_summary_data
    for candidate in pipelines[1:]:
        assert candidate.qc_summary_data == reference


def test_additional_table_specs_are_uniform() -> None:
    """Additional table registration should mirror Target pipeline behaviour."""

    pipelines = [
        _make_pipeline_stub(TargetPipeline, "target"),
        _make_pipeline_stub(ActivityPipeline, "activity"),
        _make_pipeline_stub(DocumentPipeline, "document"),
    ]

    supplemental = pd.DataFrame({"id": [1], "value": ["foo"]}).convert_dtypes()
    relative = Path("qc") / "supplemental.csv"

    for pipeline in pipelines:
        pipeline.add_additional_table("supplemental", supplemental, relative_path=relative)

    expected = pipelines[0].additional_tables["supplemental"]
    for candidate in pipelines[1:]:
        spec = candidate.additional_tables["supplemental"]
        assert spec.relative_path == expected.relative_path
        pd.testing.assert_frame_equal(spec.dataframe, expected.dataframe)


def test_export_metadata_generation_consistent() -> None:
    """Pipelines should rely on the shared helper for export metadata."""

    pipelines = [
        _make_pipeline_stub(TargetPipeline, "target"),
        _make_pipeline_stub(ActivityPipeline, "activity"),
        _make_pipeline_stub(DocumentPipeline, "document"),
    ]

    dataset = pd.DataFrame({"col": [1, 2], "other": ["a", "b"]})

    for pipeline in pipelines:
        metadata = pipeline.set_export_metadata_from_dataframe(
            dataset,
            pipeline_version="1.2.3",
            source_system="chembl",
            chembl_release="33",
        )
        assert metadata.pipeline_version == "1.2.3"
        assert metadata.source_system == "chembl"
        assert metadata.chembl_release == "33"
        assert metadata.row_count == len(dataset)
        assert metadata.column_count == dataset.shape[1]
        assert metadata.column_order == list(dataset.columns)
        assert metadata.run_id == "test"
        assert metadata.config_hash == pipeline.config_hash
        assert metadata.git_commit == pipeline.git_commit
        assert metadata.sources == pipeline.export_metadata.sources

    reference = pipelines[0].export_metadata
    for candidate in pipelines[1:]:
        meta = candidate.export_metadata
        assert meta.pipeline_version == reference.pipeline_version
        assert meta.source_system == reference.source_system
        assert meta.chembl_release == reference.chembl_release
        assert meta.row_count == reference.row_count
        assert meta.column_count == reference.column_count
        assert meta.column_order == reference.column_order
        assert isinstance(meta.generated_at, str)
        assert meta.config_hash == reference.config_hash
        assert meta.git_commit == reference.git_commit
        assert meta.sources == reference.sources

