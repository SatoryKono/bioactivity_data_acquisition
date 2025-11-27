from pathlib import Path

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.services import RunMetadataBuilder
from bioetl.core.pipeline.types import (
    DefaultArtifactContext,
    DefaultDomainContext,
    DefaultExecutionContext,
    DefaultInfrastructureContext,
    StageContext,
)


class _DummyStage:
    def __init__(self, name: str) -> None:
        self.name = name


class _DummyPipeline:
    pipeline_code = "dummy_pipeline"

    def build_pipeline_metadata(self, context: StageContext | None = None) -> dict:
        del context
        return {"chembl_release": "33", "extract_metadata": {"source": "pipeline"}}


def test_run_metadata_builder_merges_pipeline_metadata(tmp_path: Path) -> None:
    pipeline = _DummyPipeline()
    context = StageContext(
        execution=DefaultExecutionContext(
            logger=UnifiedLogger.get("test"), request_id="test-run"
        ),
        domain=DefaultDomainContext(pipeline=pipeline, metadata={"extract_rows": 5}),
        infrastructure=DefaultInfrastructureContext(output_dir=tmp_path),
        artifacts=DefaultArtifactContext(),
    )
    builder = RunMetadataBuilder({}, pipeline.pipeline_code)

    metadata = builder.build(
        context,
        stages=(_DummyStage("extract"),),
        durations={"extract": 100},
        run_tag="tag-1",
        mode=None,
    )

    assert metadata["chembl_release"] == "33"
    assert metadata["extract_metadata"] == {"extract_rows": 5, "source": "pipeline"}
