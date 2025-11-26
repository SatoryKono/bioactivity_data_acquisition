from __future__ import annotations

from pathlib import Path
from bioetl.core.pipeline.types import (
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
)
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
from bioetl.pipelines.chembl.common.descriptor import (
    ChemblExtractionDescriptor,
)


class DummyChemblClient:
    def __init__(self, releases: list[str] | None = None) -> None:
        self.releases = releases or ["test-release"]
        self.status_calls = 0
        self.fetch_calls: list[list[str]] = []

    def status(self):
        self.status_calls += 1
        return {"chembl_release": self.releases[0]}

    def fetch_by_ids(self, ids):
        self.fetch_calls.append(list(ids))
        return {
            str(identifier): {
                "activity_id": identifier,
                "assay_id": f"ASSAY{identifier}",
                "target_chembl_id": f"T{identifier}",
                "standard_value": 1.0,
                "standard_units": "nM",
            }
            for identifier in ids
        }


class FailingChemblClient(DummyChemblClient):
    def __init__(self) -> None:
        super().__init__()
        self.fail_for: set[str] = {"2"}

    def fetch_by_ids(self, ids):
        self.fetch_calls.append(list(ids))
        data = {}
        for identifier in ids:
            if identifier in self.fail_for:
                raise RuntimeError("boom")
            data[str(identifier)] = {
                "activity_id": identifier,
                "assay_id": f"ASSAY{identifier}",
                "target_chembl_id": f"T{identifier}",
                "standard_value": 1.0,
                "standard_units": "nM",
            }
        return data


class DummyConfig(PipelineConfig):
    metadata: dict

    def __init__(
        self,
        root: Path,
        pipeline_name: str = "activity_chembl",
        ids=None,
    ) -> None:
        super().__init__(
            pipeline=PipelineInfo(name=pipeline_name),
            materialization=MaterializationConfig(root=root),
        )
        self.metadata = {"ids": ids or ["1", "2", "3", "4", "5"]}


def test_run_smoke(tmp_path: Path) -> None:
    config = DummyConfig(tmp_path / "materialization")
    client = DummyChemblClient()
    pipeline = ChemblActivityPipeline(
        config,
        run_id="run-1",
        client_factory=lambda _cfg: client,
    )

    result = pipeline.run(tmp_path, sample=5)

    assert result.success
    run_dir = next(tmp_path.glob("_*"))
    assert (run_dir / "meta.yaml").exists()
    assert (run_dir / "run_manifest.json").exists()
    quality_reports = list((run_dir / "qc").glob("*quality_report.csv"))
    assert quality_reports
    assert client.status_calls == 1


def test_descriptor_extraction_handles_batches_and_failures(
    tmp_path: Path,
) -> None:
    config = DummyConfig(tmp_path / "materialization")
    client = FailingChemblClient()
    pipeline = ChemblActivityPipeline(
        config,
        run_id="run-2",
        client_factory=lambda _cfg: client,
    )
    descriptor = ChemblExtractionDescriptor(
        ids=["1", "2", "3"],
        pagination=None,
        mode="chembl",
        batch_plan=None,
    )

    df, meta = pipeline.run_descriptor_extraction(descriptor, batch_size=2)

    assert client.status_calls == 1
    assert client.fetch_calls[0] == ["1", "2"]
    assert client.fetch_calls[1] == ["3"]
    assert meta["failures"] == 1
    assert "chembl_release" in meta
    assert {"1", "3"}.issubset(set(df["activity_id"].astype(str)))
