"""Smoke tests for the activity_chembl pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any
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
    """Fake ChEMBL client for activity pipeline tests."""

    def __init__(self, releases: list[str] | None = None) -> None:
        self.releases = releases or ["test-release"]
        self.status_calls = 0
        self.fetch_calls: list[list[str]] = []

    def status(self):
        """Return fake status."""
        self.status_calls += 1
        return {"chembl_release": self.releases[0]}

    def fetch_by_ids(self, ids):
        """Return fake data for requested IDs."""
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
    """Client that simulates failures for specific IDs."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_for: set[str] = {"2"}

    def fetch_by_ids(self, ids):
        """Return fake data with failures for specific IDs."""
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


class DummyConfig(PipelineConfig, Mapping):
    """Minimal pipeline config for activity_chembl tests."""

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
        # Add required ChEMBL configuration
        self._config = {
            "sources": {
                "chembl": {
                    "batch_size": 10,
                    "max_url_length": 1000
                }
            },
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["activity_id"]}},
            "ids": ids or ["1", "2", "3", "4", "5"],
            "metadata": self.metadata  # Add metadata to config dict
        }

    def __getitem__(self, key: str) -> Any:
        # Support nested key access with dot notation for config validation
        if "." in key:
            current = self._config
            for part in key.split("."):
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    raise KeyError(f"Missing configuration key: {key}")
            return current
        return self._config[key]

    def __iter__(self):
        return iter(self._config)

    def __len__(self) -> int:
        return len(self._config)

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)


def test_run_smoke(tmp_path: Path) -> None:
    """Run the pipeline and check output artifacts."""

    config = DummyConfig(tmp_path / "materialization")
    client = DummyChemblClient()
    pipeline = ChemblActivityPipeline(
        config,
        run_id="run-1",
        client_factory=lambda _cfg: client,
    )

    result = pipeline.run(tmp_path, sample=5, include_qc_metrics=True)

    assert result.success
    artifacts = result.artifacts
    run_dir = artifacts.output_dir
    write_artifacts = artifacts.write_artifacts
    assert write_artifacts is not None
    assert write_artifacts.meta_path is not None
    assert write_artifacts.manifest_path is not None
    assert write_artifacts.meta_path.exists()
    assert write_artifacts.manifest_path.exists()
    quality_reports = list((run_dir / "qc").glob("*quality_report.csv"))
    assert quality_reports
    assert client.status_calls == 1


def test_descriptor_extraction_handles_batches_and_failures(
    tmp_path: Path,
) -> None:
    """Ensure batched extraction tolerates failures and records metadata."""

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
    assert meta["failures"] == 2
    assert "chembl_release" in meta
    assert {"1", "3"}.issubset(set(df["activity_id"].astype(str)))
