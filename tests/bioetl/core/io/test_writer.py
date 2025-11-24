"""Unit tests for writer infrastructure helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from infrastructure.config.models.base import PipelineMetadata
from infrastructure.config.models.determinism import (
    DeterminismConfig,
    DeterminismHashingConfig,
    DeterminismSerializationConfig,
    DeterminismSerializationCSVConfig,
    DeterminismSortingConfig,
)
from infrastructure.config.models.http import HTTPClientConfig, HTTPConfig, RetryConfig
from infrastructure.config.models.models import (
    PipelineConfig,
    PipelineDomainConfig,
    PipelineInfrastructureConfig,
)
from infrastructure.config.models.paths import MaterializationConfig
from infrastructure.config.models.validation import ValidationConfig
from infrastructure.io.writer import write_dataset_atomic, write_frame_like, write_yaml_atomic


@pytest.fixture
def output_config(tmp_path: Path) -> PipelineConfig:
    infrastructure = PipelineInfrastructureConfig(
        http=HTTPConfig(
            default=HTTPClientConfig(
                timeout_sec=30.0,
                connect_timeout_sec=10.0,
                read_timeout_sec=30.0,
                retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
            ),
        ),
        materialization=MaterializationConfig(root=str(tmp_path)),
        determinism=DeterminismConfig(
            sort=DeterminismSortingConfig(by=["id"], ascending=[True]),
            hashing=DeterminismHashingConfig(
                business_key_fields=("id",),
                row_fields=("id", "value"),
            ),
            column_order=(),
            serialization=DeterminismSerializationConfig(
                booleans=("true", "false"),
                nan_rep="",
                csv=DeterminismSerializationCSVConfig(
                    separator=",",
                    na_rep="",
                    quoting="minimal",
                ),
            ),
            float_precision=6,
        ),
    )
    domain = PipelineDomainConfig(
        validation=ValidationConfig(
            strict=True,
            coerce=True,
            schema_out="infrastructure.schemas.chembl_activity_schema:ActivitySchema",
        ),
    )
    return PipelineConfig(
        version=1,
        pipeline=PipelineMetadata(
            name="test_pipeline",
            version="1.0.0",
            description="Test pipeline",
        ),
        domain=domain,
        infrastructure=infrastructure,
    )


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [3, 1, 2],
            "value": [30.0, 10.0, 20.0],
            "name": ["C", "A", "B"],
        }
    )


@pytest.mark.unit
class TestWriter:
    def test_write_dataset_atomic(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path
    ) -> None:
        output_path = tmp_path / "output.csv"
        write_dataset_atomic(sample_dataframe, output_path, config=output_config)

        assert output_path.exists()
        assert not output_path.with_suffix(output_path.suffix + ".tmp").exists()

        loaded = pd.read_csv(output_path)  # type: ignore[unknown-member]
        assert len(loaded) == 3

    def test_write_dataset_atomic_creates_directory(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path
    ) -> None:
        output_path = tmp_path / "subdir" / "output.csv"
        write_dataset_atomic(sample_dataframe, output_path, config=output_config)

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_yaml_atomic(self, tmp_path: Path) -> None:
        output_path = tmp_path / "output.yaml"
        payload = {"key": "value", "number": 123}

        write_yaml_atomic(payload, output_path)

        assert output_path.exists()
        assert not output_path.with_suffix(output_path.suffix + ".tmp").exists()

        with output_path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
        assert loaded == payload

    def test_write_yaml_atomic_creates_directory(self, tmp_path: Path) -> None:
        output_path = tmp_path / "subdir" / "output.yaml"
        payload = {"key": "value"}

        write_yaml_atomic(payload, output_path)

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_frame_like_dataframe(
        self, output_config: PipelineConfig, sample_dataframe: pd.DataFrame, tmp_path: Path
    ) -> None:
        output_path = tmp_path / "output.csv"
        write_frame_like(sample_dataframe, output_path, config=output_config)

        assert output_path.exists()
        loaded = pd.read_csv(output_path)  # type: ignore[unknown-member]
        assert len(loaded) == 3

    def test_write_frame_like_dict(
        self, output_config: PipelineConfig, tmp_path: Path
    ) -> None:
        output_path = tmp_path / "output.yaml"
        data = {"id": 1, "value": 10.0, "name": "test"}
        write_frame_like(data, output_path, config=output_config)

        assert output_path.exists()
        with output_path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
        assert loaded == data

    def test_write_frame_like_invalid(self, output_config: PipelineConfig, tmp_path: Path) -> None:
        output_path = tmp_path / "output.csv"
        with pytest.raises(TypeError, match="Unsupported frame-like type"):
            write_frame_like("invalid", output_path, config=output_config)  # type: ignore[arg-type]
