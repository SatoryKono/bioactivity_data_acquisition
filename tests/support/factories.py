"""Reusable test factories for configs and sample datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config.models.base import PipelineMetadata
from bioetl.config.models.cli import CLIConfig
from bioetl.config.models.determinism import (
    DeterminismConfig,
    DeterminismHashingConfig,
    DeterminismSortingConfig,
)
from bioetl.config.models.domain import PipelineDomainConfig
from bioetl.config.models.http import HTTPClientConfig, HTTPConfig, RetryConfig
from bioetl.config.models.infrastructure import PipelineInfrastructureConfig
from bioetl.config.models.models import PipelineConfig
from bioetl.config.models.paths import MaterializationConfig
from bioetl.config.models.postprocess import PostprocessConfig
from bioetl.config.models.source import SourceConfig, SourceParameters
from bioetl.config.models.validation import ValidationConfig

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "bioetl" / "data"


def load_test_json(relative_path: str) -> Any:
    """Load JSON payloads stored under ``tests/bioetl/data``."""

    data_path = DATA_DIR / relative_path
    with data_path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def load_sample_activity_dataframe() -> pd.DataFrame:
    """Return the canonical sample activity DataFrame used in tests."""

    dataset: dict[str, list[Any]] = load_test_json("sample_activity_data.json")
    frame = pd.DataFrame(dataset)

    # Explicit nullable integer columns for deterministic schema assertions.
    nullable_columns = ("assay_tax_id", "record_id", "src_id", "target_tax_id")
    for column in nullable_columns:
        frame[column] = pd.Series(dataset[column], dtype="Int64")

    return frame


def build_pipeline_config(output_root: Path) -> PipelineConfig:
    """Construct a deterministic ``PipelineConfig`` for tests."""

    http_config = HTTPConfig(
        default=HTTPClientConfig(
            timeout_sec=30.0,
            connect_timeout_sec=10.0,
            read_timeout_sec=30.0,
            retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
        ),
    )

    determinism_config = DeterminismConfig(
        sort=DeterminismSortingConfig(by=[], ascending=[]),
        hashing=DeterminismHashingConfig(business_key_fields=()),
    )

    infrastructure_config = PipelineInfrastructureConfig(
        http=http_config,
        materialization=MaterializationConfig(root=str(output_root)),
        determinism=determinism_config,
        cli=CLIConfig(date_tag="20240101"),
    )

    domain_config = PipelineDomainConfig(
        validation=ValidationConfig(schema_out=None, strict=True, coerce=True),
        postprocess=PostprocessConfig(),
        sources={
            "chembl": SourceConfig(
                enabled=True,
                parameters=SourceParameters.from_mapping(
                    {
                        "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                        "max_url_length": 2000,
                    }
                ),
            )
        },
    )

    pipeline_metadata = PipelineMetadata(
        name="activity_chembl",
        version="1.0.0",
        description="Test activity pipeline",
    )
    payload = {
        "version": 1,
        "pipeline": pipeline_metadata.model_dump(),
        "domain": domain_config.model_dump(),
        "infrastructure": infrastructure_config.model_dump(),
    }
    return PipelineConfig.model_validate(payload)

