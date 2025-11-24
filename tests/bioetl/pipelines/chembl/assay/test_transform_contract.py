"""Контрактные тесты transform/save для ChemblAssayPipeline."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.config.models.base import PipelineMetadata
from bioetl.config.models.http import HTTPClientConfig, HTTPConfig
from bioetl.config.models.models import (
    PipelineConfig,
    PipelineDomainConfig,
    PipelineInfrastructureConfig,
)
from bioetl.config.models.transform import TransformConfig
from bioetl.config.models.validation import ValidationConfig
from bioetl.pipelines.chembl.assay.run import ChemblAssayPipeline


def _minimal_config() -> PipelineConfig:
    domain = PipelineDomainConfig(
        transform=TransformConfig(
            arrays_to_header_rows=["assay_classifications", "assay_parameters"],
        ),
        validation=ValidationConfig(schema_out="bioetl.schemas.chembl_assay_schema.AssaySchema"),
    )
    infrastructure = PipelineInfrastructureConfig(
        http=HTTPConfig(default=HTTPClientConfig()),
    )
    return PipelineConfig(
        version=1,
        pipeline=PipelineMetadata(name="assay_chembl", version="1.0.0"),
        domain=domain,
        infrastructure=infrastructure,
    )


@pytest.mark.unit
def test_transform_empty_dataframe_passthrough() -> None:
    """Пустой вход должен возвращаться без модификаций и без побочных эффектов."""

    pipeline = ChemblAssayPipeline(_minimal_config(), run_id="run-empties")

    empty = pd.DataFrame()
    result = pipeline.transform(empty)

    assert result.empty
    assert list(result.columns) == []


@pytest.mark.unit
def test_transform_adds_missing_columns_and_coerces_confidence_score() -> None:
    """transform обеспечивает обязательные столбцы и типы без обращения к IO."""

    pipeline = ChemblAssayPipeline(_minimal_config(), run_id="run-transform")
    df = pd.DataFrame(
        [
            {
                "assay_chembl_id": "CHEMBL1",
                "assay_id": 1,
                "assay_type": "B",
                "confidence_score": "7",
                "assay_classifications": [],
            }
        ]
    )

    result = pipeline.transform(df)

    assert "assay_type_description" in result.columns
    assert result["confidence_score"].dtype == "Int64"
    assert result.loc[0, "confidence_score"] == 7


@pytest.mark.unit
def test_normalize_nested_structures_handles_nan() -> None:
    """NaN в классификациях трактуется как отсутствие идентификатора."""

    pipeline = ChemblAssayPipeline(_minimal_config(), run_id="run-nan")
    df = pd.DataFrame(
        {
            "assay_chembl_id": ["CHEMBL1"],
            "assay_classifications": [float("nan")],
        }
    )

    normalized = pipeline._normalize_nested_structures(df)

    assert pd.isna(normalized.loc[0, "assay_class_id"])
