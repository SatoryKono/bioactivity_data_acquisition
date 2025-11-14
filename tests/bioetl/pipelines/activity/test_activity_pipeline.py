"""Unit tests for ChemblActivityPipeline transformations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests.exceptions import RequestException

from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.config.models.models import PipelineConfig
from bioetl.core.http.api_client import CircuitBreakerOpenError
from bioetl.pipelines.chembl.activity import run
from bioetl.schemas.chembl_activity_schema import ActivitySchema


@pytest.mark.unit
class TestChemblActivityPipelineTransformations:
    """Test suite for ChemblActivityPipeline transformations."""

    def test_normalize_identifiers_valid(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of valid ChEMBL IDs."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["CHEMBL1", "chembl2", "CHEMBL3"],
                "assay_chembl_id": ["CHEMBL100", "chembl101", None],
                "target_chembl_id": ["CHEMBL200", None, "chembl201"],
            }
        )

        normalized = pipeline._normalize_identifiers(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["molecule_chembl_id"].iloc[0] == "CHEMBL1"
        assert normalized["molecule_chembl_id"].iloc[1] == "CHEMBL2"
        assert normalized["molecule_chembl_id"].iloc[2] == "CHEMBL3"

    def test_normalize_identifiers_invalid(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of invalid ChEMBL IDs."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["INVALID", "CHEMBL1", "not_chembl"],
                "assay_chembl_id": ["CHEMBL100", None, None],
            }
        )

        normalized = pipeline._normalize_identifiers(df, MagicMock())  # type: ignore[reportPrivateUsage]

        # Invalid IDs should be set to None
        assert pd.isna(normalized["molecule_chembl_id"].iloc[0])
        assert normalized["molecule_chembl_id"].iloc[1] == "CHEMBL1"
        assert pd.isna(normalized["molecule_chembl_id"].iloc[2])

    def test_normalize_identifiers_bao(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of BAO identifiers."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "bao_endpoint": ["BAO_0000001", "bao_0000002", "INVALID"],
                "bao_format": ["BAO_0000003", None, "not_bao"],
            }
        )

        normalized = pipeline._normalize_identifiers(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["bao_endpoint"].iloc[0] == "BAO_0000001"
        assert normalized["bao_endpoint"].iloc[1] == "BAO_0000002"
        assert pd.isna(normalized["bao_endpoint"].iloc[2])

    def test_normalize_measurements_standard_value(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of standard_value."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_value": ["10.5", "20.0", " 5.3 ", "10-20", "invalid"],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["standard_value"].iloc[0] == 10.5
        assert normalized["standard_value"].iloc[1] == 20.0
        assert normalized["standard_value"].iloc[2] == 5.3
        assert normalized["standard_value"].iloc[3] == 10.0  # First value from range
        assert pd.isna(normalized["standard_value"].iloc[4])  # Invalid becomes NaN

    def test_normalize_measurements_negative_values(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that negative standard_values are set to None."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_value": [-10.0, 10.0, -5.5],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert pd.isna(normalized["standard_value"].iloc[0])
        assert normalized["standard_value"].iloc[1] == 10.0
        assert pd.isna(normalized["standard_value"].iloc[2])

    def test_normalize_measurements_standard_relation(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of standard_relation."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_relation": ["=", "<=", "≥", "≤", "invalid"],
                "relation": ["=", " ≤", "≥", "invalid", None],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["standard_relation"].iloc[0] == "="
        # "<=" is not listed in RELATIONS, therefore it becomes None.
        assert pd.isna(normalized["standard_relation"].iloc[1])
        # "≥" is normalized to ">=", but the value is not allowed in RELATIONS, so it becomes None.
        assert pd.isna(normalized["standard_relation"].iloc[2])
        # "≤" is normalized to "<=", but the value is not allowed in RELATIONS, so it becomes None.
        assert pd.isna(normalized["standard_relation"].iloc[3])
        assert pd.isna(normalized["standard_relation"].iloc[4])  # Invalid becomes None

        assert normalized["relation"].iloc[0] == "="
        # "≤" is normalized to "<=", but it is not allowed in RELATIONS, so it becomes None.
        assert pd.isna(normalized["relation"].iloc[1])
        # "≥" is normalized to ">=", but it is not allowed in RELATIONS, so it becomes None.
        assert pd.isna(normalized["relation"].iloc[2])
        assert pd.isna(normalized["relation"].iloc[3])
        assert pd.isna(normalized["relation"].iloc[4])

    def test_normalize_measurements_standard_type(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of standard_type."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_type": ["IC50", "EC50", "invalid_type", None],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["standard_type"].iloc[0] == "IC50"
        # "EC50" is not part of STANDARD_TYPES, so it becomes None.
        assert pd.isna(normalized["standard_type"].iloc[1])
        assert pd.isna(normalized["standard_type"].iloc[2])  # Invalid becomes None
        assert pd.isna(normalized["standard_type"].iloc[3])  # None stays None

    def test_normalize_measurements_standard_units(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of standard_units."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_units": [
                    "nM",
                    "nanomolar",
                    "μM",
                    "uM",
                    "mM",
                    "millimolar",
                    "%",
                    "percent",
                ],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["standard_units"].iloc[0] == "nM"
        assert normalized["standard_units"].iloc[1] == "nM"  # Normalized
        assert normalized["standard_units"].iloc[2] == "μM"
        assert normalized["standard_units"].iloc[3] == "μM"  # Normalized
        assert normalized["standard_units"].iloc[4] == "mM"
        assert normalized["standard_units"].iloc[5] == "mM"  # Normalized
        assert normalized["standard_units"].iloc[6] == "%"
        assert normalized["standard_units"].iloc[7] == "%"  # Normalized

    def test_normalize_string_fields(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of string fields."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "canonical_smiles": [" CCO ", "  ", "CCN"],
                "bao_label": ["Binding", "  ", None],
                "target_organism": ["homo sapiens", "mus musculus", None],
                "data_validity_comment": ["Comment", "", None],
                "activity_comment": [" comment ", "", None],
                "standard_text_value": ["  >10 ", "", None],
                "type": ["  IC50  ", "", None],
                "units": ["  nM ", "", None],
            }
        )

        normalized = pipeline._normalize_string_fields(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["canonical_smiles"].iloc[0] == "CCO"
        assert pd.isna(normalized["canonical_smiles"].iloc[1])  # Empty becomes None
        assert normalized["canonical_smiles"].iloc[2] == "CCN"

        assert normalized["target_organism"].iloc[0] == "Homo Sapiens"  # Title case
        assert normalized["target_organism"].iloc[1] == "Mus Musculus"

        assert normalized["activity_comment"].iloc[0] == "comment"
        assert pd.isna(normalized["activity_comment"].iloc[1])

        assert normalized["standard_text_value"].iloc[0] == ">10"
        assert pd.isna(normalized["standard_text_value"].iloc[1])

        assert normalized["type"].iloc[0] == "IC50"
        assert pd.isna(normalized["type"].iloc[1])

        assert normalized["units"].iloc[0] == "nM"
        assert pd.isna(normalized["units"].iloc[1])

    def test_normalize_nested_structures(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of nested structures to JSON strings."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "ligand_efficiency": [{"LE": 0.5}, None, '{"LE": 0.3}'],
                "activity_properties": [
                    {"type": "Ki", "value": 5.0, "units": "nM"},
                    None,
                    [],
                ],
            }
        )

        normalized = pipeline._normalize_nested_structures(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert isinstance(normalized["ligand_efficiency"].iloc[0], str)
        assert pd.isna(normalized["ligand_efficiency"].iloc[1])
        assert normalized["ligand_efficiency"].iloc[2] == '{"LE": 0.3}'

        props_row = json.loads(normalized["activity_properties"].iloc[0])
        assert props_row == [
            {
                "type": "Ki",
                "relation": None,
                "units": "nM",
                "value": 5.0,
                "text_value": None,
                "result_flag": None,
            }
        ]
        assert pd.isna(normalized["activity_properties"].iloc[1])
        assert json.loads(normalized["activity_properties"].iloc[2]) == []

    def test_normalize_activity_properties_mixed_payloads(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Ensure numeric/text payloads and result flag survive normalization."""

        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_properties": [
                    [
                        {
                            "type": "IC50",
                            "relation": "=",
                            "units": "nM",
                            "value": 10.0,
                            "text_value": None,
                            "result_flag": True,
                            "extra": "drop",
                        },
                        {
                            "type": "Comment",
                            "text_value": "Active",
                        },
                    ],
                    '{"type": "Ki", "value": "5", "text_value": "not numeric"}',
                ]
            }
        )

        normalized = pipeline._normalize_nested_structures(df, MagicMock())  # type: ignore[reportPrivateUsage]

        first_payload = json.loads(normalized["activity_properties"].iloc[0])
        assert len(first_payload) == 2
        assert first_payload[0] == {
            "type": "IC50",
            "relation": "=",
            "units": "nM",
            "value": 10.0,
            "text_value": None,
            "result_flag": True,
        }
        assert first_payload[1] == {
            "type": "Comment",
            "relation": None,
            "units": None,
            "value": None,
            "text_value": "Active",
            "result_flag": None,
        }

        second_payload = json.loads(normalized["activity_properties"].iloc[1])
        assert second_payload == [
            {
                "type": "Ki",
                "relation": None,
                "units": None,
                "value": "5",
                "text_value": "not numeric",
                "result_flag": None,
            }
        ]

    def test_normalize_data_types(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test data type conversions."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": ["1", "2", "3"],
                "target_tax_id": ["9606", "10090", None],
                "standard_value": ["10.5", "20.0", None],
                "pchembl_value": ["7.98", None, "8.28"],
                "potential_duplicate": [1, 0, None],
                "standard_flag": ["1", "0", None],
                "upper_value": ["15.0", None, "30"],
                "lower_value": [None, "5", "10"],
            }
        )

        log = MagicMock()
        normalized = pipeline._normalize_data_types(df, ActivitySchema, log)  # type: ignore[reportPrivateUsage]

        assert normalized["activity_id"].dtype.name == "Int64"  # type: ignore[reportUnknownMemberType]
        assert normalized["target_tax_id"].dtype.name == "Int64"  # type: ignore[reportUnknownMemberType]
        assert normalized["standard_value"].dtype.name == "float64"  # type: ignore[reportUnknownMemberType]
        assert normalized["pchembl_value"].dtype.name == "float64"  # type: ignore[reportUnknownMemberType]
        assert normalized["potential_duplicate"].dtype.name == "boolean"  # type: ignore[reportUnknownMemberType]
        assert normalized["standard_flag"].dtype.name == "Int64"  # type: ignore[reportUnknownMemberType]
        assert normalized["standard_flag"].tolist() == [1, 0, pd.NA]  # type: ignore[reportUnknownMemberType]
        assert normalized["upper_value"].dtype.name == "float64"  # type: ignore[reportUnknownMemberType]
        assert normalized["lower_value"].dtype.name == "float64"  # type: ignore[reportUnknownMemberType]

    def test_validate_foreign_keys(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test foreign key validation."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["CHEMBL1", "INVALID", "CHEMBL2"],
                "assay_chembl_id": ["CHEMBL100", "CHEMBL101", "invalid"],
            }
        )

        # Should not raise, but log warnings
        normalized = pipeline._validate_foreign_keys(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert len(normalized) == 3

    def test_check_activity_id_uniqueness(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test activity_id uniqueness check."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df_unique = pd.DataFrame({"activity_id": [1, 2, 3]})
        pipeline._check_activity_id_uniqueness(df_unique, MagicMock())  # type: ignore[reportPrivateUsage]
        # Should not raise

        df_duplicate = pd.DataFrame({"activity_id": [1, 1, 2]})
        with pytest.raises(ValueError, match="duplicate activity_id"):
            pipeline._check_activity_id_uniqueness(df_duplicate, MagicMock())  # type: ignore[reportPrivateUsage]

    def test_check_foreign_key_integrity(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test foreign key integrity check."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df_valid = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100", "CHEMBL101", None],
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", None],
            }
        )
        pipeline._check_foreign_key_integrity(df_valid, MagicMock())  # type: ignore[reportPrivateUsage]
        # Should not raise

        df_invalid = pd.DataFrame(
            {
                "assay_chembl_id": ["INVALID", "CHEMBL101", None],
                "molecule_chembl_id": ["CHEMBL1", "invalid", None],
            }
        )
        with pytest.raises(ValueError, match="Foreign key integrity check failed"):
            pipeline._check_foreign_key_integrity(df_invalid, MagicMock())  # type: ignore[reportPrivateUsage]

    def test_transform_empty_dataframe(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test transform with empty DataFrame."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df_empty = pd.DataFrame()
        result = pipeline.transform(df_empty)

        assert result.empty

    def test_transform_invalid_payload(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test transform with invalid payload type."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # transform expects pd.DataFrame, so passing string should raise TypeError or AttributeError
        with pytest.raises((TypeError, AttributeError)):
            pipeline.transform("invalid_payload")  # type: ignore[arg-type]

    def test_transform_harmonizes_identifier_columns(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that transform harmonizes identifier columns and drops aliases."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "assay_id": ["CHEMBL100", "CHEMBL101"],
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2"],
                "testitem_id": ["CHEMBL1", "CHEMBL2"],
            }
        )

        transformed = pipeline.transform(df)

        assert "assay_chembl_id" in transformed.columns
        assert "testitem_chembl_id" in transformed.columns
        assert "assay_id" not in transformed.columns
        assert "testitem_id" not in transformed.columns
        assert transformed["assay_chembl_id"].tolist() == ["CHEMBL100", "CHEMBL101"]
        assert transformed["testitem_chembl_id"].equals(transformed["molecule_chembl_id"])

    def test_transform_preserves_raw_measurements(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Ensure transform keeps raw measurement fields alongside standardized ones."""

        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1],
                "assay_chembl_id": ["CHEMBL100"],
                "testitem_chembl_id": ["CHEMBL1"],
                "molecule_chembl_id": ["CHEMBL1"],
                "type": [" Ki "],
                "relation": ["≤"],
                "value": ["~10"],
                "units": [" μM"],
                "standard_type": [None],
                "standard_relation": [None],
                "standard_value": [None],
                "standard_units": [None],
                "standard_text_value": ["  about 10"],
                "standard_flag": ["0"],
                "upper_value": ["15"],
                "lower_value": [5],
                "activity_comment": [" raw measurement"],
            }
        )

        transformed = pipeline.transform(df)

        assert transformed.loc[0, "type"] == "Ki"
        # "≤" normalizes to "<=", but RELATIONS does not include it, so it becomes None.
        assert pd.isna(transformed.loc[0, "relation"])
        assert transformed.loc[0, "value"] == "~10"
        assert transformed.loc[0, "units"] == "μM"
        assert transformed.loc[0, "standard_flag"] == 0
        assert transformed.loc[0, "upper_value"] == 15.0
        assert transformed.loc[0, "lower_value"] == 5.0
        assert transformed.loc[0, "activity_comment"] == "raw measurement"
        assert pd.isna(transformed.loc[0, "standard_value"])
        assert pd.isna(transformed.loc[0, "standard_type"])

    def test_extract_from_chembl_batches_and_cache(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        tmp_path: Path,
    ) -> None:
        """Ensure batched extraction invokes the API and warms the cache."""

        pipeline_config_fixture.paths.cache_root = str(tmp_path)
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "33"  # type: ignore[reportPrivateUsage]

        dataset = pd.DataFrame({"activity_id": [1, 2, 3]})

        chembl_client_stub = MagicMock()

        iterator_client = MagicMock()

        def paginate(
            endpoint: str, *, params: Mapping[str, Any], page_size: int, items_key: str | None
        ) -> Any:
            values = params["activity_id__in"].split(",")
            payload = [{"activity_id": int(value), "standard_type": "IC50"} for value in values]
            return iter(payload)

        iterator_client.paginate.side_effect = paginate

        activity_iterator = ChemblActivityClient(iterator_client, batch_size=2)

        def passthrough(df: pd.DataFrame, *_: Any, **__: Any) -> pd.DataFrame:
            return df

        with (
            patch.object(pipeline, "_extract_data_validity_descriptions", side_effect=passthrough),
            patch.object(pipeline, "_extract_assay_fields", side_effect=passthrough),
            patch.object(pipeline, "_log_validity_comments_metrics"),
        ):
            result = pipeline._extract_from_chembl(
                dataset,
                chembl_client_stub,
                activity_iterator,
                select_fields=["activity_id", "standard_type"],
            )  # type: ignore[reportPrivateUsage]

        assert list(result["activity_id"]) == [1, 2, 3]
        stats = pipeline._last_batch_extract_stats  # type: ignore[reportPrivateUsage]
        assert stats is not None
        assert stats["api_calls"] == 2
        assert stats["cache_hits"] == 0

        cached_iterator_client = MagicMock()
        cached_iterator_client.paginate.side_effect = AssertionError(
            "paginate should not be called when cache is warm"
        )
        cached_activity_iterator = ChemblActivityClient(cached_iterator_client, batch_size=2)

        with (
            patch.object(pipeline, "_extract_data_validity_descriptions", side_effect=passthrough),
            patch.object(pipeline, "_extract_assay_fields", side_effect=passthrough),
            patch.object(pipeline, "_log_validity_comments_metrics"),
        ):
            cached_result = pipeline._extract_from_chembl(
                dataset,
                chembl_client_stub,
                cached_activity_iterator,
                select_fields=["activity_id", "standard_type"],
            )  # type: ignore[reportPrivateUsage]

        assert list(cached_result["activity_id"]) == [1, 2, 3]
        cached_iterator_client.paginate.assert_not_called()
        cached_stats = pipeline._last_batch_extract_stats  # type: ignore[reportPrivateUsage]
        assert cached_stats is not None
        assert cached_stats["cache_hits"] == 3

    def test_extract_from_chembl_handles_request_error(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        tmp_path: Path,
    ) -> None:
        """Network failures should produce fallback records."""

        pipeline_config_fixture.paths.cache_root = str(tmp_path)
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "33"  # type: ignore[reportPrivateUsage]

        dataset = pd.DataFrame({"activity_id": [10, 11]})
        chembl_client_stub = MagicMock()

        iterator_client = MagicMock()
        iterator_client.paginate.side_effect = RequestException("boom")
        activity_iterator = ChemblActivityClient(iterator_client, batch_size=2)

        def passthrough(df: pd.DataFrame, *_: Any, **__: Any) -> pd.DataFrame:
            return df

        with (
            patch.object(pipeline, "_extract_data_validity_descriptions", side_effect=passthrough),
            patch.object(pipeline, "_extract_assay_fields", side_effect=passthrough),
            patch.object(pipeline, "_log_validity_comments_metrics"),
        ):
            result = pipeline._extract_from_chembl(
                dataset,
                chembl_client_stub,
                activity_iterator,
                select_fields=["activity_id"],
            )  # type: ignore[reportPrivateUsage]

        iterator_client.paginate.assert_called_once()
        assert list(result["activity_id"]) == [10, 11]
        assert all(result["data_validity_comment"].str.contains("Fallback"))  # type: ignore[reportUnknownMemberType]
        metadata = result["activity_properties"].apply(json.loads)  # type: ignore[reportUnknownMemberType]
        for item in metadata:
            assert isinstance(item, list)
            assert len(item) == 1  # type: ignore[reportUnknownArgumentType]
            payload: dict[str, object] = item[0]  # type: ignore[reportUnknownVariableType]
            assert payload["type"] == "fallback_metadata"
            text_value: object = payload["text_value"]  # type: ignore[reportUnknownVariableType]
            assert isinstance(text_value, str)
            details = json.loads(text_value)
            assert details["source_system"] == "ChEMBL_FALLBACK"
        stats = pipeline._last_batch_extract_stats  # type: ignore[reportPrivateUsage]
        assert stats is not None
        assert stats["fallback"] == 2
        assert stats["errors"] == 2

    def test_extract_from_chembl_handles_circuit_breaker(
        self,
        pipeline_config_fixture: PipelineConfig,
        run_id: str,
        tmp_path: Path,
    ) -> None:
        """Circuit breaker errors should also yield fallback records."""

        pipeline_config_fixture.paths.cache_root = str(tmp_path)
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "34"  # type: ignore[reportPrivateUsage]

        dataset = pd.DataFrame({"activity_id": [42]})
        chembl_client_stub = MagicMock()

        iterator_client = MagicMock()
        iterator_client.paginate.side_effect = CircuitBreakerOpenError("open")
        activity_iterator = ChemblActivityClient(iterator_client, batch_size=1)

        def passthrough(df: pd.DataFrame, *_: Any, **__: Any) -> pd.DataFrame:
            return df

        with (
            patch.object(pipeline, "_extract_data_validity_descriptions", side_effect=passthrough),
            patch.object(pipeline, "_extract_assay_fields", side_effect=passthrough),
            patch.object(pipeline, "_log_validity_comments_metrics"),
        ):
            result = pipeline._extract_from_chembl(
                dataset,
                chembl_client_stub,
                activity_iterator,
                select_fields=["activity_id"],
            )  # type: ignore[reportPrivateUsage]

        iterator_client.paginate.assert_called_once()
        assert result.shape[0] == 1
        assert "Fallback" in result["data_validity_comment"].iloc[0]
        stats = pipeline._last_batch_extract_stats  # type: ignore[reportPrivateUsage]
        assert stats is not None
        assert stats["fallback"] == 1
        assert stats["errors"] == 1

    def test_extract_activity_properties_fields_result_flag_priority(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that elements with result_flag==1 have priority over others."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "value": None,
            "text_value": None,
            "activity_properties": [
                {"type": "IC50", "value": 100.0, "text_value": "~100", "result_flag": 0},
                {"type": "IC50", "value": 50.0, "text_value": "~50%", "result_flag": 1},
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # The element with result_flag==1 must take precedence.
        assert result["value"] == 50.0
        assert result["text_value"] == "~50%"

    def test_extract_activity_properties_fields_preserves_existing_values(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that existing values in record are not overwritten by properties."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "value": 123.0,
            "relation": "=",
            "units": "nM",
            "activity_properties": [
                {"type": "IC50", "value": 200.0, "relation": "<", "units": "μM", "result_flag": 1},
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Existing values must remain unchanged.
        assert result["value"] == 123.0
        assert result["relation"] == "="
        assert result["units"] == "nM"

    def test_extract_activity_properties_fields_no_standard_fields(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that standard_* fields are never extracted from properties."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "standard_value": None,
            "standard_units": None,
            "standard_relation": None,
            "standard_type": None,
            "standard_text_value": None,
            "standard_upper_value": None,
            "activity_properties": [
                {
                    "type": "standard_IC50",
                    "value": 10.0,
                    "units": "nM",
                    "relation": "=",
                    "text_value": "standard",
                    "result_flag": 1,
                },
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Standardized fields must never be populated from properties.
        assert result["standard_value"] is None
        assert result["standard_units"] is None
        assert result["standard_relation"] is None
        assert result["standard_type"] is None
        assert result["standard_text_value"] is None
        assert result["standard_upper_value"] is None

    def test_extract_activity_properties_fields_data_validity_comment_fallback(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that data_validity_comment is extracted from properties as fallback when empty."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Scenario 1: fallback applies when data_validity_comment is empty.
        record = {
            "activity_id": 1,
            "data_validity_comment": None,
            "activity_comment": None,
            "activity_properties": [
                {
                    "type": "data_validity",
                    "value": "invalid",
                    "text_value": "Invalid data",
                    "result_flag": 1,
                },
                {
                    "type": "activity_comment",
                    "value": "comment",
                    "text_value": "Some comment",
                    "result_flag": 1,
                },
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # data_validity_comment should be read from properties as a fallback.
        assert result["data_validity_comment"] == "Invalid data"
        # activity_comment must not be populated from properties.
        assert result["activity_comment"] is None

        # Scenario 2: direct field value takes priority over fallback.
        record_with_direct = {
            "activity_id": 1,
            "data_validity_comment": "Direct comment",
            "activity_properties": [
                {
                    "type": "data_validity",
                    "text_value": "Fallback comment",
                    "result_flag": 1,
                },
            ],
        }

        result_direct = pipeline._extract_activity_properties_fields(record_with_direct)  # type: ignore[reportPrivateUsage]

        # Direct field value must remain; fallback should not apply.
        assert result_direct["data_validity_comment"] == "Direct comment"

    def test_extract_activity_properties_fields_data_validity_comment_priority(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that data_validity_comment fallback prioritizes elements with result_flag == 1."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Scenario: prioritize elements where result_flag == 1.
        record = {
            "activity_id": 1,
            "data_validity_comment": None,
            "activity_properties": [
                {
                    "type": "data_validity",
                    "text_value": "Non-measured comment",
                    "result_flag": 0,
                },
                {
                    "type": "data_validity",
                    "text_value": "Measured comment",
                    "result_flag": 1,
                },
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # The element with result_flag == 1 must be used.
        assert result["data_validity_comment"] == "Measured comment"

        # Scenario: if no elements have result_flag == 1, use the first entry.
        record_no_measured = {
            "activity_id": 1,
            "data_validity_comment": "",
            "activity_properties": [
                {
                    "type": "data_validity",
                    "text_value": "First comment",
                    "result_flag": 0,
                },
                {
                    "type": "data_validity",
                    "text_value": "Second comment",
                    "result_flag": 0,
                },
            ],
        }

        result_no_measured = pipeline._extract_activity_properties_fields(record_no_measured)  # type: ignore[reportPrivateUsage]

        # The first available element should be used.
        assert result_no_measured["data_validity_comment"] == "First comment"

        # Scenario: whitespace-only strings count as empty values.
        record_empty_string = {
            "activity_id": 1,
            "data_validity_comment": "   ",
            "activity_properties": [
                {
                    "type": "data_validity",
                    "text_value": "Fallback from whitespace",
                    "result_flag": 1,
                },
            ],
        }

        result_empty_string = pipeline._extract_activity_properties_fields(record_empty_string)  # type: ignore[reportPrivateUsage]

        # A whitespace string is treated as empty, so fallback must apply.
        assert result_empty_string["data_validity_comment"] == "Fallback from whitespace"

    def test_extract_activity_properties_fields_data_validity_comment_value_only(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that data_validity_comment is extracted from value when text_value is missing."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Scenario: element provides only value (no text_value).
        record = {
            "activity_id": 1,
            "data_validity_comment": None,
            "activity_properties": [
                {
                    "type": "data_validity",
                    "value": "Manually validated",
                    "result_flag": 1,
                },
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # data_validity_comment should be sourced from the value.
        assert result["data_validity_comment"] == "Manually validated"

        # Scenario: text_value takes precedence over value when both exist.
        record_both = {
            "activity_id": 1,
            "data_validity_comment": None,
            "activity_properties": [
                {
                    "type": "data_validity",
                    "value": "Value only",
                    "text_value": "Text value preferred",
                    "result_flag": 1,
                },
            ],
        }

        result_both = pipeline._extract_activity_properties_fields(record_both)  # type: ignore[reportPrivateUsage]

        # text_value must outrank value.
        assert result_both["data_validity_comment"] == "Text value preferred"

        # Scenario: fall back to value when text_value is empty.
        record_empty_text = {
            "activity_id": 1,
            "data_validity_comment": None,
            "activity_properties": [
                {
                    "type": "data_validity",
                    "value": "Value fallback",
                    "text_value": "   ",
                    "result_flag": 1,
                },
            ],
        }

        result_empty_text = pipeline._extract_activity_properties_fields(record_empty_text)  # type: ignore[reportPrivateUsage]

        # When text_value is empty, value must be used.
        assert result_empty_text["data_validity_comment"] == "Value fallback"

    def test_extract_activity_properties_fields_invalid_json(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that invalid JSON in activity_properties returns record unchanged."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "value": 123.0,
            "activity_properties": "{invalid json}",
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Invalid JSON should leave the record untouched.
        assert result["value"] == 123.0
        assert result == record

    def test_extract_activity_properties_fields_not_list(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that non-list activity_properties returns record unchanged."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "value": 123.0,
            "activity_properties": {"not": "a list"},
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # A non-list must leave the record unchanged.
        assert result["value"] == 123.0
        assert result == record

    def test_extract_activity_properties_fields_coordinated_fallback(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that relation and units are pulled together with value from same element."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "value": None,
            "relation": None,
            "units": None,
            "activity_properties": [
                {
                    "type": "IC50",
                    "value": 10.0,
                    "relation": "=",
                    "units": "nM",
                    "result_flag": 1,
                },
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Filling value must also pull relation and units from the same element.
        assert result["value"] == 10.0
        assert result["relation"] == "="
        assert result["units"] == "nM"

    def test_extract_activity_properties_fields_text_value_coordinated_fallback(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that relation and units are pulled together with text_value from same element."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "text_value": None,
            "relation": None,
            "units": None,
            "activity_properties": [
                {
                    "type": "IC50",
                    "text_value": "~50%",
                    "relation": "<",
                    "units": "%",
                    "result_flag": 1,
                },
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Filling text_value must pull relation and units from the same element.
        assert result["text_value"] == "~50%"
        assert result["relation"] == "<"
        assert result["units"] == "%"

    def test_extract_activity_properties_fields_filters_invalid_items(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that only items with type and value/text_value are processed."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "value": None,
            "activity_properties": [
                {"type": "IC50", "value": 10.0},  # Valid
                {"type": "IC50"},  # No value or text_value - should be skipped
                {"value": 20.0},  # No type - should be skipped
                {"type": "IC50", "text_value": "~20"},  # Valid
                None,  # Not a mapping - should be skipped
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Only valid elements should be processed for fallback.
        assert result["value"] == 10.0  # First valid element.
        assert result["text_value"] == "~20"  # Second valid element.
        # activity_properties should be normalized and preserved.
        assert "activity_properties" in result
        if result["activity_properties"] is not None:
            props: list[dict[str, Any]] = result["activity_properties"]
            # All valid properties must remain.
            assert len(props) >= 2  # At least two valid entries.

    def test_deduplicate_activity_properties_exact_duplicates(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that exact duplicates are removed from activity_properties."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        properties = [
            {
                "type": "pH",
                "relation": "=",
                "units": None,
                "value": 7.4,
                "text_value": None,
                "result_flag": None,
            },
            {
                "type": "pH",
                "relation": "=",
                "units": None,
                "value": 7.4,
                "text_value": None,
                "result_flag": None,
            },  # Exact duplicate
            {
                "type": "Temperature",
                "relation": "=",
                "units": "C",
                "value": 37,
                "text_value": None,
                "result_flag": None,
            },
            {
                "type": "pH",
                "relation": "=",
                "units": None,
                "value": 7.4,
                "text_value": None,
                "result_flag": None,
            },  # Another exact duplicate
        ]

        deduplicated, stats = pipeline._deduplicate_activity_properties(  # type: ignore[reportPrivateUsage]
            properties, MagicMock(), activity_id=1
        )

        assert len(deduplicated) == 2  # Only 2 unique properties
        assert stats["duplicates_removed"] == 2
        assert stats["deduplicated_count"] == 2
        # Check that order is preserved (first occurrence kept)
        assert deduplicated[0]["type"] == "pH"
        assert deduplicated[1]["type"] == "Temperature"

    def test_validate_activity_properties_truv_invariant(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test TRUV validation: value and text_value cannot both be not None."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        properties = [
            {
                "type": "pH",
                "relation": "=",
                "units": None,
                "value": 7.4,
                "text_value": None,
                "result_flag": None,
            },  # Valid
            {
                "type": "Temperature",
                "relation": "=",
                "units": "C",
                "value": 37,
                "text_value": None,
                "result_flag": None,
            },  # Valid
            {
                "type": "Comment",
                "relation": None,
                "units": None,
                "value": None,
                "text_value": "test",
                "result_flag": None,
            },  # Valid
            {
                "type": "Invalid",
                "relation": "=",
                "units": None,
                "value": 10.0,
                "text_value": "also set",
                "result_flag": None,
            },  # Invalid: both set
            {
                "type": "Invalid2",
                "relation": "invalid",
                "units": None,
                "value": 5.0,
                "text_value": None,
                "result_flag": None,
            },  # Invalid: relation not in RELATIONS
        ]

        validated, stats = pipeline._validate_activity_properties_truv(  # type: ignore[reportPrivateUsage]
            properties, MagicMock(), activity_id=1
        )

        # All properties should be kept (no filtering, only logging)
        assert len(validated) == len(properties)
        assert stats["invalid_count"] == 2  # 2 invalid properties
        assert stats["valid_count"] == len(validated)

    def test_extract_activity_properties_missing_chEMBL_v24(  # noqa: N802
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test handling of missing activity_properties (ChEMBL < v24 compatibility)."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Record without activity_properties (ChEMBL < v24)
        record = {
            "activity_id": 1,
            "value": 10.0,
            "standard_value": 10.0,
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Should log warning and set activity_properties to None
        assert "activity_properties" in result
        assert result["activity_properties"] is None

    def test_extract_activity_properties_null_chEMBL_v24(  # noqa: N802
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test handling of null activity_properties (ChEMBL < v24 compatibility)."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Record with activity_properties = None (ChEMBL < v24)
        record = {
            "activity_id": 1,
            "value": 10.0,
            "standard_value": 10.0,
            "activity_properties": None,
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # Should log debug and return record unchanged
        assert "activity_properties" in result
        assert result["activity_properties"] is None

    def test_extract_activity_properties_preserves_all_properties(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that all properties are preserved without filtering."""
        pipeline = run.ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        record = {
            "activity_id": 1,
            "activity_properties": [
                {"type": "pH", "relation": "=", "units": None, "value": 7.4, "text_value": None},
                {
                    "type": "Temperature",
                    "relation": "=",
                    "units": "C",
                    "value": 37,
                    "text_value": None,
                },
                {
                    "type": "Invalid",
                    "relation": "=",
                    "units": None,
                    "value": 10.0,
                    "text_value": "also set",
                },  # Invalid but kept
            ],
        }

        result = pipeline._extract_activity_properties_fields(record)  # type: ignore[reportPrivateUsage]

        # All properties should be preserved (normalized and deduplicated, but not filtered)
        assert "activity_properties" in result
        assert result["activity_properties"] is not None
        props: list[dict[str, Any]] = result["activity_properties"]
        assert isinstance(props, list)
        # All 3 properties should be present (invalid one is kept but logged)
        assert len(props) == 3
