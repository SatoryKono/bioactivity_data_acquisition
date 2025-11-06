"""Unit tests for ChemblActivityPipeline transformations."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from requests.exceptions import RequestException

from bioetl.config import PipelineConfig
from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.pipelines.activity.activity import ChemblActivityPipeline


@pytest.mark.unit
class TestChemblActivityPipelineTransformations:
    """Test suite for ChemblActivityPipeline transformations."""

    def test_normalize_identifiers_valid(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of valid ChEMBL IDs."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_relation": ["=", "<=", "≥", "≤", "invalid"],
                "relation": ["=", " ≤", "≥", "invalid", None],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["standard_relation"].iloc[0] == "="
        assert normalized["standard_relation"].iloc[1] == "<="
        assert normalized["standard_relation"].iloc[2] == ">="  # Unicode to ASCII
        assert normalized["standard_relation"].iloc[3] == "<="  # Unicode to ASCII
        assert pd.isna(normalized["standard_relation"].iloc[4])  # Invalid becomes None

        assert normalized["relation"].iloc[0] == "="
        assert normalized["relation"].iloc[1] == "<="
        assert normalized["relation"].iloc[2] == ">="
        assert pd.isna(normalized["relation"].iloc[3])
        assert pd.isna(normalized["relation"].iloc[4])

    def test_normalize_measurements_standard_type(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of standard_type."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_type": ["IC50", "EC50", "invalid_type", None],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert normalized["standard_type"].iloc[0] == "IC50"
        assert normalized["standard_type"].iloc[1] == "EC50"
        assert pd.isna(normalized["standard_type"].iloc[2])  # Invalid becomes None
        assert pd.isna(normalized["standard_type"].iloc[3])  # None stays None

    def test_normalize_measurements_standard_units(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test normalization of standard_units."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_units": ["nM", "nanomolar", "μM", "uM", "mM", "millimolar", "%", "percent"],
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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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

        normalized = pipeline._normalize_data_types(df, MagicMock())  # type: ignore[reportPrivateUsage]

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df_empty = pd.DataFrame()
        result = pipeline.transform(df_empty)

        assert result.empty

    def test_transform_invalid_payload(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test transform with invalid payload type."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # transform expects pd.DataFrame, so passing string should raise TypeError or AttributeError
        with pytest.raises((TypeError, AttributeError)):
            pipeline.transform("invalid_payload")  # type: ignore[arg-type]

    def test_transform_harmonizes_identifier_columns(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that transform harmonizes identifier columns and drops aliases."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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

        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

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
        assert transformed.loc[0, "relation"] == "<="
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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "33"  # type: ignore[reportPrivateUsage]

        dataset = pd.DataFrame({"activity_id": [1, 2, 3]})

        client = MagicMock()
        response_batch_one = MagicMock()
        response_batch_one.json.return_value = {
            "activities": [
                {"activity_id": 1, "standard_type": "IC50"},
                {"activity_id": 2, "standard_type": "IC50"},
            ]
        }
        response_batch_two = MagicMock()
        response_batch_two.json.return_value = {
            "activities": [{"activity_id": 3, "standard_type": "IC50"}]
        }
        client.get.side_effect = [response_batch_one, response_batch_two]

        result = pipeline._extract_from_chembl(dataset, client, batch_size=2)  # type: ignore[reportPrivateUsage]

        assert list(result["activity_id"]) == [1, 2, 3]
        stats = pipeline._last_batch_extract_stats  # type: ignore[reportPrivateUsage]
        assert stats is not None
        assert stats["api_calls"] == 2
        assert stats["cache_hits"] == 0

        cached_client = MagicMock()
        cached_result = pipeline._extract_from_chembl(dataset, cached_client, batch_size=2)  # type: ignore[reportPrivateUsage]

        assert list(cached_result["activity_id"]) == [1, 2, 3]
        assert cached_client.get.call_count == 0
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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "33"  # type: ignore[reportPrivateUsage]

        dataset = pd.DataFrame({"activity_id": [10, 11]})
        client = MagicMock()
        client.get.side_effect = RequestException("boom")

        result = pipeline._extract_from_chembl(dataset, client, batch_size=2)  # type: ignore[reportPrivateUsage]

        assert client.get.call_count == 1
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
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "34"  # type: ignore[reportPrivateUsage]

        dataset = pd.DataFrame({"activity_id": [42]})
        client = MagicMock()
        client.get.side_effect = CircuitBreakerOpenError("open")

        result = pipeline._extract_from_chembl(dataset, client, batch_size=1)  # type: ignore[reportPrivateUsage]

        assert client.get.call_count == 1
        assert result.shape[0] == 1
        assert "Fallback" in result["data_validity_comment"].iloc[0]
        stats = pipeline._last_batch_extract_stats  # type: ignore[reportPrivateUsage]
        assert stats is not None
        assert stats["fallback"] == 1
        assert stats["errors"] == 1

