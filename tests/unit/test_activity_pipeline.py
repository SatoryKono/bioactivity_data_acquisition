"""Unit tests for ChemblActivityPipeline transformations."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pandas as pd
import pytest
from requests.exceptions import RequestException

from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.pipelines.chembl.activity import ChemblActivityPipeline


@pytest.mark.unit
class TestChemblActivityPipelineTransformations:
    """Test suite for ChemblActivityPipeline transformations."""

    def test_normalize_identifiers_valid(self, pipeline_config_fixture, run_id: str):
        """Test normalization of valid ChEMBL IDs."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["CHEMBL1", "chembl2", "CHEMBL3"],
                "assay_chembl_id": ["CHEMBL100", "chembl101", None],
                "target_chembl_id": ["CHEMBL200", None, "chembl201"],
            }
        )

        normalized = pipeline._normalize_identifiers(df, MagicMock())

        assert normalized["molecule_chembl_id"].iloc[0] == "CHEMBL1"
        assert normalized["molecule_chembl_id"].iloc[1] == "CHEMBL2"
        assert normalized["molecule_chembl_id"].iloc[2] == "CHEMBL3"

    def test_normalize_identifiers_invalid(self, pipeline_config_fixture, run_id: str):
        """Test normalization of invalid ChEMBL IDs."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["INVALID", "CHEMBL1", "not_chembl"],
                "assay_chembl_id": ["CHEMBL100", None, None],
            }
        )

        normalized = pipeline._normalize_identifiers(df, MagicMock())

        # Invalid IDs should be set to None
        assert pd.isna(normalized["molecule_chembl_id"].iloc[0])
        assert normalized["molecule_chembl_id"].iloc[1] == "CHEMBL1"
        assert pd.isna(normalized["molecule_chembl_id"].iloc[2])

    def test_normalize_identifiers_bao(self, pipeline_config_fixture, run_id: str):
        """Test normalization of BAO identifiers."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "bao_endpoint": ["BAO_0000001", "bao_0000002", "INVALID"],
                "bao_format": ["BAO_0000003", None, "not_bao"],
            }
        )

        normalized = pipeline._normalize_identifiers(df, MagicMock())

        assert normalized["bao_endpoint"].iloc[0] == "BAO_0000001"
        assert normalized["bao_endpoint"].iloc[1] == "BAO_0000002"
        assert pd.isna(normalized["bao_endpoint"].iloc[2])

    def test_normalize_measurements_standard_value(self, pipeline_config_fixture, run_id: str):
        """Test normalization of standard_value."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_value": ["10.5", "20.0", " 5.3 ", "10-20", "invalid"],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())

        assert normalized["standard_value"].iloc[0] == 10.5
        assert normalized["standard_value"].iloc[1] == 20.0
        assert normalized["standard_value"].iloc[2] == 5.3
        assert normalized["standard_value"].iloc[3] == 10.0  # First value from range
        assert pd.isna(normalized["standard_value"].iloc[4])  # Invalid becomes NaN

    def test_normalize_measurements_negative_values(self, pipeline_config_fixture, run_id: str):
        """Test that negative standard_values are set to None."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_value": [-10.0, 10.0, -5.5],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())

        assert pd.isna(normalized["standard_value"].iloc[0])
        assert normalized["standard_value"].iloc[1] == 10.0
        assert pd.isna(normalized["standard_value"].iloc[2])

    def test_normalize_measurements_standard_relation(self, pipeline_config_fixture, run_id: str):
        """Test normalization of standard_relation."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_relation": ["=", "<=", "≥", "≤", "invalid"],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())

        assert normalized["standard_relation"].iloc[0] == "="
        assert normalized["standard_relation"].iloc[1] == "<="
        assert normalized["standard_relation"].iloc[2] == ">="  # Unicode to ASCII
        assert normalized["standard_relation"].iloc[3] == "<="  # Unicode to ASCII
        assert pd.isna(normalized["standard_relation"].iloc[4])  # Invalid becomes None

    def test_normalize_measurements_standard_type(self, pipeline_config_fixture, run_id: str):
        """Test normalization of standard_type."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_type": ["IC50", "EC50", "invalid_type", None],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())

        assert normalized["standard_type"].iloc[0] == "IC50"
        assert normalized["standard_type"].iloc[1] == "EC50"
        assert pd.isna(normalized["standard_type"].iloc[2])  # Invalid becomes None
        assert pd.isna(normalized["standard_type"].iloc[3])  # None stays None

    def test_normalize_measurements_standard_units(self, pipeline_config_fixture, run_id: str):
        """Test normalization of standard_units."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "standard_units": ["nM", "nanomolar", "μM", "uM", "mM", "millimolar", "%", "percent"],
            }
        )

        normalized = pipeline._normalize_measurements(df, MagicMock())

        assert normalized["standard_units"].iloc[0] == "nM"
        assert normalized["standard_units"].iloc[1] == "nM"  # Normalized
        assert normalized["standard_units"].iloc[2] == "μM"
        assert normalized["standard_units"].iloc[3] == "μM"  # Normalized
        assert normalized["standard_units"].iloc[4] == "mM"
        assert normalized["standard_units"].iloc[5] == "mM"  # Normalized
        assert normalized["standard_units"].iloc[6] == "%"
        assert normalized["standard_units"].iloc[7] == "%"  # Normalized

    def test_normalize_string_fields(self, pipeline_config_fixture, run_id: str):
        """Test normalization of string fields."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "canonical_smiles": [" CCO ", "  ", "CCN"],
                "bao_label": ["Binding", "  ", None],
                "target_organism": ["homo sapiens", "mus musculus", None],
                "data_validity_comment": ["Comment", "", None],
            }
        )

        normalized = pipeline._normalize_string_fields(df, MagicMock())

        assert normalized["canonical_smiles"].iloc[0] == "CCO"
        assert pd.isna(normalized["canonical_smiles"].iloc[1])  # Empty becomes None
        assert normalized["canonical_smiles"].iloc[2] == "CCN"

        assert normalized["target_organism"].iloc[0] == "Homo Sapiens"  # Title case
        assert normalized["target_organism"].iloc[1] == "Mus Musculus"

    def test_normalize_nested_structures(self, pipeline_config_fixture, run_id: str):
        """Test normalization of nested structures to JSON strings."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "ligand_efficiency": [{"LE": 0.5}, None, '{"LE": 0.3}'],
                "activity_properties": [{"property": "value"}, None, None],
            }
        )

        normalized = pipeline._normalize_nested_structures(df, MagicMock())

        # Should be JSON strings
        assert isinstance(normalized["ligand_efficiency"].iloc[0], str)
        assert pd.isna(normalized["ligand_efficiency"].iloc[1])
        assert normalized["ligand_efficiency"].iloc[2] == '{"LE": 0.3}'

    def test_normalize_data_types(self, pipeline_config_fixture, run_id: str):
        """Test data type conversions."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": ["1", "2", "3"],
                "target_tax_id": ["9606", "10090", None],
                "standard_value": ["10.5", "20.0", None],
                "pchembl_value": ["7.98", None, "8.28"],
                "is_citation": [1, 0, "1"],
                "high_citation_rate": [True, False, None],
            }
        )

        normalized = pipeline._normalize_data_types(df, MagicMock())

        assert normalized["activity_id"].dtype.name == "Int64"
        assert normalized["target_tax_id"].dtype.name == "Int64"
        assert normalized["standard_value"].dtype.name == "float64"
        assert normalized["pchembl_value"].dtype.name == "float64"
        assert normalized["is_citation"].dtype.name == "bool"
        assert normalized["high_citation_rate"].dtype.name == "bool"

    def test_validate_foreign_keys(self, pipeline_config_fixture, run_id: str):
        """Test foreign key validation."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "molecule_chembl_id": ["CHEMBL1", "INVALID", "CHEMBL2"],
                "assay_chembl_id": ["CHEMBL100", "CHEMBL101", "invalid"],
            }
        )

        # Should not raise, but log warnings
        normalized = pipeline._validate_foreign_keys(df, MagicMock())

        assert len(normalized) == 3

    def test_check_activity_id_uniqueness(self, pipeline_config_fixture, run_id: str):
        """Test activity_id uniqueness check."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df_unique = pd.DataFrame({"activity_id": [1, 2, 3]})
        pipeline._check_activity_id_uniqueness(df_unique, MagicMock())
        # Should not raise

        df_duplicate = pd.DataFrame({"activity_id": [1, 1, 2]})
        with pytest.raises(ValueError, match="duplicate activity_id"):
            pipeline._check_activity_id_uniqueness(df_duplicate, MagicMock())

    def test_check_foreign_key_integrity(self, pipeline_config_fixture, run_id: str):
        """Test foreign key integrity check."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df_valid = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100", "CHEMBL101", None],
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", None],
            }
        )
        pipeline._check_foreign_key_integrity(df_valid, MagicMock())
        # Should not raise

        df_invalid = pd.DataFrame(
            {
                "assay_chembl_id": ["INVALID", "CHEMBL101", None],
                "molecule_chembl_id": ["CHEMBL1", "invalid", None],
            }
        )
        with pytest.raises(ValueError, match="Foreign key integrity check failed"):
            pipeline._check_foreign_key_integrity(df_invalid, MagicMock())

    def test_transform_empty_dataframe(self, pipeline_config_fixture, run_id: str):
        """Test transform with empty DataFrame."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df_empty = pd.DataFrame()
        result = pipeline.transform(df_empty)

        assert result.empty

    def test_transform_invalid_payload(self, pipeline_config_fixture, run_id: str):
        """Test transform with invalid payload type."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        result = pipeline.transform("invalid_payload")

        assert result.empty

    def test_transform_with_aliases(self, pipeline_config_fixture, run_id: str):
        """Test that transform creates aliases for sorting."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL100", "CHEMBL101"],
                "molecule_chembl_id": ["CHEMBL1", "CHEMBL2"],
            }
        )

        transformed = pipeline.transform(df)

        assert "assay_id" in transformed.columns
        assert "testitem_id" in transformed.columns
        assert transformed["assay_id"].equals(transformed["assay_chembl_id"])
        assert transformed["testitem_id"].equals(transformed["molecule_chembl_id"])

    def test_extract_from_chembl_batches_and_cache(
        self,
        pipeline_config_fixture,
        run_id: str,
        tmp_path,
    ) -> None:
        """Ensure batched extraction invokes the API and warms the cache."""

        pipeline_config_fixture.paths.cache_root = str(tmp_path)
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "33"

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

        result = pipeline._extract_from_chembl(dataset, client, batch_size=2)

        assert list(result["activity_id"]) == [1, 2, 3]
        stats = pipeline._last_batch_extract_stats
        assert stats is not None
        assert stats["api_calls"] == 2
        assert stats["cache_hits"] == 0

        cached_client = MagicMock()
        cached_result = pipeline._extract_from_chembl(dataset, cached_client, batch_size=2)

        assert list(cached_result["activity_id"]) == [1, 2, 3]
        assert cached_client.get.call_count == 0
        cached_stats = pipeline._last_batch_extract_stats
        assert cached_stats is not None
        assert cached_stats["cache_hits"] == 3

    def test_extract_from_chembl_handles_request_error(
        self,
        pipeline_config_fixture,
        run_id: str,
        tmp_path,
    ) -> None:
        """Network failures should produce fallback records."""

        pipeline_config_fixture.paths.cache_root = str(tmp_path)
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "33"

        dataset = pd.DataFrame({"activity_id": [10, 11]})
        client = MagicMock()
        client.get.side_effect = RequestException("boom")

        result = pipeline._extract_from_chembl(dataset, client, batch_size=2)

        assert client.get.call_count == 1
        assert list(result["activity_id"]) == [10, 11]
        assert all(result["data_validity_comment"].str.contains("Fallback"))
        metadata = result["activity_properties"].apply(json.loads)
        assert all(item["source_system"] == "ChEMBL_FALLBACK" for item in metadata)
        stats = pipeline._last_batch_extract_stats
        assert stats is not None
        assert stats["fallback"] == 2
        assert stats["errors"] == 2

    def test_extract_from_chembl_handles_circuit_breaker(
        self,
        pipeline_config_fixture,
        run_id: str,
        tmp_path,
    ) -> None:
        """Circuit breaker errors should also yield fallback records."""

        pipeline_config_fixture.paths.cache_root = str(tmp_path)
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        pipeline._chembl_release = "34"

        dataset = pd.DataFrame({"activity_id": [42]})
        client = MagicMock()
        client.get.side_effect = CircuitBreakerOpenError("open")

        result = pipeline._extract_from_chembl(dataset, client, batch_size=1)

        assert client.get.call_count == 1
        assert result.shape[0] == 1
        assert "Fallback" in result["data_validity_comment"].iloc[0]
        stats = pipeline._last_batch_extract_stats
        assert stats is not None
        assert stats["fallback"] == 1
        assert stats["errors"] == 1

