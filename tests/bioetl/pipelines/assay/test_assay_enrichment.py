"""Unit tests for assay enrichment functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients import ChemblClient
from bioetl.pipelines.chembl.assay.normalize import enrich_with_assay_parameters


@pytest.mark.unit
class TestAssayParametersEnrichment:
    """Test suite for assay_parameters enrichment."""

    def test_enrich_with_all_truv_fields(self) -> None:
        """Test enrichment with all TRUV fields (type, relation, value, units, text_value)."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        mock_client.fetch_assay_parameters_by_assay_ids.return_value = {
            "CHEMBL1": [
                {
                    "type": "TEMPERATURE",
                    "relation": "=",
                    "value": 37.0,
                    "units": "°C",
                    "text_value": None,
                },
                {
                    "type": "CONDITION",
                    "relation": None,
                    "value": None,
                    "units": None,
                    "text_value": "pH 7.4",
                },
            ],
        }

        cfg = {
            "fields": [
                "assay_chembl_id",
                "type",
                "relation",
                "value",
                "units",
                "text_value",
            ],
            "page_limit": 1000,
            "active_only": True,
        }

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        assert "assay_parameters" in result.columns
        assert result["assay_parameters"].iloc[0] is not pd.NA

        import json

        params = json.loads(result["assay_parameters"].iloc[0])
        assert len(params) == 2
        assert params[0]["type"] == "TEMPERATURE"
        assert params[0]["value"] == 37.0
        assert params[0]["text_value"] is None
        assert params[1]["text_value"] == "pH 7.4"
        assert params[1]["value"] is None

    def test_enrich_with_standard_fields(self) -> None:
        """Test enrichment with standard_* fields."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        mock_client.fetch_assay_parameters_by_assay_ids.return_value = {
            "CHEMBL1": [
                {
                    "type": "TEMPERATURE",
                    "relation": "=",
                    "value": 37.0,
                    "units": "°C",
                    "text_value": None,
                    "standard_type": "TEMPERATURE",
                    "standard_relation": "=",
                    "standard_value": 310.15,
                    "standard_units": "K",
                    "standard_text_value": None,
                    "active": 1,
                },
            ],
        }

        cfg = {
            "fields": [
                "assay_chembl_id",
                "type",
                "relation",
                "value",
                "units",
                "text_value",
                "standard_type",
                "standard_relation",
                "standard_value",
                "standard_units",
                "standard_text_value",
                "active",
            ],
            "page_limit": 1000,
            "active_only": True,
        }

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        import json

        params = json.loads(result["assay_parameters"].iloc[0])
        assert params[0]["standard_type"] == "TEMPERATURE"
        assert params[0]["standard_value"] == 310.15
        assert params[0]["standard_units"] == "K"
        assert params[0]["active"] == 1

    def test_enrich_with_optional_normalization_fields(self) -> None:
        """Test enrichment with optional normalization fields (type_normalized, type_fixed)."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        mock_client.fetch_assay_parameters_by_assay_ids.return_value = {
            "CHEMBL1": [
                {
                    "type": "TEMP",
                    "type_normalized": "TEMPERATURE",
                    "type_fixed": "TEMPERATURE",
                    "relation": "=",
                    "value": 37.0,
                    "units": "°C",
                },
            ],
        }

        cfg = {
            "fields": [
                "assay_chembl_id",
                "type",
                "type_normalized",
                "type_fixed",
                "relation",
                "value",
                "units",
            ],
            "page_limit": 1000,
            "active_only": True,
        }

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        import json

        params = json.loads(result["assay_parameters"].iloc[0])
        assert params[0]["type"] == "TEMP"
        assert params[0]["type_normalized"] == "TEMPERATURE"
        assert params[0]["type_fixed"] == "TEMPERATURE"

    def test_enrich_empty_dataframe(self) -> None:
        """Test enrichment with empty DataFrame."""
        df = pd.DataFrame()

        mock_client = MagicMock(spec=ChemblClient)
        cfg = {"fields": ["assay_chembl_id", "type"], "page_limit": 1000, "active_only": True}

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        assert result.empty
        mock_client.fetch_assay_parameters_by_assay_ids.assert_not_called()

    def test_enrich_no_valid_ids(self) -> None:
        """Test enrichment with no valid assay_chembl_id values."""
        df = pd.DataFrame({"assay_chembl_id": [None, pd.NA, ""]})

        mock_client = MagicMock(spec=ChemblClient)
        cfg = {"fields": ["assay_chembl_id", "type"], "page_limit": 1000, "active_only": True}

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        assert "assay_parameters" in result.columns
        mock_client.fetch_assay_parameters_by_assay_ids.assert_not_called()

    def test_enrich_no_parameters(self) -> None:
        """Test enrichment when no parameters are found."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        mock_client.fetch_assay_parameters_by_assay_ids.return_value = {}

        cfg = {"fields": ["assay_chembl_id", "type"], "page_limit": 1000, "active_only": True}

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        assert "assay_parameters" in result.columns
        assert pd.isna(result["assay_parameters"].iloc[0])

    def test_enrich_preserves_original_values(self) -> None:
        """Test that enrichment preserves original values and doesn't copy to standard_*."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        # Параметр с исходными значениями, но без standard_*
        mock_client.fetch_assay_parameters_by_assay_ids.return_value = {
            "CHEMBL1": [
                {
                    "type": "TEMPERATURE",
                    "relation": "=",
                    "value": 37.0,
                    "units": "°C",
                    "text_value": None,
                    # Нет standard_* полей
                },
            ],
        }

        cfg = {
            "fields": [
                "assay_chembl_id",
                "type",
                "relation",
                "value",
                "units",
                "text_value",
                "standard_type",
                "standard_value",
            ],
            "page_limit": 1000,
            "active_only": True,
        }

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        import json

        params = json.loads(result["assay_parameters"].iloc[0])
        # Исходные значения должны быть сохранены
        assert params[0]["type"] == "TEMPERATURE"
        assert params[0]["value"] == 37.0
        # standard_* должны быть None (не копируются автоматически)
        assert params[0].get("standard_type") is None
        assert params[0].get("standard_value") is None

    def test_enrich_multiple_assays(self) -> None:
        """Test enrichment with multiple assays."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1", "CHEMBL2"]})

        mock_client = MagicMock(spec=ChemblClient)
        mock_client.fetch_assay_parameters_by_assay_ids.return_value = {
            "CHEMBL1": [{"type": "TEMPERATURE", "value": 37.0, "units": "°C"}],
            "CHEMBL2": [{"type": "pH", "value": 7.4, "units": None}],
        }

        cfg = {
            "fields": ["assay_chembl_id", "type", "value", "units"],
            "page_limit": 1000,
            "active_only": True,
        }

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        import json

        assert result["assay_parameters"].iloc[0] is not pd.NA
        assert result["assay_parameters"].iloc[1] is not pd.NA

        params1 = json.loads(result["assay_parameters"].iloc[0])
        params2 = json.loads(result["assay_parameters"].iloc[1])

        assert params1[0]["type"] == "TEMPERATURE"
        assert params2[0]["type"] == "pH"
