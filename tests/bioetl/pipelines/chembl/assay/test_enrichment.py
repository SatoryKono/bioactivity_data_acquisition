"""Unit tests for assay enrichment functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.clients import ChemblClient
from bioetl.clients.client_exceptions import HTTPError
from bioetl.pipelines.chembl.assay.normalize import (
    enrich_with_assay_classifications,
    enrich_with_assay_parameters,
)


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
        # Parameter contains raw values but lacks standard_* fields.
        mock_client.fetch_assay_parameters_by_assay_ids.return_value = {
            "CHEMBL1": [
                {
                    "type": "TEMPERATURE",
                    "relation": "=",
                    "value": 37.0,
                    "units": "°C",
                    "text_value": None,
                    # No standard_* fields provided.
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
        # Original values must remain intact.
        assert params[0]["type"] == "TEMPERATURE"
        assert params[0]["value"] == 37.0
        # standard_* fields should remain None (they are not copied automatically).
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

    def test_enrich_parameters_handles_404_gracefully(self) -> None:
        """Test that 404 errors are handled gracefully and return DataFrame with NA values."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        # Simulate 404 HTTPError
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = HTTPError("404 Client Error: Not Found")
        http_error.response = mock_response
        mock_client.fetch_assay_parameters_by_assay_ids.side_effect = http_error

        cfg = {"fields": ["assay_chembl_id", "type"], "page_limit": 1000, "active_only": True}

        result = enrich_with_assay_parameters(df, mock_client, cfg)

        assert "assay_parameters" in result.columns
        assert pd.isna(result["assay_parameters"].iloc[0])

    def test_enrich_parameters_raises_non_404_errors(self) -> None:
        """Test that non-404 HTTP errors are re-raised."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        # Simulate 500 HTTPError
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = HTTPError("500 Internal Server Error")
        http_error.response = mock_response
        mock_client.fetch_assay_parameters_by_assay_ids.side_effect = http_error

        cfg = {"fields": ["assay_chembl_id", "type"], "page_limit": 1000, "active_only": True}

        with pytest.raises(HTTPError):
            enrich_with_assay_parameters(df, mock_client, cfg)


@pytest.mark.unit
class TestAssayClassificationsEnrichment:
    """Test suite for assay_classifications enrichment."""

    def test_enrich_classifications_handles_404_gracefully(self) -> None:
        """Test that 404 errors are handled gracefully for classifications enrichment."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        # Simulate 404 HTTPError for class_map fetch
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = HTTPError("404 Client Error: Not Found")
        http_error.response = mock_response
        mock_client.fetch_assay_class_map_by_assay_ids.side_effect = http_error

        cfg = {
            "class_map_fields": ["assay_chembl_id", "assay_class_id"],
            "classification_fields": ["assay_class_id", "l1", "l2"],
            "page_limit": 1000,
        }

        result = enrich_with_assay_classifications(df, mock_client, cfg)

        assert "assay_classifications" in result.columns
        assert "assay_class_id" in result.columns
        assert pd.isna(result["assay_classifications"].iloc[0])
        assert pd.isna(result["assay_class_id"].iloc[0])

    def test_enrich_classifications_handles_404_on_classification_fetch(self) -> None:
        """Test that 404 on classification fetch is handled gracefully."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        mock_client = MagicMock(spec=ChemblClient)
        # Return empty class_map to trigger classification fetch
        mock_client.fetch_assay_class_map_by_assay_ids.return_value = pd.DataFrame(
            columns=["assay_chembl_id", "assay_class_id"]
        )
        # Simulate 404 HTTPError for classification fetch
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_error = HTTPError("404 Client Error: Not Found")
        http_error.response = mock_response
        mock_client.fetch_assay_classifications_by_class_ids.side_effect = http_error

        cfg = {
            "class_map_fields": ["assay_chembl_id", "assay_class_id"],
            "classification_fields": ["assay_class_id", "l1", "l2"],
            "page_limit": 1000,
        }

        result = enrich_with_assay_classifications(df, mock_client, cfg)

        assert "assay_classifications" in result.columns
        assert "assay_class_id" in result.columns

    def test_enrich_classifications_raises_non_404_errors(self) -> None:
        """Test that non-404 HTTP errors are re-raised."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [[{"assay_class_id": "CLASS1"}]],
            }
        )

        mock_client = MagicMock(spec=ChemblClient)
        # Simulate 500 HTTPError
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_error = HTTPError("500 Internal Server Error")
        http_error.response = mock_response
        mock_client.fetch_assay_classifications_by_class_ids.side_effect = http_error

        cfg = {
            "classification_fields": ["assay_class_id", "l1", "l2"],
            "page_limit": 1000,
        }

        with pytest.raises(HTTPError):
            enrich_with_assay_classifications(df, mock_client, cfg)
