"""Unit tests for assay empty fields and array handling."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.config.models.models import PipelineMetadata
from bioetl.config.models.policies import HTTPClientConfig, HTTPConfig
from bioetl.config.models.models import TransformConfig
from bioetl.config.models.models import ValidationConfig
from bioetl.pipelines.assay.assay import ChemblAssayPipeline
from bioetl.pipelines.assay.assay_transform import header_rows_serialize


def _create_minimal_config() -> PipelineConfig:
    """Create minimal pipeline config for testing."""
    return PipelineConfig(
        version=1,
        pipeline=PipelineMetadata(name="assay_chembl", version="1.0.0"),
        transform=TransformConfig(
            arrays_to_header_rows=["assay_classifications", "assay_parameters"]
        ),
        sources={},
        http=HTTPConfig(default=HTTPClientConfig()),
        validation=ValidationConfig(schema_out="bioetl.schemas.assay.assay_chembl.AssaySchema"),
    )


@pytest.mark.unit
class TestEmptyFieldsAndArrays:
    """Test suite for empty fields and array serialization in assay pipeline."""

    def test_all_fields_null_or_empty(self) -> None:
        """Test that all fields NULL/[] serialize to empty strings."""
        items_empty_list: list[dict[str, str]] = []
        items_none = None

        result_empty = header_rows_serialize(items_empty_list)
        result_none = header_rows_serialize(items_none)

        assert result_empty == ""
        assert result_none == ""

    def test_assay_classifications_single_object(self) -> None:
        """Test serialization of single object in assay_classifications."""
        items = [{"a": "A", "b": "B"}]
        result = header_rows_serialize(items)
        assert result == "a|b/A|B"

    def test_assay_parameters_multiple_objects(self) -> None:
        """Test serialization of multiple objects in assay_parameters."""
        items = [{"a": "A1", "b": "B1"}, {"a": "A2", "b": "B2"}]
        result = header_rows_serialize(items)
        assert result == "a|b/A1|B1/A2|B2"

    def test_escaping_delimiters(self) -> None:
        """Test escaping of pipe and slash delimiters in values."""
        items = [{"x": "A|B", "y": "C/D"}]
        result = header_rows_serialize(items)
        assert result == "x|y/A\\|B|C\\/D"

    def test_extract_assay_class_id_from_classifications_with_assay_class_id_key(
        self,
    ) -> None:
        """Test extraction of assay_class_id from classifications using assay_class_id key."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [[{"assay_class_id": "BAO_0000015"}]],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        assert result["assay_class_id"].iloc[0] == "BAO_0000015"

    def test_extract_assay_class_id_from_classifications_with_class_id_key(
        self,
    ) -> None:
        """Test extraction of assay_class_id from classifications using class_id key."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [[{"class_id": "BAO_0000015"}]],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        assert result["assay_class_id"].iloc[0] == "BAO_0000015"

    def test_extract_assay_class_id_from_classifications_with_id_key(self) -> None:
        """Test extraction of assay_class_id from classifications using id key."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [[{"id": "BAO_0000015"}]],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        assert result["assay_class_id"].iloc[0] == "BAO_0000015"

    def test_extract_assay_class_id_from_classifications_with_bao_format_key(
        self,
    ) -> None:
        """Test extraction of assay_class_id from classifications using bao_format key."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [[{"bao_format": "BAO_0000015"}]],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        assert result["assay_class_id"].iloc[0] == "BAO_0000015"

    def test_aggregate_multiple_ids_with_semicolon(self) -> None:
        """Test aggregation of multiple IDs from classifications with semicolon."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [
                    [
                        {"assay_class_id": "BAO_0000015"},
                        {"class_id": "BAO_0000016"},
                        {"id": "BAO_0000017"},
                    ]
                ],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        # Should aggregate all IDs with semicolon
        assert result["assay_class_id"].iloc[0] == "BAO_0000015;BAO_0000016;BAO_0000017"

    def test_priority_order_for_id_keys(self) -> None:
        """Test that priority order is respected (assay_class_id > class_id > id > bao_format)."""
        # If multiple keys exist, should use the first one in priority order
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [
                    [
                        {
                            "assay_class_id": "BAO_0000015",
                            "class_id": "BAO_0000016",
                            "id": "BAO_0000017",
                            "bao_format": "BAO_0000018",
                        }
                    ]
                ],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        # Should use assay_class_id (first in priority)
        assert result["assay_class_id"].iloc[0] == "BAO_0000015"

    def test_empty_classifications_array(self) -> None:
        """Test that empty classifications array doesn't extract assay_class_id."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [[]],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        # Should remain None
        assert pd.isna(result["assay_class_id"].iloc[0])

    def test_normalize_non_canonical_bao_values(self) -> None:
        """Normalize various BAO identifier representations from classifications."""

        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [
                    [
                        {"assay_class_id": "bao_0000015"},
                        {"class_id": "BAO:0000016"},
                        {"id": "0000017"},
                        {"bao_format": "BAO_0000018"},
                    ]
                ],
                "assay_class_id": [None],
            }
        )

        pipeline = ChemblAssayPipeline(_create_minimal_config(), "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        assert (
            result["assay_class_id"].iloc[0]
            == "BAO_0000015;BAO_0000016;BAO_0000017;BAO_0000018"
        )

    def test_extract_from_nested_classifications(self) -> None:
        """Extract BAO identifiers from nested classification structures."""

        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [
                    [
                        {
                            "metadata": {
                                "children": [
                                    {"assay_class_id": "bao_0000015"},
                                    {"classifications": [{"id": "BAO:0000016"}]},
                                ]
                            }
                        }
                    ]
                ],
                "assay_class_id": [None],
            }
        )

        pipeline = ChemblAssayPipeline(_create_minimal_config(), "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        assert result["assay_class_id"].iloc[0] == "BAO_0000015;BAO_0000016"

    def test_deduplicate_bao_ids(self) -> None:
        """Deduplicate BAO identifiers extracted from classifications."""

        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [
                    [
                        {"assay_class_id": "BAO_0000015"},
                        {"class_id": "BAO:0000015"},
                        {"id": "0000015"},
                    ]
                ],
                "assay_class_id": [None],
            }
        )

        pipeline = ChemblAssayPipeline(_create_minimal_config(), "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        assert result["assay_class_id"].iloc[0] == "BAO_0000015"

    def test_none_classifications(self) -> None:
        """Test that None classifications doesn't extract assay_class_id."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_classifications": [None],
                "assay_class_id": [None],
            }
        )

        config = _create_minimal_config()
        pipeline = ChemblAssayPipeline(config, "test_run")
        mock_log = MagicMock()
        result = pipeline._normalize_nested_structures(df, mock_log)

        # Should remain None
        assert pd.isna(result["assay_class_id"].iloc[0])
