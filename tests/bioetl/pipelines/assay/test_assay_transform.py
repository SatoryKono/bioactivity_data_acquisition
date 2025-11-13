"""Unit tests for assay transform validation functions."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from bioetl.pipelines.chembl.assay.transform import validate_assay_parameters_truv


@pytest.mark.unit
class TestAssayParametersTruvValidation:
    """Test suite for TRUV invariant validation of assay_parameters."""

    def test_validate_truv_value_xor_text_value_valid(self) -> None:
        """Test that value XOR text_value invariant is satisfied."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {"type": "TEMPERATURE", "value": 37.0, "text_value": None},
                            {"type": "CONDITION", "value": None, "text_value": "pH 7.4"},
                        ],
                    ),
                ],
            },
        )

        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 1

    def test_validate_truv_value_xor_text_value_violation(self) -> None:
        """Test that violation of value XOR text_value invariant raises error."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {
                                "type": "TEMPERATURE",
                                "value": 37.0,
                                "text_value": "also present",
                            },
                        ],
                    ),
                ],
            },
        )

        with pytest.raises(ValueError, match="TRUV invariant violation"):
            validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

    def test_validate_truv_standard_value_xor_standard_text_value_valid(self) -> None:
        """Test that standard_value XOR standard_text_value invariant is satisfied."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {
                                "type": "TEMPERATURE",
                                "value": 37.0,
                                "standard_value": 310.15,
                                "standard_text_value": None,
                            },
                        ],
                    ),
                ],
            },
        )

        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 1

    def test_validate_truv_standard_value_xor_standard_text_value_violation(self) -> None:
        """Test that violation of standard_value XOR standard_text_value raises error."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {
                                "type": "TEMPERATURE",
                                "value": 37.0,
                                "standard_value": 310.15,
                                "standard_text_value": "also present",
                            },
                        ],
                    ),
                ],
            },
        )

        with pytest.raises(ValueError, match="TRUV invariant violation"):
            validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

    def test_validate_active_valid_values(self) -> None:
        """Test that active ∈ {0, 1, NULL} is satisfied."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {"type": "TEMPERATURE", "value": 37.0, "active": 1},
                            {"type": "pH", "value": 7.4, "active": 0},
                            {
                                "type": "CONDITION",
                                "value": None,
                                "text_value": "test",
                                "active": None,
                            },
                        ],
                    ),
                ],
            },
        )

        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 1

    def test_validate_active_invalid_value(self) -> None:
        """Test that invalid active value raises error."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {"type": "TEMPERATURE", "value": 37.0, "active": 2},
                        ],
                    ),
                ],
            },
        )

        with pytest.raises(ValueError, match="Invalid 'active' value"):
            validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

    def test_validate_relation_standard_operators(self) -> None:
        """Test that standard relation operators are accepted."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {"type": "TEMPERATURE", "value": 37.0, "relation": "="},
                            {"type": "pH", "value": 7.4, "relation": "<"},
                            {"type": "TIME", "value": 60, "relation": "≤"},
                            {"type": "DOSE", "value": 10, "relation": ">"},
                            {"type": "CONCENTRATION", "value": 1.0, "relation": "≥"},
                            {"type": "APPROX", "value": 5.0, "relation": "~"},
                            {"type": "NONE", "value": 0, "relation": None},
                        ],
                    ),
                ],
            },
        )

        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 1

    def test_validate_relation_non_standard_warning(self) -> None:
        """Test that non-standard relation operators generate warnings but don't fail."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {"type": "TEMPERATURE", "value": 37.0, "relation": "≈"},
                        ],
                    ),
                ],
            },
        )

        # Не должно выбрасывать ошибку, только предупреждение
        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 1

    def test_validate_empty_parameters(self) -> None:
        """Test validation with empty or missing parameters."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
                "assay_parameters": [None, "", json.dumps([])],
            },
        )

        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 3

    def test_validate_missing_column(self) -> None:
        """Test validation when column is missing."""
        df = pd.DataFrame({"assay_chembl_id": ["CHEMBL1"]})

        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 1

    def test_validate_invalid_json(self) -> None:
        """Test validation with invalid JSON."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": ["not valid json"],
            },
        )

        with pytest.raises(ValueError, match="Invalid JSON"):
            validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

    def test_validate_non_array_json(self) -> None:
        """Test validation with non-array JSON."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [json.dumps({"type": "TEMPERATURE"})],
            },
        )

        with pytest.raises(ValueError, match="must be a JSON array"):
            validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

    def test_validate_non_dict_parameter(self) -> None:
        """Test validation with non-dict parameter in array."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [json.dumps(["not a dict"])],
            },
        )

        with pytest.raises(ValueError, match="Parameter must be a dict"):
            validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

    def test_validate_fail_fast_false(self) -> None:
        """Test validation with fail_fast=False (warnings instead of errors)."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {
                                "type": "TEMPERATURE",
                                "value": 37.0,
                                "text_value": "also present",
                            },
                        ],
                    ),
                ],
            },
        )

        # Не должно выбрасывать ошибку, только логировать предупреждение
        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=False)

        assert len(result) == 1

    def test_validate_empty_strings_as_null(self) -> None:
        """Test that empty strings are treated as NULL for value/text_value."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {"type": "TEMPERATURE", "value": 37.0, "text_value": ""},
                            {"type": "CONDITION", "value": "", "text_value": "pH 7.4"},
                        ],
                    ),
                ],
            },
        )

        # Пустые строки должны считаться NULL, поэтому инвариант должен быть выполнен
        result = validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)

        assert len(result) == 1

    def test_validate_multiple_parameters_multiple_violations(self) -> None:
        """Test validation with multiple parameters and multiple violations."""
        df = pd.DataFrame(
            {
                "assay_chembl_id": ["CHEMBL1"],
                "assay_parameters": [
                    json.dumps(
                        [
                            {
                                "type": "TEMPERATURE",
                                "value": 37.0,
                                "text_value": "also present",  # Нарушение 1
                            },
                            {
                                "type": "pH",
                                "value": 7.4,
                                "standard_value": 7.4,
                                "standard_text_value": "also present",  # Нарушение 2
                            },
                            {
                                "type": "DOSE",
                                "value": 10,
                                "active": 2,  # Нарушение 3
                            },
                        ],
                    ),
                ],
            },
        )

        with pytest.raises(ValueError, match="TRUV validation failed"):
            validate_assay_parameters_truv(df, column="assay_parameters", fail_fast=True)
