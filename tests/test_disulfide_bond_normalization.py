"""Tests for disulfide_bond field normalization in target postprocessing."""

import pandas as pd
import pytest

from library.pipelines.target.postprocessing import _pipe_merge, align_target_columns


class TestDisulfideBondNormalization:
    """Test suite for disulfide_bond normalization via _pipe_merge."""

    @pytest.mark.parametrize(
        ("input_value", "expected"),
        [
            ("", ""),  # empty
            ("|", ""),  # single delimiter
            ("||", ""),  # double delimiter
            ("|||||", ""),  # five delimiters
            ("||||||||||||||||||||||||", ""),  # 24 delimiters
            ("|||", ""),  # triple delimiter
            ("a||||b", "a|b"),  # tokens with multiple delimiters
            ("a|||b|||c", "a|b|c"),  # tokens with triple delimiters
            (
                "|||||||interchain (between light and heavy chains)|||",
                "interchain (between light and heavy chains)",
            ),
            ("||||||||||||||||interchain||", "interchain"),
            (
                "|||||||||||interchain (between a and b chains)|||||",
                "interchain (between a and b chains)",
            ),
            (" a | b | c ", "a|b|c"),  # spaces around tokens
            ("a||b|c|||d", "a|b|c|d"),  # mixed delimiters
        ],
    )
    def test_pipe_merge_removes_empty_tokens(self, input_value: str, expected: str) -> None:
        """Verify _pipe_merge correctly removes empty tokens and extra delimiters."""
        result = _pipe_merge([input_value])
        assert result == expected

    def test_align_target_columns_normalizes_disulfide_bond(self) -> None:
        """Test that align_target_columns normalizes disulfide_bond field."""
        # Create test DataFrame with problematic disulfide_bond values
        test_df = pd.DataFrame(
            {
                "target_chembl_id": ["T1", "T2", "T3", "T4", "T5"],
                "disulfide_bond": [
                    "|||||",
                    "||||||||||||||||||||||||",
                    "|||||||interchain (between light and heavy chains)|||",
                    "||||||||||||||||interchain||",
                    "-",
                ],
            }
        )

        # Apply alignment (which includes normalization)
        result = align_target_columns(test_df)

        # Verify disulfide_bond column exists and is normalized
        assert "disulfide_bond" in result.columns

        # Check that multiple pipes are removed
        has_double_pipes = (
            result["disulfide_bond"]
            .astype(str)
            .str.contains(r"\|\|", regex=True, na=False)
            .any()
        )
        assert not has_double_pipes, "disulfide_bond should not contain || after normalization"

        # Check specific values
        expected_values = [
            "-",  # ||||| → empty → -
            "-",  # 24 pipes → empty → -
            "interchain (between light and heavy chains)",
            "interchain",
            "-",
        ]
        actual = result["disulfide_bond"].tolist()
        assert actual == expected_values

    def test_disulfide_bond_empty_values_replaced_with_dash(self) -> None:
        """Test that empty disulfide_bond values are replaced with '-'."""
        test_df = pd.DataFrame(
            {
                "target_chembl_id": ["T1", "T2", "T3"],
                "disulfide_bond": ["", None, "|"],
            }
        )

        result = align_target_columns(test_df)

        # All empty/None/pipe-only values should become '-'
        assert (result["disulfide_bond"] == "-").all()

