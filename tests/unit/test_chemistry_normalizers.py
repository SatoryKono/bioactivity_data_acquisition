"""Unit tests for chemistry normalizers."""

import pytest

from bioetl.normalizers import registry
from bioetl.normalizers.chemistry import (
    ChemistryBooleanFlagNormalizer,
    ChemistryRelationNormalizer,
    ChemistryUnitsNormalizer,
)


class TestChemistryRelationNormalizer:
    """Tests for ChemistryRelationNormalizer."""

    def setup_method(self) -> None:
        self.normalizer = ChemistryRelationNormalizer()

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("=", "="),
            ("==", "="),
            ("≤", "<="),
            ("≥", ">="),
            (None, "="),
            ("", "="),
        ],
    )
    def test_normalize(self, value, expected) -> None:
        """Normalize various inputs to canonical relations."""

        assert self.normalizer.normalize(value) == expected

    def test_normalize_with_default(self) -> None:
        """Fallback to provided default when relation is unknown."""

        assert self.normalizer.normalize("?", default="~") == "~"

    def test_registry_integration(self) -> None:
        """Ensure relation normalizer is available via registry."""

        assert registry.normalize("chemistry.relation", "⩽") == "<="


class TestChemistryUnitsNormalizer:
    """Tests for ChemistryUnitsNormalizer."""

    def setup_method(self) -> None:
        self.normalizer = ChemistryUnitsNormalizer()

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("nm", "nM"),
            ("nanomolar", "nM"),
            ("μg/ml", "µg/mL"),
            ("unknown", "unknown"),
        ],
    )
    def test_normalize_units(self, value, expected) -> None:
        """Normalize unit synonyms to canonical representation."""

        assert self.normalizer.normalize(value) == expected

    def test_normalize_units_with_default(self) -> None:
        """Return default value when source is NA."""

        assert self.normalizer.normalize(None, default="nM") == "nM"

    def test_registry_integration(self) -> None:
        """Ensure units normalizer is exposed via registry."""

        assert registry.normalize("chemistry.units", "µm") == "µM"


class TestChemistryBooleanFlagNormalizer:
    """Tests for ChemistryBooleanFlagNormalizer."""

    def setup_method(self) -> None:
        self.normalizer = ChemistryBooleanFlagNormalizer()

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (True, True),
            (False, False),
            (1, True),
            (0, False),
            ("yes", True),
            ("no", False),
        ],
    )
    def test_normalize_boolean_values(self, value, expected) -> None:
        """Normalize diverse boolean representations."""

        assert self.normalizer.normalize(value) is expected

    def test_normalize_with_default(self) -> None:
        """Return default for NA or indeterminate values."""

        assert self.normalizer.normalize(None, default=True) is True
        assert self.normalizer.normalize(0.5, default=False) is False

    def test_validate(self) -> None:
        """Delegated validation behaviour should mirror boolean normalizer."""

        assert self.normalizer.validate("true") is True
        assert self.normalizer.validate("maybe") is False

    def test_registry_integration(self) -> None:
        """Ensure boolean flag normalizer is available via registry."""

        assert registry.normalize("chemistry.boolean_flag", "false") is False
        assert registry.normalize("chemistry.boolean_flag", None, default=True) is True
