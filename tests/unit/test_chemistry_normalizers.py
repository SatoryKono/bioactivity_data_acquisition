"""Unit tests for chemistry normalizers."""

from bioetl.normalizers.chemistry import (
    ChemistryRelationNormalizer,
    ChemistryUnitsNormalizer,
)


class TestChemistryRelationNormalizer:
    """Tests for ChemistryRelationNormalizer."""

    def setup_method(self) -> None:
        self.normalizer = ChemistryRelationNormalizer()

    def test_normalize_relation(self) -> None:
        """Ensure synonyms are canonicalized."""

        assert self.normalizer.normalize("=") == "="
        assert self.normalizer.normalize("==") == "="
        assert self.normalizer.normalize("≤") == "<="
        assert self.normalizer.normalize("⩽") == "<="
        assert self.normalizer.normalize("≥") == ">="
        assert self.normalizer.normalize("⩾") == ">="

        assert self.normalizer.normalize(None) == "="
        assert self.normalizer.normalize("") == "="

        assert self.normalizer.normalize("unknown") == "="
        assert self.normalizer.normalize("unknown", default="~") == "~"

    def test_validate(self) -> None:
        """Unknown relations fall back to default but remain valid."""

        assert self.normalizer.validate("=") is True
        assert self.normalizer.validate("≤") is True
        assert self.normalizer.validate(None) is True
        assert self.normalizer.validate("unknown") is False


class TestChemistryUnitsNormalizer:
    """Tests for ChemistryUnitsNormalizer."""

    def setup_method(self) -> None:
        self.normalizer = ChemistryUnitsNormalizer()

    def test_normalize_units(self) -> None:
        """Check unit synonyms mapping and NA handling."""

        assert self.normalizer.normalize("nm") == "nM"
        assert self.normalizer.normalize("nanomolar") == "nM"
        assert self.normalizer.normalize("μm") == "µM"
        assert self.normalizer.normalize("um") == "µM"
        assert self.normalizer.normalize("μg/ml") == "µg/mL"

        assert self.normalizer.normalize(None) is None
        assert self.normalizer.normalize("") is None
        assert self.normalizer.normalize(None, default="nM") == "nM"

        assert self.normalizer.normalize("unknown") == "unknown"
        assert self.normalizer.normalize("kg") == "kg"

    def test_validate(self) -> None:
        """Units must be stringable and non-empty."""

        assert self.normalizer.validate("nM") is True
        assert self.normalizer.validate("unknown") is True
        assert self.normalizer.validate(None) is True
        assert self.normalizer.validate("") is False
