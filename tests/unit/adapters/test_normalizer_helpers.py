"""Tests for adapter normalizer helpers."""

import unittest
from unittest.mock import Mock, patch

from bioetl.adapters import _normalizer_helpers


class TestGetBibliographyNormalizers(unittest.TestCase):
    """Validate :func:`get_bibliography_normalizers`."""

    def setUp(self) -> None:
        _normalizer_helpers.get_bibliography_normalizers.cache_clear()

    def tearDown(self) -> None:
        _normalizer_helpers.get_bibliography_normalizers.cache_clear()

    def test_returns_cached_registry_entries(self) -> None:
        """Normalizers are fetched once and cached for reuse."""

        identifier = Mock(name="identifier_normalizer")
        string = Mock(name="string_normalizer")

        with patch.object(
            _normalizer_helpers.registry,
            "get",
            side_effect=[identifier, string],
        ) as registry_get:
            first = _normalizer_helpers.get_bibliography_normalizers()
            second = _normalizer_helpers.get_bibliography_normalizers()

        self.assertEqual(registry_get.call_count, 2)
        self.assertIs(first, second)
        self.assertTupleEqual(first, (identifier, string))

    def test_raises_when_identifier_missing(self) -> None:
        """Missing identifier normalizer surfaces the registry error."""

        with patch.object(
            _normalizer_helpers.registry, "get", side_effect=ValueError("identifier")
        ):
            with self.assertRaisesRegex(ValueError, "identifier"):
                _normalizer_helpers.get_bibliography_normalizers()

    def test_raises_when_string_missing(self) -> None:
        """Missing string normalizer surfaces the registry error."""

        identifier = Mock(name="identifier_normalizer")

        with patch.object(
            _normalizer_helpers.registry,
            "get",
            side_effect=[identifier, ValueError("string")],
        ):
            with self.assertRaisesRegex(ValueError, "string"):
                _normalizer_helpers.get_bibliography_normalizers()


if __name__ == "__main__":
    unittest.main()
