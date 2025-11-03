"""Tests for hash generation functions."""

import hashlib
import json

import pandas as pd

from bioetl.core.hashing import generate_hash_business_key, generate_hash_row


class TestGenerateHashBusinessKey:
    """Tests for generate_hash_business_key function."""

    def test_hash_business_key_string(self):
        """Test hash generation from string key."""
        key = "CHEMBL123"
        hash_value = generate_hash_business_key(key)

        assert len(hash_value) == 64
        assert isinstance(hash_value, str)
        # Verify it's a valid hex string
        assert all(c in "0123456789abcdef" for c in hash_value)

    def test_hash_business_key_int(self):
        """Test hash generation from integer key."""
        key = 12345
        hash_value = generate_hash_business_key(key)

        assert len(hash_value) == 64
        assert isinstance(hash_value, str)
        # Verify it's a valid hex string
        assert all(c in "0123456789abcdef" for c in hash_value)

    def test_hash_business_key_determinism(self):
        """Test that same key produces same hash."""
        key = "CHEMBL456"
        hash1 = generate_hash_business_key(key)
        hash2 = generate_hash_business_key(key)

        assert hash1 == hash2

    def test_hash_business_key_uniqueness(self):
        """Test that different keys produce different hashes."""
        hash1 = generate_hash_business_key("CHEMBL123")
        hash2 = generate_hash_business_key("CHEMBL456")

        assert hash1 != hash2


class TestGenerateHashRow:
    """Tests for generate_hash_row function."""

    def test_hash_row_basic(self):
        """Test hash generation from basic row."""
        row = {
            "id": 1,
            "name": "test",
            "value": 3.14,
        }
        hash_value = generate_hash_row(row)

        assert len(hash_value) == 64
        assert isinstance(hash_value, str)
        assert all(c in "0123456789abcdef" for c in hash_value)

    def test_hash_row_determinism(self):
        """Test that same row produces same hash."""
        row = {
            "id": 1,
            "name": "test",
            "value": 3.14159,
        }
        hash1 = generate_hash_row(row)
        hash2 = generate_hash_row(row)

        assert hash1 == hash2

    def test_hash_row_order_independence(self):
        """Test that column order doesn't affect hash."""
        row1 = {"id": 1, "name": "test", "value": 3.14}
        row2 = {"value": 3.14, "name": "test", "id": 1}

        hash1 = generate_hash_row(row1)
        hash2 = generate_hash_row(row2)

        # Hashes should be the same because keys are sorted
        assert hash1 == hash2

    def test_hash_row_canonical_serialization(self):
        """Test that canonical JSON serialization is used."""
        # Manually create expected canonical JSON
        row = {"id": 1, "value": 3.14159}
        canonical_json = json.dumps(row, sort_keys=True, separators=(",", ":"))
        expected_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

        actual_hash = generate_hash_row(row)

        assert actual_hash == expected_hash

    def test_hash_row_float_precision(self):
        """Test that floats are rounded to 6 decimal places."""
        row1 = {"value": 3.141592653589793}
        row2 = {"value": 3.141593}  # Rounded to 6 decimals

        # Both should produce same hash because float is rounded
        hash1 = generate_hash_row(row1)
        hash2 = generate_hash_row(row2)

        assert hash1 == hash2

    def test_hash_row_datetime_isoformat(self):
        """Test that datetime is converted to ISO8601."""
        timestamp = pd.Timestamp("2024-01-01 12:00:00", tz="UTC")
        row = {"id": 1, "timestamp": timestamp}

        hash_value = generate_hash_row(row)

        # Verify it works without error
        assert len(hash_value) == 64

    def test_hash_row_none_exclusion(self):
        """Test that None values are excluded from hash."""
        row1 = {"id": 1, "name": "test", "value": None}
        row2 = {"id": 1, "name": "test"}

        hash1 = generate_hash_row(row1)
        hash2 = generate_hash_row(row2)

        # Hashes should be the same because None is excluded
        assert hash1 == hash2

    def test_hash_row_unique_different_rows(self):
        """Test that different rows produce different hashes."""
        row1 = {"id": 1, "name": "test1"}
        row2 = {"id": 2, "name": "test2"}

        hash1 = generate_hash_row(row1)
        hash2 = generate_hash_row(row2)

        assert hash1 != hash2

    def test_hash_row_complex_data_types(self):
        """Test hash generation with various data types."""
        row = {
            "id": 123,
            "name": "test",
            "value": 3.14159,
            "active": True,
            "timestamp": pd.Timestamp.now(tz="UTC"),
        }

        hash_value = generate_hash_row(row)

        assert len(hash_value) == 64
        assert isinstance(hash_value, str)


class TestHashLength:
    """Test that all generated hashes are exactly 64 characters."""

    def test_all_hashes_length_64(self):
        """Test that all hash generation functions produce 64-char strings."""
        # Business key hashes
        assert len(generate_hash_business_key("test")) == 64
        assert len(generate_hash_business_key(123)) == 64

        # Row hashes
        assert len(generate_hash_row({"id": 1})) == 64
        assert len(generate_hash_row({"id": 1, "value": 3.14, "name": "test"})) == 64
