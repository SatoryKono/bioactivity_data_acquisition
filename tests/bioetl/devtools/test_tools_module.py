"""Tests for shared helpers exposed via :mod:`bioetl.tools`."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from bioetl.tools import hash_file


def test_hash_file_matches_hashlib(tmp_path: Path) -> None:
    sample = tmp_path / "sample.bin"
    sample.write_bytes(b"hello world")

    expected = hashlib.sha256(sample.read_bytes()).hexdigest()
    assert hash_file(sample) == expected


def test_hash_file_supports_custom_algorithm(tmp_path: Path) -> None:
    sample = tmp_path / "sample.bin"
    sample.write_bytes(b"payload")

    expected = hashlib.blake2b(sample.read_bytes()).hexdigest()
    assert hash_file(sample, algorithm="blake2b") == expected


def test_hash_file_rejects_unknown_algorithm(tmp_path: Path) -> None:
    sample = tmp_path / "sample.bin"
    sample.write_text("payload", encoding="utf-8")

    with pytest.raises(ValueError):
        hash_file(sample, algorithm="unknown")
