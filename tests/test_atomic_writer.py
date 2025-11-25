from __future__ import annotations

from pathlib import Path

from bioetl.core.io.output import AtomicWriter
from bioetl.core.io.artifacts import compute_file_hash


def test_atomic_write_atomic_rename(tmp_path: Path):
    target_path = tmp_path / "data.txt"

    writer = AtomicWriter(target_path)
    with writer as temp_writer:
        temp_writer.write_text("hello")

    assert target_path.exists()
    assert target_path.read_text(encoding="utf-8") == "hello"
    assert not writer.temp_path.exists()
    assert compute_file_hash(target_path)
