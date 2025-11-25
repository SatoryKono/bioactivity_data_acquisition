from __future__ import annotations

from pathlib import Path

from bioetl.core.output import AtomicWriter, compute_file_hash


def test_atomic_writer_moves_into_final_dir(tmp_path: Path):
    writer = AtomicWriter(tmp_path, "run123")
    writer.prepare()
    writer.write_text("data.txt", "hello")

    assert not (tmp_path / "run123").exists()

    final_dir = writer.commit()

    assert final_dir.exists()
    assert (final_dir / "data.txt").read_text() == "hello"
    assert not writer.temp_dir.exists()
    assert compute_file_hash(final_dir / "data.txt")
