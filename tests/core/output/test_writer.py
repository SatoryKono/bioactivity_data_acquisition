from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from bioetl.core.output import (
    AtomicFileWriter,
    DeterministicPathStrategy,
    YamlMetadataWriter,
)


@pytest.mark.unit
@pytest.mark.determinism
def test_atomic_csv_write_and_metadata(tmp_path: Path) -> None:
    run_id = "run-001"
    strategy = DeterministicPathStrategy(tmp_path, extension=".csv")
    metadata_writer = YamlMetadataWriter(run_id, path_strategy=strategy)
    writer = AtomicFileWriter(run_id, strategy, metadata_writer=metadata_writer)

    df = pd.DataFrame({"b": [2, 1], "a": ["y", "x"]})

    result = writer.write("assays", df)

    output_path = strategy.resolve_path("assays", run_id)
    meta_path = strategy.resolve_metadata_path("assays", run_id)

    assert Path(result.output_uri) == output_path
    assert output_path.exists()
    assert meta_path.exists()

    # Проверяем сортировку столбцов и строк
    csv_content = output_path.read_text(encoding="utf-8").splitlines()
    assert csv_content == ["a,b", "x,1", "y,2"]

    metadata = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    assert metadata["run_id"] == run_id
    assert metadata["rows"] == 2
    assert metadata["columns"] == ["a", "b"]
    assert "data_sha256" in metadata
    assert "metadata_sha256" in result.metadata
    assert not any(".tmp" in p.name for p in tmp_path.rglob("*"))


@pytest.mark.unit
@pytest.mark.determinism
def test_idempotent_repeated_write(tmp_path: Path) -> None:
    run_id = "run-002"
    strategy = DeterministicPathStrategy(tmp_path, extension=".csv")
    metadata_writer = YamlMetadataWriter(run_id, path_strategy=strategy)
    writer = AtomicFileWriter(run_id, strategy, metadata_writer=metadata_writer)

    df = pd.DataFrame({"col": [3, 1, 2]})

    writer.write("targets", df)
    output_path = strategy.resolve_path("targets", run_id)
    meta_path = strategy.resolve_metadata_path("targets", run_id)

    first_bytes = output_path.read_bytes()
    first_meta = meta_path.read_bytes()

    writer.write("targets", df)

    assert output_path.read_bytes() == first_bytes
    assert meta_path.read_bytes() == first_meta


@pytest.mark.unit
@pytest.mark.determinism
def test_existing_file_preserved_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_id = "run-003"
    strategy = DeterministicPathStrategy(tmp_path, extension=".csv")
    writer = AtomicFileWriter(run_id, strategy)

    df = pd.DataFrame({"x": [1, 2]})
    output_path = strategy.resolve_path("activities", run_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("x\n1\n2\n", encoding="utf-8")

    def fail_to_csv(self: pd.DataFrame, path: Path, **_: object) -> None:  # type: ignore[override]
        raise RuntimeError("forced failure")

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_to_csv)

    with pytest.raises(RuntimeError):
        writer.write("activities", df)

    assert output_path.read_text(encoding="utf-8") == "x\n1\n2\n"
    assert not any(".tmp" in p.name for p in output_path.parent.rglob("*"))
