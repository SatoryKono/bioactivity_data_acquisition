from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from infrastructure.runtime.load_meta_writer import LoadMetaWriter


def test_write_pandas_parquet(tmp_path: Path) -> None:
    writer = LoadMetaWriter()
    frame = pd.DataFrame([{"value": 1}])
    target = tmp_path / "data" / "item.parquet"

    writer.write(frame, target)

    assert target.exists()
    loaded = pd.read_parquet(target)
    assert loaded.at[0, "value"] == 1


def test_write_pandas_missing_dependency_logs_warning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    writer = LoadMetaWriter()
    frame = pd.DataFrame([{"value": 2}])
    target = tmp_path / "data" / "item.parquet"

    def _raise_import_error(*_: object, **__: object) -> None:
        raise ImportError("pyarrow not available")

    monkeypatch.setattr(frame, "to_parquet", _raise_import_error)

    writer.write(frame, target)

    assert not target.exists()
