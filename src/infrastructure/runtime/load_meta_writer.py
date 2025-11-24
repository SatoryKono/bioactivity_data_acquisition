"""Infrastructure writer for chembl_metadata_schema datasets."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from infrastructure.logging import UnifiedLogger

__all__ = ["LoadMetaWriter"]


class LoadMetaWriter:
    def __init__(
        self,
        *,
        dataset_format: str = "parquet",
        logger: UnifiedLogger | None = None,
    ) -> None:
        if dataset_format not in {"parquet", "delta"}:
            msg = f"Unsupported dataset format: {dataset_format}"
            raise ValueError(msg)
        self._dataset_format = dataset_format
        self._logger = logger or UnifiedLogger.get(__name__)

    def write(self, frame: Any, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(frame, pd.DataFrame):
            self._write_pandas(frame, path)
            return
        _write_spark_dataframe(frame, path, fmt=self._dataset_format)

    def _write_pandas(self, frame: pd.DataFrame, path: Path) -> None:
        if self._dataset_format != "parquet":
            msg = "Only parquet format is supported for pandas DataFrames"
            raise RuntimeError(msg)
        suffix = path.suffix or ".parquet"
        with tempfile.NamedTemporaryFile("wb", suffix=suffix, delete=False, dir=path.parent) as handle:
            temp_path = Path(handle.name)
        try:
            frame.to_parquet(temp_path, index=False)
            os.replace(temp_path, path)
        except ImportError as exc:  # pragma: no cover - optional dependency guard
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            self._logger.warning(
                "load_meta_parquet_unavailable",
                path=str(path),
                dataset_format=self._dataset_format,
                error=str(exc),
            )
        except Exception:  # pragma: no cover - cleanup branch
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise


# Optional Spark support helpers -------------------------------------------------

if TYPE_CHECKING:  # pragma: no cover - typing aid
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore[import-not-found]
else:  # pragma: no cover - optional dependency
    try:
        from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore[import-not-found]
    except Exception:
        SparkDataFrame = None  # type: ignore[assignment]


def _write_spark_dataframe(frame: Any, path: Path, *, fmt: str) -> None:
    if SparkDataFrame is None or not isinstance(frame, SparkDataFrame):
        msg = "Spark DataFrame support is unavailable"
        raise RuntimeError(msg)
    temp_dir = Path(tempfile.mkdtemp(prefix="load_meta_", dir=str(path.parent)))
    try:
        writer = frame.write.mode("overwrite")
        if fmt == "delta":
            writer.format("delta").save(str(temp_dir))
        else:
            writer.parquet(str(temp_dir))
        if path.exists():
            shutil.rmtree(path)
        os.replace(temp_dir, path)
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
