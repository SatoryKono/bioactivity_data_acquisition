"""Input/output helpers for deterministic pipeline operations."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig


def resolve_input_path(config: PipelineConfig, path: str | Path) -> Path:
    """Return the fully-resolved path for an input artefact.

    The helper accepts either absolute or relative paths. Relative paths are
    interpreted with respect to the pipeline's configured ``input_root``
    directory. When a caller passes a path that already includes the configured
    ``input_root`` prefix (for example ``data/input/activity.csv``) the helper
    honours it verbatim instead of duplicating the prefix.
    """

    candidate = Path(path)
    if candidate.is_absolute():
        return candidate

    input_root = Path(config.paths.input_root)
    if not input_root.is_absolute():
        input_root = (Path.cwd() / input_root).resolve(strict=False)
    else:
        input_root = input_root.resolve(strict=False)

    candidate_absolute = (Path.cwd() / candidate).resolve(strict=False)
    try:
        candidate_absolute.relative_to(input_root)
    except ValueError:
        resolved = (input_root / candidate).resolve(strict=False)
    else:
        resolved = candidate_absolute

    return resolved


def load_input_frame(
    config: PipelineConfig,
    path: str | Path,
    *,
    expected_columns: Sequence[str] | None = None,
    limit: int | None = None,
    sample: int | None = None,
    dtype: Any | None = None,
    **read_csv_kwargs: Any,
) -> pd.DataFrame:
    """Load an input CSV applying runtime sampling constraints.

    Parameters
    ----------
    config:
        The pipeline configuration used to resolve filesystem locations.
    path:
        Relative or absolute path to the CSV. Relative paths are scoped to the
        configured ``input_root``.
    expected_columns:
        Optional ordered collection describing the desired shape for empty
        frames. When provided and the file is missing or yields no records the
        helper returns an empty ``DataFrame`` with this column order.
    limit / sample:
        Optional record limits. ``sample`` acts as a backwards-compatible alias
        for ``limit`` and is ignored when ``limit`` is provided.
    dtype:
        Optional dtype hint forwarded to :func:`pandas.read_csv`.
    read_csv_kwargs:
        Additional keyword arguments passed directly to
        :func:`pandas.read_csv`.
    """

    resolved_path = resolve_input_path(config, path)
    if not resolved_path.exists():
        columns = list(expected_columns) if expected_columns is not None else []
        return pd.DataFrame(columns=columns)

    effective_limit: int | None = limit if limit is not None else sample
    if effective_limit is not None:
        try:
            effective_limit = int(effective_limit)
        except (TypeError, ValueError):
            effective_limit = None
        else:
            if effective_limit < 0:
                effective_limit = None

    read_csv_params = dict(read_csv_kwargs)
    use_head_fallback = False

    if effective_limit is not None and "nrows" not in read_csv_params:
        if "chunksize" in read_csv_params:
            # ``read_csv`` does not allow combining ``nrows`` with ``chunksize``.
            use_head_fallback = True
        else:
            read_csv_params["nrows"] = effective_limit

    try:
        dataframe = pd.read_csv(resolved_path, dtype=dtype, **read_csv_params)
    except TypeError as exc:  # pragma: no cover - compatibility for older pandas
        if (
            effective_limit is None
            or "nrows" not in read_csv_params
            or "nrows" not in str(exc)
        ):
            raise

        # Older pandas releases raise a ``TypeError`` when ``nrows`` is provided
        # alongside other arguments (notably ``chunksize``). When this happens we
        # retry without ``nrows`` and fall back to truncating the resulting
        # frame, matching the behaviour used prior to optimising this helper.
        read_csv_params.pop("nrows", None)
        dataframe = pd.read_csv(resolved_path, dtype=dtype, **read_csv_params)
        use_head_fallback = True

    if effective_limit is not None and use_head_fallback:
        dataframe = dataframe.head(effective_limit)

    if dataframe.empty and expected_columns is not None:
        return pd.DataFrame(columns=list(expected_columns))

    return dataframe


__all__ = ["load_input_frame", "resolve_input_path"]
