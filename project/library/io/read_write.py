"""I/O helpers for interacting with CSV artefacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from library.io.normalize import (
    PUBLICATION_COLUMNS,
    normalize_publication_frame,
    normalize_query_frame,
)


def read_queries(path: Path) -> pd.DataFrame:
    """Load a CSV containing search queries."""

    frame = pd.read_csv(path)
    return normalize_query_frame(frame)


def write_publications(df: pd.DataFrame, path: Path) -> None:
    """Persist publication records as a deterministic CSV."""

    normalized = normalize_publication_frame(df)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(path, index=False, encoding="utf-8", columns=PUBLICATION_COLUMNS)


def empty_publications_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the canonical publication columns."""

    return pd.DataFrame(columns=PUBLICATION_COLUMNS)
