from __future__ import annotations

import os
import random
from pathlib import Path
from typing import Iterable

import numpy as np


def enforce_determinism(seed: int = 0) -> None:
    """Set deterministic seeds and timezone for reproducible runs."""

    os.environ.setdefault("TZ", "UTC")
    random.seed(seed)
    np.random.seed(seed)


def compare_artifacts(
    new_dir: Path,
    golden_dir: Path,
    *,
    filenames: Iterable[str],
) -> dict[str, bool]:
    """Compare specific files between new and golden directories."""

    results: dict[str, bool] = {}
    for name in filenames:
        new_path = new_dir / name
        golden_path = golden_dir / name
        results[name] = new_path.read_bytes() == golden_path.read_bytes()
    return results


__all__ = ["compare_artifacts", "enforce_determinism"]
