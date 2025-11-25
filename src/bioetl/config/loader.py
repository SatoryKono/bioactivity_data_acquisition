"""Configuration loader stub used by the orchestration layer."""
from __future__ import annotations

from pathlib import Path
from typing import Mapping, Any


def load_config(path: str | Path, *, section: str | None = None) -> Mapping[str, Any]:
    """Load pipeline configuration from a file path.

    This is a placeholder that documents the expected signature. Implementations
    should parse YAML/TOML and return a typed mapping, optionally selecting a
    specific section of the config for a named pipeline.
    """

    raise NotImplementedError("Runtime configuration loading is not implemented yet")
