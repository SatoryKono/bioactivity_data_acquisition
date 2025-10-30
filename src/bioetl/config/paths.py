"""Helpers for locating packaged configuration resources."""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path

CONFIGS_ENV_VAR = "BIOETL_CONFIGS_ROOT"


def get_configs_root() -> Path:
    """Return the root directory hosting bundled configuration resources."""

    override = os.getenv(CONFIGS_ENV_VAR)
    if override:
        root = Path(override).expanduser().resolve()
        if not root.exists():  # pragma: no cover - defensive guard
            raise FileNotFoundError(
                f"Configured {CONFIGS_ENV_VAR} directory does not exist: {root}"
            )
        return root

    package_root = resources.files("bioetl") / "configs"
    try:
        return Path(package_root)
    except TypeError as exc:  # pragma: no cover - zipimport fallback
        raise RuntimeError(
            "Configuration resources are not available as filesystem paths. "
            f"Set the {CONFIGS_ENV_VAR} environment variable to point to a directory "
            "containing the configuration bundle."
        ) from exc


def get_config_path(relative_path: str | Path) -> Path:
    """Resolve ``relative_path`` against the packaged configuration root."""

    relative = Path(relative_path)
    if relative.is_absolute():
        return relative
    return (get_configs_root() / relative).resolve()


def resolve_config_path(pathish: str | os.PathLike[str] | Path) -> Path:
    """Resolve a configuration reference to an absolute filesystem path."""

    path = Path(pathish)
    if path.is_absolute():
        return path

    parts = path.parts
    if parts and parts[0] == "configs":
        path = Path(*parts[1:])
    return get_config_path(path)
