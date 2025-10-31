"""Helpers for locating packaged configuration resources."""

from __future__ import annotations

import contextlib
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

    package_candidates = ("bioetl.configs", "bioetl")
    for package_name in package_candidates:
        try:
            package_root = resources.files(package_name)
        except ModuleNotFoundError:  # pragma: no cover - defensive guard
            continue

        candidate = package_root if package_name.endswith(".configs") else package_root / "configs"

        with contextlib.suppress(TypeError):  # pragma: no cover - zipimport fallback
            candidate_path = Path(candidate)
            if candidate_path.exists():
                return candidate_path

    raise RuntimeError(
        "Configuration resources are not available as filesystem paths. "
        f"Set the {CONFIGS_ENV_VAR} environment variable to point to a directory "
        "containing the configuration bundle."
    )


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
