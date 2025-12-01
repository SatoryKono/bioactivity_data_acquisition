from __future__ import annotations

from pathlib import Path
from typing import Sequence

DEFAULT_PROFILE_DIR = Path("configs/defaults")
_LAYER_PATTERNS: tuple[str, ...] = ("*.yaml", "*.yml")


def _resolve_reference(value: str | Path, *, base: Path) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        resolved = candidate.expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(resolved)
        return resolved

    search_paths = [base / candidate, *base.parents, Path.cwd() / candidate]
    for candidate_path in search_paths:
        resolved = candidate_path.expanduser().resolve()
        if resolved.exists():
            return resolved
    raise FileNotFoundError(candidate)


def _resolve_config_path(config_path: str | Path) -> Path:
    candidate = Path(config_path).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    if not candidate.exists():
        msg = f"Configuration file not found: {candidate}"
        raise FileNotFoundError(msg)
    return candidate


def _discover_layer_files(directory: Path, *, base: Path) -> list[Path]:
    resolved_dir = (base / directory).resolve() if not directory.is_absolute() else directory
    if not resolved_dir.exists():
        return []
    files: list[Path] = []
    for pattern in _LAYER_PATTERNS:
        files.extend(sorted(resolved_dir.glob(pattern)))
    return files


__all__ = [
    "DEFAULT_PROFILE_DIR",
    "_LAYER_PATTERNS",
    "_discover_layer_files",
    "_resolve_config_path",
    "_resolve_reference",
]
