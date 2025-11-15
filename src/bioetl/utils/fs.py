"""Filesystem helpers shared across pipeline implementations."""

from __future__ import annotations

from pathlib import Path


def ensure_directory(path: Path, *, exist_ok: bool = True) -> Path:
    """Ensure that ``path`` exists as a directory and return it.

    Parameters
    ----------
    path:
        Target directory path.
    exist_ok:
        Forwarded to :meth:`Path.mkdir`. When ``False`` an existing directory
        triggers :class:`FileExistsError`.

    Returns
    -------
    Path
        The directory path.

    Raises
    ------
    NotADirectoryError
        If the target exists but is not a directory.
    FileExistsError
        If the directory exists and ``exist_ok`` is ``False``.
    """

    if path.exists():
        if path.is_dir():
            if exist_ok:
                return path
            raise FileExistsError(f"Directory already exists: {path}")
        raise NotADirectoryError(f"Path exists and is not a directory: {path}")

    try:
        path.mkdir(parents=True, exist_ok=exist_ok)
    except FileExistsError as exc:
        if path.is_dir():
            if exist_ok:
                return path
            raise FileExistsError(f"Directory already exists: {path}") from exc
        raise NotADirectoryError(f"Path exists and is not a directory: {path}") from exc

    return path
