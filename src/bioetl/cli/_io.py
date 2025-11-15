"""Low-level filesystem and Git helpers for CLI utilities."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping, Sequence
from hashlib import blake2b
from hashlib import new as hashlib_new
from pathlib import Path
from typing import Any

import yaml

__all__ = [
    "atomic_write_yaml",
    "hash_file",
    "git_ls",
    "git_diff_cached",
]


def atomic_write_yaml(
    payload: Mapping[str, Any],
    path: Path,
    *,
    sort_keys: bool = False,
    allow_unicode: bool = True,
) -> None:
    """Persist ``payload`` as YAML using an atomic replace strategy."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            dict(payload),
            handle,
            sort_keys=sort_keys,
            allow_unicode=allow_unicode,
        )
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def hash_file(
    path: Path,
    *,
    algorithm: str = "blake2b",
    digest_size: int | None = 32,
    chunk_size: int = 64 * 1024,
) -> str:
    """Compute a hexadecimal digest for ``path`` using the requested algorithm."""

    if algorithm.lower() == "blake2b":
        hasher = blake2b(digest_size=digest_size or blake2b().digest_size)
    elif algorithm.lower() == "blake2s":
        from hashlib import blake2s

        hasher = blake2s(digest_size=digest_size or blake2s().digest_size)
    else:
        hasher = hashlib_new(algorithm)

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _normalise_git_paths(paths: Sequence[str | Path]) -> list[str]:
    return [str(Path(entry)) for entry in paths]


def git_ls(
    *paths: str | Path,
    repo_root: Path | None = None,
) -> list[Path]:
    """Return repository-tracked files restricted to ``paths`` if provided."""

    args = ["git", "ls-files"]
    if paths:
        args.extend(_normalise_git_paths(paths))
    result = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return [Path(line) for line in lines]


def git_diff_cached(
    *paths: str | Path,
    repo_root: Path | None = None,
    diff_filter: str = "AM",
) -> list[Path]:
    """Return staged files restricted to ``paths`` (defaults to added/modified)."""

    args = [
        "git",
        "diff",
        "--cached",
        "--name-only",
        f"--diff-filter={diff_filter}",
    ]
    if paths:
        args.append("--")
        args.extend(_normalise_git_paths(paths))
    result = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return [Path(line) for line in lines]
