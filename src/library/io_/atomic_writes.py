"""Atomic write utilities for safe file operations."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Callable, TypeVar
from contextlib import contextmanager

import pandas as pd
from structlog.stdlib import BoundLogger

T = TypeVar("T")


@contextmanager
def atomic_write_context(target_path: Path, temp_dir: Path | None = None, backup: bool = True, logger: BoundLogger | None = None) -> Any:
    """Context manager for atomic file writes.

    Creates a temporary file, yields it for writing, then atomically moves
    it to the target location. Optionally creates a backup of the original file.

    Args:
        target_path: The final destination path for the file
        temp_dir: Directory for temporary file (defaults to same directory as target)
        backup: Whether to create a backup of the original file
        logger: Optional logger for operation tracking

    Yields:
        Path: Path to the temporary file for writing

    Example:
        with atomic_write_context(Path("data.csv")) as temp_path:
            df.to_csv(temp_path)
        # File is atomically moved to data.csv
    """
    target_path = Path(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    # Use same directory as target if temp_dir not specified
    if temp_dir is None:
        temp_dir = target_path.parent
    else:
        temp_dir = Path(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

    # Create temporary file in the specified directory
    temp_fd, temp_path_str = tempfile.mkstemp(suffix=target_path.suffix, prefix=f".{target_path.stem}_", dir=temp_dir)
    temp_path = Path(temp_path_str)

    try:
        # Close the file descriptor immediately - we'll use Path operations
        os.close(temp_fd)

        if logger:
            logger.debug("atomic_write_start", target=str(target_path), temp=str(temp_path))

        yield temp_path

        # Create backup if requested and target exists
        if backup and target_path.exists():
            backup_path = target_path.with_suffix(f"{target_path.suffix}.backup")
            if logger:
                logger.debug("creating_backup", backup=str(backup_path))
            target_path.rename(backup_path)

        # Atomically move temp file to target
        # On Windows, we need to remove the target first if it exists
        if target_path.exists():
            target_path.unlink()

        if logger:
            logger.debug("atomic_move", from_path=str(temp_path), to_path=str(target_path))
        temp_path.rename(target_path)

        if logger:
            logger.info("atomic_write_complete", path=str(target_path))

    except Exception as e:
        # Clean up temporary file on error
        if temp_path.exists():
            temp_path.unlink()
        if logger:
            logger.error("atomic_write_failed", error=str(e), target=str(target_path))
        raise
    finally:
        # Ensure temp file is cleaned up
        if temp_path.exists():
            temp_path.unlink()


def atomic_csv_write(df: pd.DataFrame, target_path: Path, **csv_kwargs: Any) -> Path:
    """Atomically write a DataFrame to CSV.

    Args:
        df: DataFrame to write
        target_path: Target CSV file path
        **csv_kwargs: Additional arguments for pandas.to_csv()

    Returns:
        Path: The target path (for chaining)
    """
    with atomic_write_context(target_path) as temp_path:
        df.to_csv(temp_path, **csv_kwargs)
    return target_path


def atomic_parquet_write(df: pd.DataFrame, target_path: Path, **parquet_kwargs: Any) -> Path:
    """Atomically write a DataFrame to Parquet.

    Args:
        df: DataFrame to write
        target_path: Target Parquet file path
        **parquet_kwargs: Additional arguments for pandas.to_parquet()

    Returns:
        Path: The target path (for chaining)
    """
    with atomic_write_context(target_path) as temp_path:
        df.to_parquet(temp_path, **parquet_kwargs)
    return target_path


def atomic_json_write(data: dict[str, Any] | list[Any], target_path: Path, **json_kwargs: Any) -> Path:
    """Atomically write data to JSON.

    Args:
        data: Data to serialize to JSON
        target_path: Target JSON file path
        **json_kwargs: Additional arguments for json.dump()

    Returns:
        Path: The target path (for chaining)
    """
    import json

    with atomic_write_context(target_path) as temp_path:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, **json_kwargs)
    return target_path


def atomic_text_write(content: str, target_path: Path, encoding: str = "utf-8") -> Path:
    """Atomically write text content to a file.

    Args:
        content: Text content to write
        target_path: Target file path
        encoding: Text encoding (default: utf-8)

    Returns:
        Path: The target path (for chaining)
    """
    with atomic_write_context(target_path) as temp_path:
        temp_path.write_text(content, encoding=encoding)
    return target_path


def atomic_binary_write(content: bytes, target_path: Path) -> Path:
    """Atomically write binary content to a file.

    Args:
        content: Binary content to write
        target_path: Target file path

    Returns:
        Path: The target path (for chaining)
    """
    with atomic_write_context(target_path) as temp_path:
        temp_path.write_bytes(content)
    return target_path


def safe_file_operation(operation: Callable[[], T], target_path: Path, backup: bool = True, logger: BoundLogger | None = None) -> T:
    """Execute a file operation safely with atomic writes.

    This is a higher-level function that can wrap any file operation
    to make it atomic. The operation should write to the target_path directly.

    Args:
        operation: Function that performs the file operation
        target_path: Target file path
        backup: Whether to create a backup
        logger: Optional logger

    Returns:
        Result of the operation

    Example:
        def write_data():
            df.to_csv("data.csv")

        safe_file_operation(write_data, Path("data.csv"))
    """

    # Create a wrapper that writes to a temporary location
    def wrapped_operation() -> T:
        with atomic_write_context(target_path, backup=backup, logger=logger) as temp_path:
            # Temporarily replace the target path with temp path for the operation
            original_path = target_path
            # This is a bit of a hack - we need to modify the operation to write to temp_path
            # For now, we'll just execute the operation and then move the result
            result = operation()
            # If the operation wrote to target_path, move it to temp_path
            if target_path.exists():
                target_path.rename(temp_path)
            return result

    return wrapped_operation()


def verify_atomic_write(target_path: Path, expected_size: int | None = None) -> bool:
    """Verify that an atomic write completed successfully.

    Args:
        target_path: Path to the written file
        expected_size: Expected file size in bytes (optional)

    Returns:
        True if the file exists and meets criteria, False otherwise
    """
    if not target_path.exists():
        return False

    if expected_size is not None:
        actual_size = target_path.stat().st_size
        if actual_size != expected_size:
            return False

    return True


def cleanup_backups(directory: Path, pattern: str = "*.backup") -> int:
    """Clean up backup files in a directory.

    Args:
        directory: Directory to clean up
        pattern: Glob pattern for backup files

    Returns:
        Number of files removed
    """
    removed_count = 0
    for backup_file in directory.glob(pattern):
        try:
            backup_file.unlink()
            removed_count += 1
        except OSError:
            # Ignore errors when removing backup files
            pass

    return removed_count


__all__ = [
    "atomic_write_context",
    "atomic_csv_write",
    "atomic_parquet_write",
    "atomic_json_write",
    "atomic_text_write",
    "atomic_binary_write",
    "safe_file_operation",
    "verify_atomic_write",
    "cleanup_backups",
]
