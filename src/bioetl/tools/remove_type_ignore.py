"""Strip `type: ignore` directives from source files."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

from bioetl.core.logging import UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.tools import get_project_root

__all__ = ["remove_type_ignore"]


TYPE_IGNORE_PATTERN = re.compile(r"\s+#\s*type:\s*ignore(?::[^\n]*)?")


def _iter_python_files(root: Path) -> Iterable[Path]:
    """Yield Python files under ``root`` excluding virtual environment directories."""
    for path in root.rglob("*.py"):
        if "/.venv/" in path.as_posix():
            continue
        yield path


def _cleanse_file(path: Path) -> int:
    """Remove ``type: ignore`` directives from a file and return the count removed."""
    content = path.read_text(encoding="utf-8")
    new_content, count = TYPE_IGNORE_PATTERN.subn("", content)
    if count:
        path.write_text(new_content, encoding="utf-8")
    return count


def remove_type_ignore(root: Path | None = None) -> int:
    """Remove ``type: ignore`` directives and return the number stripped."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    target_root = root if root is not None else get_project_root()
    total_removed = 0
    for file_path in _iter_python_files(target_root):
        total_removed += _cleanse_file(file_path)

    log.info(LogEvents.TYPE_IGNORE_REMOVED, count=total_removed, root=str(target_root))
    return total_removed
