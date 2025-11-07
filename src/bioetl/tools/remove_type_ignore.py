"""Удаление директив `type: ignore` из исходников."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

from bioetl.core.logger import UnifiedLogger
from bioetl.tools import get_project_root

__all__ = ["remove_type_ignore"]


TYPE_IGNORE_PATTERN = re.compile(r"\s+#\s*type:\s*ignore(?::[^\n]*)?")


def _iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if "/.venv/" in path.as_posix():
            continue
        yield path


def _cleanse_file(path: Path) -> int:
    content = path.read_text(encoding="utf-8")
    new_content, count = TYPE_IGNORE_PATTERN.subn("", content)
    if count:
        path.write_text(new_content, encoding="utf-8")
    return count


def remove_type_ignore(root: Path | None = None) -> int:
    """Удаляет директивы `type: ignore` и возвращает их количество."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    target_root = root if root is not None else get_project_root()
    total_removed = 0
    for file_path in _iter_python_files(target_root):
        total_removed += _cleanse_file(file_path)

    log.info("type_ignore_removed", count=total_removed, root=str(target_root))
    return total_removed

