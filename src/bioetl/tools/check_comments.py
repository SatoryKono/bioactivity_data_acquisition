"""Заготовка проверки комментариев в кодовой базе."""

from __future__ import annotations

from pathlib import Path

from bioetl.core.logger import UnifiedLogger
from bioetl.core.log_events import LogEvents
from bioetl.tools import get_project_root

__all__ = ["run_comment_check"]


def run_comment_check(root: Path | None = None) -> None:
    """Проверяет комментарии в репозитории (пока не реализовано)."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    target_root = root if root is not None else get_project_root()
    log.warning(LogEvents.COMMENT_CHECK_NOT_IMPLEMENTED, root=str(target_root))
    raise NotImplementedError("Comment check is not yet implemented")
