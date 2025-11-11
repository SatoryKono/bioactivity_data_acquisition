"""Shim для совместимости с `bioetl-check-comments`."""

from __future__ import annotations

from tools.check_comments import app, main, run, run_comment_check

__all__ = ["app", "main", "run", "run_comment_check"]


if __name__ == "__main__":
    run()

