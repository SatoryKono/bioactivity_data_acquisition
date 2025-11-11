"""Shim для совместимости с `bioetl-run-test-report`."""

from __future__ import annotations

from tools.run_test_report import (
    TEST_REPORTS_ROOT,
    app,
    generate_test_report,
    main,
    run,
)

__all__ = ["app", "main", "run", "generate_test_report", "TEST_REPORTS_ROOT"]


if __name__ == "__main__":
    run()

