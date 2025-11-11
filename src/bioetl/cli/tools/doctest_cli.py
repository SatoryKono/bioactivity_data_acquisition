"""Shim для совместимости с `bioetl-doctest-cli`."""

from __future__ import annotations

from tools.doctest_cli import (
    CLIExample,
    CLIExampleResult,
    app,
    extract_cli_examples,
    main,
    run,
    run_examples,
)

__all__ = [
    "app",
    "main",
    "run",
    "run_examples",
    "extract_cli_examples",
    "CLIExample",
    "CLIExampleResult",
]


if __name__ == "__main__":
    run()

