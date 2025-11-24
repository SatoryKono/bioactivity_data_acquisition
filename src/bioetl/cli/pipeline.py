"""Backward-compatible CLI entrypoint for ``python -m bioetl.cli.pipeline``."""

from __future__ import annotations

from bioetl.cli.cli_app import run


def main() -> None:
    """Execute the primary BioETL CLI application."""

    run()


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    main()
