"""Backward-compatible CLI entrypoint for ``python -m interfaces.cli.pipeline``."""

from __future__ import annotations

from interfaces.cli.cli_app import run


def main() -> None:
    """Execute the primary BioETL CLI application."""

    run()


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    main()
