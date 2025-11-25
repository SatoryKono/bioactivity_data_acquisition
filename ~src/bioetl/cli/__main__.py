"""Module entrypoint to support ``python -m bioetl.cli`` invocation."""

from __future__ import annotations

from bioetl.cli.cli_app import run


def main() -> None:
    """Execute the Typer application."""

    run()


if __name__ == "__main__":
    main()
