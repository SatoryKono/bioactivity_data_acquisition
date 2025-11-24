"""Module entrypoint to support ``python -m interfaces.cli`` invocation."""

from __future__ import annotations

from interfaces.cli.cli_app import run


def main() -> None:
    """Execute the Typer application."""

    run()


if __name__ == "__main__":
    main()
