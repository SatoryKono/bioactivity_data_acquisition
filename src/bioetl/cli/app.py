"""Main Typer application for BioETL CLI.

This module creates the Typer application and registers all pipeline commands
from the static registry.
"""

from __future__ import annotations

import typer

from bioetl.cli.command import create_pipeline_command
from bioetl.cli.common import run as run_cli
from bioetl.cli.registry import COMMAND_REGISTRY

__all__ = ["app", "create_app"]


def create_app() -> typer.Typer:
    """Create and configure the Typer application with all registered commands."""
    app = typer.Typer(
        name="bioetl",
        help="BioETL command-line interface for executing ETL pipelines.",
        add_completion=False,
    )

    # Register list command first
    @app.command(name="list")
    def list_commands() -> None:
        """List all available pipeline commands."""
        commands = sorted(COMMAND_REGISTRY.keys())
        typer.echo("Available pipeline commands:")
        for cmd in commands:
            try:
                config = COMMAND_REGISTRY[cmd]()
                typer.echo(f"  {cmd:<20} - {config.description}")
            except NotImplementedError:
                typer.echo(f"  {cmd:<20} - (not yet implemented)")
            except Exception as exc:
                typer.echo(f"  {cmd:<20} - (error: {exc})")

    # Register all pipeline commands from registry
    for command_name, build_config_func in COMMAND_REGISTRY.items():
        try:
            command_config = build_config_func()
            command_func = create_pipeline_command(
                pipeline_class=command_config.pipeline_class,
                command_config=command_config,
            )
            app.command(name=command_name)(command_func)
        except NotImplementedError:
            # Skip not-yet-implemented pipelines
            continue
        except Exception as exc:
            # Log error but continue registering other commands
            typer.echo(
                f"Warning: Failed to register command '{command_name}': {exc}",
                err=True,
            )

    return app


# Create the app instance
app = create_app()


def run() -> None:
    """Entry point for CLI application with унифицированным раннером."""
    exit_code = run_cli(app, setup_logging=False)
    if exit_code != 0:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    run()
