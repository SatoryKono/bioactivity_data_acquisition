from __future__ import annotations

import typer

from bioetl.cli.cli_registry import PIPELINE_REGISTRY

app = typer.Typer(help="BioETL pipelines", add_completion=False)


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """Базовый callback для Typer-приложения."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())


@app.command("list")
def list_pipelines() -> None:
    """Выводит зарегистрированные пайплайны."""
    if not PIPELINE_REGISTRY:
        typer.echo("No pipelines registered")
        return
    for name in sorted(PIPELINE_REGISTRY):
        typer.echo(name)
