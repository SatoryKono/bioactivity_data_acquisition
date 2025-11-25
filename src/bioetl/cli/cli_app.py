from __future__ import annotations

from pathlib import Path

import typer
import yaml

from bioetl.cli.cli_command import (
    _handle_cli_exception,
    _parse_cli_overrides,
    create_pipeline_command,
)
from bioetl.cli.cli_registry import PIPELINE_REGISTRY
from bioetl.config.loader import load_config

app = typer.Typer(help="BioETL pipelines", add_completion=False)
config_app = typer.Typer(help="Конфигурационные утилиты")
app.add_typer(config_app, name="config")


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """Базовый callback для Typer-приложения."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())


@config_app.command("inspect")
def inspect_config(
    config: Path = typer.Option(..., exists=True, help="Path to pipeline config"),
    profile: list[str] | None = typer.Option(None, help="Profile layers"),
    set: list[str] | None = typer.Option(  # noqa: A002 - public CLI flag
        None, help="Override config values (KEY=VALUE)", metavar="KEY=VALUE"
    ),
    include_default_profiles: bool = typer.Option(
        False, help="Include defaults from configs/defaults"
    ),
) -> None:
    """Печатает итоговую конфигурацию после мерджа профилей и ``--set``."""

    try:
        merged = load_config(
            config,
            profiles=profile or (),
            cli_overrides=_parse_cli_overrides(set or []),
            include_default_profiles=include_default_profiles,
        )
        typer.echo(yaml.safe_dump(merged.model_dump(), allow_unicode=True, sort_keys=True))
    except Exception as exc:  # pragma: no cover - mapped to exit codes
        _handle_cli_exception(exc)


@app.command("list")
def list_pipelines() -> None:
    """Выводит зарегистрированные пайплайны."""
    if not PIPELINE_REGISTRY:
        typer.echo("No pipelines registered")
        return
    for name in sorted(PIPELINE_REGISTRY):
        typer.echo(name)


@app.command("run-chembl-all")
def run_chembl_all(
    config: Path = typer.Option(..., exists=True, help="Path to pipeline config"),
    output_dir: Path = typer.Option(Path("data/output"), help="Output directory"),
    set: list[str] | None = typer.Option(None, help="Override config values (KEY=VALUE)", metavar="KEY=VALUE"),
    extended: bool = typer.Option(False, help="Persist metadata and manifest"),
    dry_run: bool = typer.Option(False, help="Skip extraction and write empty dataset"),
    sample: int | None = typer.Option(None, help="Sample N rows after extraction"),
    limit: int | None = typer.Option(None, help="Limit rows after extraction"),
    golden: bool = typer.Option(False, help="Emit QC/golden artifacts when supported"),
    verbose: bool = typer.Option(False, help="Enable verbose output when supported"),
    fail_on_schema_drift: bool = typer.Option(True, help="Fail when output schema deviates from reference"),
    validate_columns: bool = typer.Option(False, help="Validate column ordering/contents when supported"),
) -> None:
    """Запускает все ChEMBL пайплайны, зарегистрированные в реестре."""

    try:
        chembl_pipelines = [name for name in sorted(PIPELINE_REGISTRY) if name.endswith("_chembl")]
        if not chembl_pipelines:
            typer.echo("No ChEMBL pipelines registered")
            return

        for name in chembl_pipelines:
            factory = PIPELINE_REGISTRY[name]
            command = create_pipeline_command(name, factory)
            command(
                config=config,
                profile=None,
                set=set,
                run_id=None,
                output_dir=output_dir,
                extended=extended,
                dry_run=dry_run,
                sample=sample,
                limit=limit,
                golden=golden,
                verbose=verbose,
                fail_on_schema_drift=fail_on_schema_drift,
                validate_columns=validate_columns,
                progress=False,
            )
    except Exception as exc:  # pragma: no cover - mapped to exit codes
        _handle_cli_exception(exc)


for pipeline_name, factory in PIPELINE_REGISTRY.items():
    app.command(pipeline_name)(create_pipeline_command(pipeline_name, factory))
