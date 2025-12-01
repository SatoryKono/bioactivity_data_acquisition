"""CLI entrypoint for BioETL Typer application and pipeline commands."""

from __future__ import annotations

from pathlib import Path

import typer
import yaml
from pydantic import SecretStr

from bioetl.cli.cli_command import (
    _handle_cli_exception,
    _parse_cli_overrides,
    create_pipeline_command,
)
from bioetl.cli.cli_registry import PIPELINE_REGISTRY
from bioetl.cli.diagrams import generate_diagrams
from bioetl.core.config.loader import load_config


def _secret_str_representer(
    dumper: yaml.SafeDumper,
    _data: SecretStr,
) -> yaml.nodes.ScalarNode:
    return dumper.represent_scalar("tag:yaml.org,2002:str", "**********")


yaml.SafeDumper.add_representer(SecretStr, _secret_str_representer)


app = typer.Typer(help="BioETL pipelines", add_completion=False)
config_app = typer.Typer(help="Конфигурационные утилиты")
app.add_typer(config_app, name="config")
tools_app = typer.Typer(help="Утилиты разработки")
app.add_typer(tools_app, name="tools")


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """Базовый callback для Typer-приложения."""
    print(f"DEBUG: main callback, invoked_subcommand={ctx.invoked_subcommand}")
    if ctx.invoked_subcommand is None:
        print("DEBUG: No subcommand, showing help")
        typer.echo(ctx.get_help())
    else:
        print(f"DEBUG: Subcommand {ctx.invoked_subcommand} will execute")


@config_app.command("inspect")
def inspect_config(
    config: Path = typer.Option(
        ...,
        exists=True,
        help="Path to pipeline config",
    ),
    profile: list[str] | None = typer.Option(
        None,
        help="Profile layers",
    ),
    cli_set: list[str] | None = typer.Option(  # noqa: A002 - public CLI flag
        None,
        "--set",
        help="Override config values (KEY=VALUE)",
        metavar="KEY=VALUE",
    ),
    include_default_profiles: bool = typer.Option(
        False,
        help="Include defaults from configs/defaults",
    ),
) -> None:
    """Печатает итоговую конфигурацию после мерджа профилей и ``--set``."""

    try:
        merged = load_config(
            config,
            profiles=profile or (),
            cli_overrides=_parse_cli_overrides(cli_set or []),
            include_default_profiles=include_default_profiles,
        )
        typer.echo(
            yaml.safe_dump(
                merged.model_dump(),
                allow_unicode=True,
                sort_keys=True,
            )
        )
    except Exception as exc:  # pragma: no cover  # noqa: BLE001
        _handle_cli_exception(exc)


@app.command("list")
def list_pipelines() -> None:
    """Выводит зарегистрированные пайплайны."""
    print(
        "DEBUG: list_pipelines called, registry size: "
        f"{len(PIPELINE_REGISTRY)}"
    )
    print(f"DEBUG: registry id: {id(PIPELINE_REGISTRY)}")
    if not PIPELINE_REGISTRY:
        print("DEBUG: Registry is empty")
        typer.echo("No pipelines registered")
        return
    print(f"DEBUG: Registry contains: {list(PIPELINE_REGISTRY.keys())}")
    for name in sorted(PIPELINE_REGISTRY):
        typer.echo(name)


@app.command("run-chembl-all")
def run_chembl_all(
    config: Path = typer.Option(
        ...,
        exists=True,
        help="Path to pipeline config",
    ),
    output_dir: Path = typer.Option(
        Path("data/output"),
        help="Output directory",
    ),
    cli_set_all: list[str] | None = typer.Option(
        None,
        "--set",
        help="Override config values (KEY=VALUE)",
        metavar="KEY=VALUE",
    ),
    extended: bool = typer.Option(
        False,
        help="Persist metadata and manifest",
    ),
    dry_run: bool = typer.Option(
        False,
        help="Skip extraction and write empty dataset",
    ),
    sample: int | None = typer.Option(
        None,
        help="Sample N rows after extraction",
    ),
    limit: int | None = typer.Option(
        None,
        help="Limit rows after extraction",
    ),
    golden: bool = typer.Option(
        False,
        help="Emit QC/golden artifacts when supported",
    ),
    verbose: bool = typer.Option(
        False,
        help="Enable verbose output when supported",
    ),
    fail_on_schema_drift: bool = typer.Option(
        True,
        help="Fail when output schema deviates from reference",
    ),
    validate_columns: bool = typer.Option(
        False,
        help="Validate column ordering/contents when supported",
    ),
) -> None:
    """Запускает все ChEMBL пайплайны, зарегистрированные в реестре."""

    try:
        chembl_pipelines = [
            name
            for name in sorted(PIPELINE_REGISTRY)
            if name.endswith("_chembl")
        ]
        if not chembl_pipelines:
            typer.echo("No ChEMBL pipelines registered")
            return

        for name in chembl_pipelines:
            pipeline_factory = PIPELINE_REGISTRY[name]
            command = create_pipeline_command(name, pipeline_factory)
            command(
                config=config,
                profile=None,
                set=cli_set_all,
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
    except Exception as exc:  # pragma: no cover  # noqa: BLE001
        _handle_cli_exception(exc)


@tools_app.command("generate-diagrams")
def cmd_generate_diagrams(
    package: str | None = typer.Option(
        None,
        "--package",
        "-p",
        help="Имя пакета для генерации (например, 'bioetl.clients'). "
        "Если не указано, генерирует для всех пакетов.",
    ),
    repo_root: Path | None = typer.Option(
        None,
        "--repo-root",
        "-r",
        help="Корень репозитория. Если не указано, определяется автоматически.",
    ),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Директория для сохранения диаграмм. "
        "По умолчанию: 'diagrams' в корне репозитория.",
    ),
) -> None:
    """Генерирует диаграммы классов и пакетов для bioetl.* пакетов.

    Требует установленные инструменты:
    - pyreverse (из пакета pylint)
    - dot (из пакета Graphviz)
    """
    try:
        generate_diagrams(
            package=package,
            repo_root=repo_root,
            output_dir=output_dir,
        )
        typer.secho("Диаграммы успешно сгенерированы", fg=typer.colors.GREEN)
    except Exception as exc:  # pragma: no cover  # noqa: BLE001
        _handle_cli_exception(exc)


for pipeline_name, factory in PIPELINE_REGISTRY.items():
    app.command(pipeline_name)(create_pipeline_command(pipeline_name, factory))


if __name__ == "__main__":
    app()
