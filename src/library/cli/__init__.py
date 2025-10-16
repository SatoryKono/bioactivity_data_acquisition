"""Command line interface for running the bioactivity ETL pipeline."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import typer

from library.config import Config, _assign_path, ensure_output_directories_exist
from library.documents.config import (
    ALLOWED_SOURCES,
    DEFAULT_ENV_PREFIX,
    ConfigLoadError,
    load_document_config,
)
from library.documents.pipeline import (
    DocumentHTTPError,
    DocumentIOError,
    DocumentQCError,
    DocumentValidationError,
    read_document_input,
    run_document_etl,
    write_document_outputs,
)
from library.etl.run import run_pipeline
from library.utils.logging import configure_logging
from library.telemetry import setup_telemetry
from library.clients.health import HealthChecker, create_health_checker_from_config
from library.utils.graceful_shutdown import ShutdownContext, register_shutdown_handler

CONFIG_OPTION = typer.Option(
    ...,
    "--config",
    "-c",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
)

def _parse_override_args(values: list[str]) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise typer.BadParameter("Overrides must be in KEY=VALUE format")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter("Override key must not be empty")
        assignments[key] = value
    return assignments


class ExitCode(int):
    """Enumerated exit codes for the document CLI."""

    OK = 0
    VALIDATION_ERROR = 1
    HTTP_ERROR = 2
    QC_ERROR = 3
    IO_ERROR = 4


def _normalise_sources(raw_sources: Iterable[str]) -> list[str]:
    seen: dict[str, None] = {}
    for item in raw_sources:
        key = item.lower()
        if key not in ALLOWED_SOURCES:
            raise DocumentValidationError(
                f"Unsupported source '{item}'. Valid options: {', '.join(ALLOWED_SOURCES)}"
            )
        seen.setdefault(key, None)
    return list(seen.keys())


def _build_cli_overrides(
    *,
    documents_csv: Path | None,
    output_dir: Path | None,
    date_tag: str | None,
    timeout_sec: float | None,
    retries: int | None,
    workers: int | None,
    limit: int | None,
    sources: list[str],
    all_sources: bool,
    dry_run: bool | None,
) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if documents_csv is not None:
        _assign_path(overrides, ["io", "input", "documents_csv"], str(documents_csv))
    if output_dir is not None:
        _assign_path(overrides, ["io", "output", "dir"], str(output_dir))
    if date_tag is not None:
        _assign_path(overrides, ["runtime", "date_tag"], date_tag)
    if timeout_sec is not None:
        _assign_path(overrides, ["http", "global", "timeout_sec"], timeout_sec)
    if retries is not None:
        _assign_path(overrides, ["http", "global", "retries", "total"], retries)
    if workers is not None:
        _assign_path(overrides, ["runtime", "workers"], workers)
    if limit is not None:
        _assign_path(overrides, ["runtime", "limit"], limit)
    if all_sources:
        # Enable all sources when --all is specified
        overrides["sources"] = {name: {"enabled": True} for name in ALLOWED_SOURCES}
    elif sources:
        # Enable only specified sources
        overrides["sources"] = {name: {"enabled": True} for name in sources}
        for name in ALLOWED_SOURCES:
            overrides["sources"].setdefault(name, {"enabled": False})
    if dry_run is not None:
        _assign_path(overrides, ["runtime", "dry_run"], dry_run)
    return overrides


app = typer.Typer(help="Bioactivity ETL pipeline")


@app.command()
def pipeline(
    config: Path = CONFIG_OPTION,
    overrides: list[str] = typer.Option([], "--set", "-s", help="Override configuration values using dotted paths (KEY=VALUE)"),
) -> None:
    """Execute the ETL pipeline using a configuration file."""

    override_dict = _parse_override_args(overrides)
    config_model = Config.load(config, overrides=override_dict)
    
    # Создаем необходимые директории после загрузки конфигурации
    ensure_output_directories_exist(config_model)
    
    logger = configure_logging(config_model.logging.level)
    logger = logger.bind(command="pipeline")
    
    # Setup graceful shutdown
    def cleanup_handler():
        logger.info("Pipeline shutdown requested, cleaning up...")
        # Add any cleanup logic here
    
    register_shutdown_handler(cleanup_handler)
    
    with ShutdownContext(timeout=30.0):
        output = run_pipeline(config_model, logger)
        typer.echo(f"Pipeline completed. Output written to {output}")


@app.command("get-document-data")
def get_document_data(
    *,
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to the YAML configuration file.",
        dir_okay=False,
        resolve_path=True,
    ),
    documents_csv: Path | None = typer.Option(
        None,
        "--documents-csv",
        help="CSV containing document identifiers to enrich.",
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
    ),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        help="Destination directory for generated artefacts.",
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    date_tag: str | None = typer.Option(
        None,
        "--date-tag",
        help="Date tag (YYYYMMDD) embedded in output filenames.",
    ),
    timeout_sec: float | None = typer.Option(
        None,
        "--timeout-sec",
        help="HTTP timeout in seconds applied to all sources.",
    ),
    retries: int | None = typer.Option(
        None,
        "--retries",
        help="Total retry attempts for HTTP requests.",
    ),
    workers: int | None = typer.Option(
        None,
        "--workers",
        help="Number of worker threads for HTTP fetching.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Limit the number of documents processed.",
    ),
    sources: list[str] = typer.Option(
        [],
        "--source",
        help="Enable only the listed sources (repeat for multiple).",
    ),
    all_sources: bool = typer.Option(
        False,
        "--all",
        help="Enable all available sources (chembl, crossref, openalex, pubmed, semantic_scholar).",
    ),
    dry_run: bool | None = typer.Option(
        None,
        "--dry-run/--no-dry-run",
        help="Execute without writing artefacts to disk.",
    ),
) -> None:
    """Collect and enrich document metadata from configured sources."""

    # Validate that --all and --source are not used together
    if all_sources and sources:
        typer.echo("Error: Cannot use --all and --source together. Use either --all or --source.", err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR)

    try:
        normalised_sources = _normalise_sources(sources)
    except DocumentValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc

    overrides = _build_cli_overrides(
        documents_csv=documents_csv,
        output_dir=output_dir,
        date_tag=date_tag,
        timeout_sec=timeout_sec,
        retries=retries,
        workers=workers,
        limit=limit,
        sources=normalised_sources,
        all_sources=all_sources,
        dry_run=dry_run,
    )

    try:
        config_model = load_document_config(
            config,
            overrides=overrides,
            env_prefix=DEFAULT_ENV_PREFIX,
        )
    except ConfigLoadError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc

    # Initialize logging
    logger = configure_logging(config_model.logging.level)
    logger = logger.bind(command="get-document-data")

    if not config_model.enabled_sources():
        typer.echo("At least one document source must be enabled", err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR)

    try:
        input_frame = read_document_input(config_model.io.input.documents_csv)
    except DocumentValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc
    except DocumentIOError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.IO_ERROR) from exc

    # Setup graceful shutdown for document processing
    def cleanup_handler():
        logger.info("Document processing shutdown requested, cleaning up...")
        # Add any cleanup logic here
    
    register_shutdown_handler(cleanup_handler)
    
    try:
        with ShutdownContext(timeout=60.0):
            result = run_document_etl(config_model, input_frame)
    except DocumentValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc
    except DocumentHTTPError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.HTTP_ERROR) from exc
    except DocumentQCError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.QC_ERROR) from exc

    if config_model.runtime.dry_run:
        typer.echo("Dry run completed; no artefacts written.")
        raise typer.Exit(code=ExitCode.OK)

    try:
        outputs = write_document_outputs(
            result,
            config_model.io.output.dir,
            config_model.runtime.date_tag,
            config_model,
        )
    except DocumentIOError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.IO_ERROR) from exc

    for name, path in outputs.items():
        typer.echo(f"{name}: {path}")


@app.command()
def health(
    config: Path = CONFIG_OPTION,
    timeout: float = typer.Option(10.0, "--timeout", "-t", help="Timeout for health checks in seconds"),
    json_output: bool = typer.Option(False, "--json", help="Output results in JSON format"),
) -> None:
    """Check health status of all configured API clients."""
    
    try:
        # Load configuration
        config_model = Config.load(config)
        logger = configure_logging(config_model.logging.level)
        logger = logger.bind(command="health")
        
        # Create health checker from API configurations
        # Get clients from config_model.clients (list of APIClientConfig)
        # and filter by enabled sources
        api_configs = {}
        enabled_sources = {name for name, source in config_model.sources.items() if source.enabled}
        
        for client_config in config_model.clients:
            if client_config.name in enabled_sources:
                api_configs[client_config.name] = client_config
        
        if not api_configs:
            typer.echo("No enabled API clients found for health checking", err=True)
            raise typer.Exit(1)
        
        health_checker = create_health_checker_from_config(api_configs)
        
        # Perform health checks
        typer.echo("Checking API health...")
        statuses = health_checker.check_all(timeout=timeout)
        
        if json_output:
            # Output JSON format
            import json
            summary = health_checker.get_health_summary(statuses)
            summary["apis"] = [
                {
                    "name": s.name,
                    "healthy": s.is_healthy,
                    "response_time_ms": s.response_time_ms,
                    "circuit_state": s.circuit_state,
                    "error": s.error_message
                }
                for s in statuses
            ]
            typer.echo(json.dumps(summary, indent=2))
        else:
            # Output formatted table
            health_checker.print_health_report(statuses)
            
            # Exit with error code if any APIs are unhealthy
            unhealthy_count = sum(1 for s in statuses if not s.is_healthy)
            if unhealthy_count > 0:
                raise typer.Exit(1)
        
    except Exception as exc:
        typer.echo(f"Health check failed: {exc}", err=True)
        raise typer.Exit(1) from exc


@app.command()
def version() -> None:
    """Print the package version."""
    try:
        from importlib.metadata import version
        package_version = version("bioactivity-data-acquisition")
        typer.echo(f"bioactivity-data-acquisition {package_version}")
    except Exception:
        # Fallback to hardcoded version if importlib.metadata fails
        typer.echo("bioactivity-data-acquisition 0.1.0")


@app.command()
def install_completion(
    shell: str = typer.Argument(..., help="Shell type (bash, zsh, fish, powershell)")
) -> None:
    """Install shell completion for the CLI."""
    
    import subprocess
    import sys
    
    # Get the current executable path
    executable = sys.executable
    
    # Get the script name
    script_name = "bioactivity-data-acquisition"
    
    try:
        # Generate completion script
        result = subprocess.run([
            executable, "-m", "typer", "library.cli:app", 
            f"--name={script_name}", 
            f"{shell}",
            "--output-file", "-"
        ], capture_output=True, text=True, check=True)
        
        completion_script = result.stdout
        
        if shell == "bash":
            completion_file = Path.home() / ".bashrc"
            completion_dir = Path.home() / ".local" / "share" / "bash-completion" / "completions"
            completion_dir.mkdir(parents=True, exist_ok=True)
            completion_path = completion_dir / script_name
            
            with completion_path.open("w") as f:
                f.write(completion_script)
            
            typer.echo(f"✅ Bash completion installed to {completion_path}")
            typer.echo(f"Run 'source {completion_path}' or restart your shell to activate.")
            
        elif shell == "zsh":
            completion_dir = Path.home() / ".zsh" / "completions"
            completion_dir.mkdir(parents=True, exist_ok=True)
            completion_path = completion_dir / f"_{script_name}"
            
            with completion_path.open("w") as f:
                f.write(completion_script)
            
            typer.echo(f"✅ Zsh completion installed to {completion_path}")
            typer.echo("Add the following to your ~/.zshrc:")
            typer.echo(f"  fpath=(~/.zsh/completions $fpath)")
            typer.echo("  autoload -U compinit && compinit")
            
        elif shell == "fish":
            completion_dir = Path.home() / ".config" / "fish" / "completions"
            completion_dir.mkdir(parents=True, exist_ok=True)
            completion_path = completion_dir / f"{script_name}.fish"
            
            with completion_path.open("w") as f:
                f.write(completion_script)
            
            typer.echo(f"✅ Fish completion installed to {completion_path}")
            typer.echo("Restart your shell to activate.")
            
        elif shell == "powershell":
            completion_dir = Path.home() / "Documents" / "PowerShell" / "Modules"
            completion_dir.mkdir(parents=True, exist_ok=True)
            completion_path = completion_dir / f"{script_name}.ps1"
            
            with completion_path.open("w") as f:
                f.write(completion_script)
            
            typer.echo(f"✅ PowerShell completion installed to {completion_path}")
            typer.echo("Add the following to your PowerShell profile:")
            typer.echo(f"  . {completion_path}")
            
        else:
            typer.echo(f"❌ Unsupported shell: {shell}")
            typer.echo("Supported shells: bash, zsh, fish, powershell")
            raise typer.Exit(1)
            
    except subprocess.CalledProcessError as e:
        typer.echo(f"❌ Failed to generate completion script: {e}")
        typer.echo(f"Error output: {e.stderr}")
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"❌ Failed to install completion: {e}")
        raise typer.Exit(1)


def main() -> None:
    """Entrypoint for ``python -m library.cli``."""
    # Initialize telemetry
    setup_telemetry(
        service_name="bioactivity-etl",
        enable_requests_instrumentation=True,
    )
    
    app()


if __name__ == "__main__":  # pragma: no cover - convenience entrypoint
    main()


__all__ = ["ExitCode", "app", "get_document_data", "install_completion", "main"]
