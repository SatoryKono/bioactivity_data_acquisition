"""Command line interface for running the bioactivity ETL pipeline."""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from library.clients.health import create_health_checker_from_config
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
from library.logging_setup import bind_stage, configure_logging, generate_run_id, set_run_context
from library.telemetry import setup_telemetry
from library.testitem.config import (
    load_testitem_config,
)
from library.testitem.pipeline import (
    TestitemHTTPError,
    TestitemIOError,
    TestitemQCError,
    TestitemValidationError,
    run_testitem_etl,
    write_testitem_outputs,
)
from library.utils.graceful_shutdown import ShutdownContext, register_shutdown_handler

# Typer option constants to avoid B008 errors
CONFIG_OPTION = typer.Option(..., "--config", "-c", help="Path to configuration YAML file")
OVERRIDES_OPTION = typer.Option([], "--set", "-s", help="Override configuration values using dotted paths (KEY=VALUE)")
LOG_LEVEL_OPTION = typer.Option("INFO", "--log-level", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
LOG_FILE_OPTION = typer.Option(None, "--log-file", help="Path to log file")
LOG_FORMAT_OPTION = typer.Option("text", "--log-format", help="Console format (text or json)")
NO_FILE_LOG_OPTION = typer.Option(False, "--no-file-log", help="Disable file logging")

# Document data command options
DOC_CONFIG_OPTION = typer.Option(
    None,
    "--config",
    "-c",
    help="Path to the YAML configuration file.",
    dir_okay=False,
    resolve_path=True,
)
DOCUMENTS_CSV_OPTION = typer.Option(
    None,
    "--documents-csv",
    help="CSV containing document identifiers to enrich.",
    file_okay=True,
    dir_okay=False,
    resolve_path=True,
)
OUTPUT_DIR_OPTION = typer.Option(
    None,
    "--output-dir",
    help="Destination directory for generated artefacts.",
    file_okay=False,
    dir_okay=True,
    resolve_path=True,
)
DATE_TAG_OPTION = typer.Option(
    None,
    "--date-tag",
    help="Date tag (YYYYMMDD) embedded in output filenames.",
)
TIMEOUT_SEC_OPTION = typer.Option(
    None,
    "--timeout-sec",
    help="HTTP timeout in seconds applied to all sources.",
)
RETRIES_OPTION = typer.Option(
    None,
    "--retries",
    help="Total retry attempts for HTTP requests.",
)
WORKERS_OPTION = typer.Option(
    None,
    "--workers",
    help="Number of concurrent workers for parallel processing.",
)
LIMIT_OPTION = typer.Option(
    None,
    "--limit",
    help="Limit the number of documents processed.",
)
SOURCES_OPTION = typer.Option(
    [],
    "--source",
    help="Enable only the listed sources (repeat for multiple).",
)
ALL_SOURCES_OPTION = typer.Option(
    False,
    "--all-sources",
    help="Enable all available sources.",
)

# Unified command options for all pipelines
UNIFIED_CONFIG_OPTION = typer.Option(
    None,
    "--config",
    "-c",
    help="Path to the YAML configuration file.",
    dir_okay=False,
    resolve_path=True,
)
UNIFIED_INPUT_OPTION = typer.Option(
    None,
    "--input",
    help="CSV containing entity identifiers to process.",
    file_okay=True,
    dir_okay=False,
    resolve_path=True,
)
UNIFIED_OUTPUT_DIR_OPTION = typer.Option(
    None,
    "--output-dir",
    help="Destination directory for generated artefacts.",
    file_okay=False,
    dir_okay=True,
    resolve_path=True,
)

# Testitem command options
TESTITEM_CONFIG_OPTION = typer.Option(
    ...,
    "--config",
    "-c",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
    help="Path to testitem configuration YAML file",
)
TESTITEM_INPUT_OPTION = typer.Option(
    ...,
    "--input",
    "-i",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
    help="Path to input CSV file with molecule identifiers",
)
TESTITEM_OUTPUT_OPTION = typer.Option(
    None,
    "--output",
    "-o",
    resolve_path=True,
    help="Output directory for results (defaults to config io.output.dir)",
)
TESTITEM_CACHE_DIR_OPTION = typer.Option(
    None,
    "--cache-dir",
    resolve_path=True,
    help="Directory for ChEMBL HTTP cache",
)
TESTITEM_PUBCHEM_CACHE_DIR_OPTION = typer.Option(
    None,
    "--pubchem-cache-dir",
    resolve_path=True,
    help="Directory for PubChem HTTP cache",
)
TESTITEM_TIMEOUT_OPTION = typer.Option(
    None,
    "--timeout",
    help="HTTP timeout in seconds",
)
TESTITEM_RETRIES_OPTION = typer.Option(
    None,
    "--retries",
    help="Number of retry attempts for failed requests",
)

# Health command options
HEALTH_TIMEOUT_OPTION = typer.Option(10.0, "--timeout", "-t", help="Timeout for health checks in seconds")
JSON_OUTPUT_OPTION = typer.Option(False, "--json", help="Output results in JSON format")


def _setup_api_keys_automatically() -> None:
    """Автоматически установить API ключи в переменные окружения при запуске."""
    # Список API ключей с их значениями по умолчанию
    api_keys = {
        "SEMANTIC_SCHOLAR_API_KEY": "o2N1y1RHYU3aqEj556Oyv4oBzZrHthM2bWda2lf4",
        # Другие ключи можно добавить здесь при необходимости
        # "CHEMBL_API_TOKEN": "your_chembl_token_here",
        # "CROSSREF_API_KEY": "your_crossref_key_here",
        # "OPENALEX_API_KEY": "your_openalex_key_here",
        # "PUBMED_API_KEY": "your_pubmed_key_here",
    }

    # Устанавливаем ключи только если они не установлены
    for key, default_value in api_keys.items():
        if not os.environ.get(key):
            os.environ[key] = default_value
            # Логируем установку ключа (только первые 10 символов для безопасности)
            display_key = default_value[:10] + "..." if len(default_value) > 10 else default_value
            print(f"Автоматически установлен API ключ: {key} = {display_key}")


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
            raise DocumentValidationError(f"Unsupported source '{item}'. Valid options: {', '.join(ALLOWED_SOURCES)}")
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
    overrides: list[str] = OVERRIDES_OPTION,
    log_level: str = LOG_LEVEL_OPTION,
    log_file: Path | None = LOG_FILE_OPTION,
    log_format: str = LOG_FORMAT_OPTION,
    no_file_log: bool = NO_FILE_LOG_OPTION,
) -> None:
    """Execute the ETL pipeline using a configuration file."""

    # Generate unique run ID for this execution
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="cli_startup")

    override_dict = _parse_override_args(overrides)
    config_model = Config.load(config, overrides=override_dict)

    # Создаем необходимые директории после загрузки конфигурации
    ensure_output_directories_exist(config_model)

    # Configure logging with CLI parameters
    logger = configure_logging(
        level=log_level,
        file_enabled=not no_file_log,
        console_format=log_format,
        log_file=log_file,
        logging_config=config_model.logging.model_dump() if hasattr(config_model, "logging") else None,
    )
    with bind_stage(logger, "pipeline", run_id=run_id) as logger:
        logger.info("Pipeline started", run_id=run_id, config=str(config))

        # Setup graceful shutdown
        def cleanup_handler() -> None:
            logger.info("Pipeline shutdown requested, cleaning up...", run_id=run_id)
            # Add any cleanup logic here

        register_shutdown_handler(cleanup_handler)

        try:
            with ShutdownContext(timeout=30.0):
                output = run_pipeline(config_model, logger)
                logger.info("Pipeline completed successfully", output=str(output), run_id=run_id)
                typer.echo(f"Pipeline completed. Output written to {output}")
        except Exception as exc:
            logger.error("Pipeline failed", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(f"Pipeline failed: {exc}", err=True)
            raise typer.Exit(1) from exc


@app.command("get-document-data")
def get_document_data(
    *,
    config: Path | None = DOC_CONFIG_OPTION,
    documents_csv: Path | None = DOCUMENTS_CSV_OPTION,
    output_dir: Path | None = OUTPUT_DIR_OPTION,
    date_tag: str | None = DATE_TAG_OPTION,
    timeout_sec: float | None = TIMEOUT_SEC_OPTION,
    retries: int | None = RETRIES_OPTION,
    workers: int | None = WORKERS_OPTION,
    limit: int | None = LIMIT_OPTION,
    sources: list[str] = SOURCES_OPTION,
    all_sources: bool = ALL_SOURCES_OPTION,
    dry_run: bool | None = typer.Option(
        None,
        "--dry-run/--no-dry-run",
        help="Execute without writing artefacts to disk.",
    ),
    log_level: str = LOG_LEVEL_OPTION,
    log_file: Path | None = LOG_FILE_OPTION,
    log_format: str = LOG_FORMAT_OPTION,
    no_file_log: bool = NO_FILE_LOG_OPTION,
) -> None:
    """Collect and enrich document metadata from configured sources."""

    # Generate unique run ID for this execution
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="document_processing")

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

    # Configure logging with CLI parameters
    logger = configure_logging(
        level=log_level,
        file_enabled=not no_file_log,
        console_format=log_format,
        log_file=log_file,
        logging_config=config_model.logging.model_dump() if hasattr(config_model, "logging") else None,
    )
    with bind_stage(logger, "document_processing", run_id=run_id) as logger:
        logger.info("Document processing started", run_id=run_id)

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
        def cleanup_handler() -> None:
            logger.info("Document processing shutdown requested, cleaning up...", run_id=run_id)
            # Add any cleanup logic here

        register_shutdown_handler(cleanup_handler)

        try:
            with ShutdownContext(timeout=60.0):
                with bind_stage(logger, "document_etl"):
                    result = run_document_etl(config_model, input_frame)

            logger.info("Document processing completed", run_id=run_id, records=len(result.documents))

        except DocumentValidationError as exc:
            logger.error("Document validation failed", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc
        except DocumentHTTPError as exc:
            logger.error("Document HTTP error", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.HTTP_ERROR) from exc
        except DocumentQCError as exc:
            logger.error("Document QC error", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.QC_ERROR) from exc

        if config_model.runtime.dry_run:
            logger.info("Dry run completed; no artefacts written.", run_id=run_id)
            typer.echo("Dry run completed; no artefacts written.")
            raise typer.Exit(code=ExitCode.OK)

        try:
            with bind_stage(logger, "write_outputs"):
                outputs = write_document_outputs(
                    result,
                    config_model.io.output.dir,
                    config_model.runtime.date_tag or "",
                    config_model,  # Передаем конфигурацию для применения determinism.column_order
                )

            logger.info("Outputs written successfully", run_id=run_id, outputs=list(outputs.keys()))

        except DocumentIOError as exc:
            logger.error("Document IO error", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.IO_ERROR) from exc

        for name, path in outputs.items():
            typer.echo(f"{name}: {path}")


# Testitem CLI commands
console = Console()


class TestitemExitCode(int):
    """Enumerated exit codes for the testitem CLI."""

    OK = 0
    VALIDATION_ERROR = 1
    HTTP_ERROR = 2
    QC_ERROR = 3
    IO_ERROR = 4


@app.command("testitem-run")
def testitem_run(
    config: Path = TESTITEM_CONFIG_OPTION,
    input: Path = TESTITEM_INPUT_OPTION,
    output: Path = TESTITEM_OUTPUT_OPTION,
    cache_dir: Path = TESTITEM_CACHE_DIR_OPTION,
    pubchem_cache_dir: Path = TESTITEM_PUBCHEM_CACHE_DIR_OPTION,
    timeout: int = TESTITEM_TIMEOUT_OPTION,
    retries: int = TESTITEM_RETRIES_OPTION,
    limit: int = typer.Option(
        None,
        "--limit",
        help="Limit number of records to process",
    ),
    disable_pubchem: bool = typer.Option(
        False,
        "--disable-pubchem",
        help="Disable PubChem enrichment",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Run in dry-run mode (no actual processing)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
    date_tag: str = typer.Option(
        None,
        "--date-tag",
        help="Date tag (YYYYMMDD) embedded in output filenames",
    ),
) -> None:
    """Run the testitem ETL pipeline."""

    try:
        # Load configuration
        console.print(f"[blue]Loading configuration from: {config}[/blue]")
        testitem_config = load_testitem_config(config)

        # Override configuration with CLI arguments
        if cache_dir:
            testitem_config.runtime.cache_dir = str(cache_dir)
        if pubchem_cache_dir:
            testitem_config.runtime.pubchem_cache_dir = str(pubchem_cache_dir)
        if timeout:
            testitem_config.runtime.timeout_sec = timeout
        if retries:
            testitem_config.runtime.retries = retries
        if limit:
            testitem_config.runtime.limit = limit
        if disable_pubchem:
            testitem_config.enable_pubchem = False
        if dry_run:
            testitem_config.runtime.dry_run = dry_run
        if date_tag:
            testitem_config.runtime.date_tag = date_tag

        # Set default output directory if not provided
        if output is None:
            # Try to get from config, fallback to default
            config_output = getattr(testitem_config.io.output, "dir", "data/output/testitem")
            output = Path(config_output)

        # Configure logging
        if verbose:
            import logging

            logging.getLogger().setLevel(logging.DEBUG)

        console.print("[green]Configuration loaded successfully[/green]")
        console.print(f"  Pipeline version: {testitem_config.pipeline_version}")
        console.print(f"  PubChem enabled: {testitem_config.enable_pubchem}")
        console.print(f"  Allow parent missing: {testitem_config.allow_parent_missing}")
        console.print(f"  Batch size: {getattr(testitem_config.runtime, 'batch_size', 200)}")
        console.print(f"  Retries: {getattr(testitem_config.runtime, 'retries', 5)}")
        console.print(f"  Timeout: {getattr(testitem_config.runtime, 'timeout_sec', 30)}s")
        console.print(f"  Output directory: {output}")

        if dry_run:
            console.print("[yellow]Running in dry-run mode - no actual processing will occur[/yellow]")
            return

        # Run ETL pipeline
        console.print("[blue]Starting testitem ETL pipeline...[/blue]")
        console.print(f"  Input file: {input}")
        console.print(f"  Output directory: {output}")

        result = run_testitem_etl(testitem_config, input_path=input)

        # Display results summary
        console.print("[green]ETL pipeline completed successfully![/green]")
        console.print(f"  Processed {len(result.testitems)} molecules")
        console.print(f"  ChEMBL release: {result.meta.get('chembl_release', 'unknown')}")

        # Display QC metrics
        if not result.qc.empty:
            console.print("\n[blue]Quality Control Metrics:[/blue]")
            qc_table = Table(show_header=True, header_style="bold magenta")
            qc_table.add_column("Metric", style="cyan")
            qc_table.add_column("Value", style="green")

            for _, row in result.qc.iterrows():
                metric = row["metric"]
                value = row["value"]
                if isinstance(value, dict):
                    value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                qc_table.add_row(metric, str(value))

            console.print(qc_table)

        # Write outputs
        console.print(f"[blue]Writing outputs to: {output}[/blue]")
        output_paths = write_testitem_outputs(result, output, date_tag or "", testitem_config)

        console.print("[green]Outputs written successfully:[/green]")
        for artifact_type, path in output_paths.items():
            console.print(f"  {artifact_type}: {path}")

        console.print("\n[green]Testitem ETL pipeline completed successfully![/green]")

    except TestitemValidationError as e:
        console.print(f"[red]Validation Error: {e}[/red]")
        raise typer.Exit(TestitemExitCode.VALIDATION_ERROR) from e
    except TestitemHTTPError as e:
        console.print(f"[red]HTTP Error: {e}[/red]")
        raise typer.Exit(TestitemExitCode.HTTP_ERROR) from e
    except TestitemQCError as e:
        console.print(f"[red]QC Error: {e}[/red]")
        raise typer.Exit(TestitemExitCode.QC_ERROR) from e
    except TestitemIOError as e:
        console.print(f"[red]I/O Error: {e}[/red]")
        raise typer.Exit(TestitemExitCode.IO_ERROR) from e
    except Exception as e:
        console.print(f"[red]Unexpected Error: {e}[/red]")
        if verbose:
            import traceback

            console.print(traceback.format_exc())
        raise typer.Exit(1) from e


@app.command("testitem-validate-config")
def testitem_validate_config(
    config: Path = TESTITEM_CONFIG_OPTION,
) -> None:
    """Validate testitem configuration file."""

    try:
        console.print(f"[blue]Validating configuration: {config}[/blue]")
        testitem_config = load_testitem_config(config)

        console.print("[green]Configuration is valid![/green]")
        console.print(f"  Pipeline version: {testitem_config.pipeline_version}")
        console.print(f"  PubChem enabled: {testitem_config.enable_pubchem}")
        console.print(f"  Allow parent missing: {testitem_config.allow_parent_missing}")

        # Display source configurations
        console.print("\n[blue]Source Configurations:[/blue]")
        for source_name, source_config in testitem_config.sources.items():
            console.print(f"  {source_name}:")
            console.print(f"    Base URL: {source_config.http.base_url}")
            console.print(f"    Timeout: {source_config.http.timeout_sec}s")
            console.print(f"    Retries: {getattr(source_config.http.retries, 'total', 'default')}")

    except Exception as e:
        console.print(f"[red]Configuration validation failed: {e}[/red]")
        raise typer.Exit(TestitemExitCode.VALIDATION_ERROR) from e


@app.command("testitem-info")
def testitem_info() -> None:
    """Display information about the testitem ETL pipeline."""

    console.print("[bold blue]Testitem ETL Pipeline[/bold blue]")
    console.print("=" * 50)
    console.print()
    console.print("[bold]Description:[/bold]")
    console.print("  ETL pipeline for extracting and normalizing molecular data")
    console.print("  from ChEMBL and PubChem APIs.")
    console.print()
    console.print("[bold]Features:[/bold]")
    console.print("  • ChEMBL molecule data extraction")
    console.print("  • PubChem enrichment (optional)")
    console.print("  • Deterministic data processing")
    console.print("  • Comprehensive validation")
    console.print("  • Quality control metrics")
    console.print("  • Structured logging")
    console.print()
    console.print("[bold]Input Requirements:[/bold]")
    console.print("  • CSV file with molecule identifiers")
    console.print("  • At least one of: molecule_chembl_id, molregno")
    console.print("  • Optional: parent_chembl_id, parent_molregno, pubchem_cid")
    console.print()
    console.print("[bold]Output:[/bold]")
    console.print("  • CSV file with normalized molecular data")
    console.print("  • Metadata YAML file")
    console.print("  • Quality control artifacts")
    console.print("  • Structured logs")
    console.print()
    console.print("[bold]Usage:[/bold]")
    console.print("  python -m library.cli testitem-run --config config.yaml --input data.csv --output results/")
    console.print()


# Deprecated aliases with warnings
@app.command("assay-run")
def assay_run_deprecated(*args, **kwargs) -> None:
    """DEPRECATED: Use 'get-assay-data' instead."""
    import warnings
    warnings.warn(
        "Command 'assay-run' is deprecated and will be removed in a future version. "
        "Use 'get-assay-data' instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return get_assay_data(*args, **kwargs)


@app.command("activity-run")
def activity_run_deprecated(*args, **kwargs) -> None:
    """DEPRECATED: Use 'get-activity-data' instead."""
    import warnings
    warnings.warn(
        "Command 'activity-run' is deprecated and will be removed in a future version. "
        "Use 'get-activity-data' instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return get_activity_data(*args, **kwargs)


@app.command("get-assay-data")
def get_assay_data(
    *,
    config: Path | None = UNIFIED_CONFIG_OPTION,
    input: Path | None = UNIFIED_INPUT_OPTION,
    output_dir: Path | None = UNIFIED_OUTPUT_DIR_OPTION,
    date_tag: str | None = DATE_TAG_OPTION,
    timeout_sec: float | None = TIMEOUT_SEC_OPTION,
    retries: int | None = RETRIES_OPTION,
    workers: int | None = WORKERS_OPTION,
    limit: int | None = LIMIT_OPTION,
    dry_run: bool | None = typer.Option(
        None,
        "--dry-run/--no-dry-run",
        help="Enable dry run mode (no actual API calls).",
    ),
    log_level: str = LOG_LEVEL_OPTION,
    log_file: Path | None = LOG_FILE_OPTION,
    no_file_log: bool = NO_FILE_LOG_OPTION,
) -> None:
    """Extract and enrich assay data from ChEMBL."""
    from library.assay.config import load_assay_config
    from library.assay.pipeline import read_assay_input, run_assay_etl_from_frame, write_assay_outputs

    # Generate unique run ID
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="assay_etl")

    try:
        # Load configuration
        config_model = load_assay_config(config)
        
        # Apply CLI overrides
        overrides = {}
        if input is not None:
            overrides["io.input.assay_ids_csv"] = input
        if output_dir is not None:
            overrides["io.output.dir"] = output_dir
        if date_tag is not None:
            overrides["runtime.date_tag"] = date_tag
        if timeout_sec is not None:
            overrides["http.global.timeout_sec"] = timeout_sec
        if retries is not None:
            overrides["http.global.retries.total"] = retries
        if workers is not None:
            overrides["runtime.workers"] = workers
        if limit is not None:
            overrides["runtime.limit"] = limit
        if dry_run is not None:
            overrides["runtime.dry_run"] = dry_run

        if overrides:
            config_model = load_assay_config(config, overrides=overrides)

        # Setup logging
        logger = configure_logging(
            level=log_level,
            file_enabled=not no_file_log,
            log_file=log_file,
        )

        logger.info("Starting assay ETL pipeline", run_id=run_id)

        # Read input data
        with bind_stage(logger, "read_input"):
            frame = read_assay_input(config_model.io.input.assay_ids_csv)
            logger.info("Input data loaded", run_id=run_id, records=len(frame))

        # Run ETL
        with bind_stage(logger, "run_etl"):
            result = run_assay_etl_from_frame(config_model, frame)
            logger.info("ETL completed", run_id=run_id, assays=len(result.assays))

        # Write outputs
        with bind_stage(logger, "write_outputs"):
            outputs = write_assay_outputs(
                result,
                config_model.io.output.dir,
                config_model.runtime.date_tag or "",
                config_model,
            )
            logger.info("Outputs written successfully", run_id=run_id, outputs=list(outputs.keys()))

        logger.info("Assay ETL pipeline completed successfully", run_id=run_id)

    except Exception as exc:
        logger.error("Assay ETL pipeline failed", run_id=run_id, error=str(exc), exc_info=True)
        typer.echo(f"Pipeline failed: {exc}", err=True)
        raise typer.Exit(1) from exc


@app.command("get-activity-data")
def get_activity_data(
    *,
    config: Path | None = UNIFIED_CONFIG_OPTION,
    input: Path | None = UNIFIED_INPUT_OPTION,
    output_dir: Path | None = UNIFIED_OUTPUT_DIR_OPTION,
    date_tag: str | None = DATE_TAG_OPTION,
    timeout_sec: float | None = TIMEOUT_SEC_OPTION,
    retries: int | None = RETRIES_OPTION,
    workers: int | None = WORKERS_OPTION,
    limit: int | None = LIMIT_OPTION,
    dry_run: bool | None = typer.Option(
        None,
        "--dry-run/--no-dry-run",
        help="Enable dry run mode (no actual API calls).",
    ),
    log_level: str = LOG_LEVEL_OPTION,
    log_file: Path | None = LOG_FILE_OPTION,
    no_file_log: bool = NO_FILE_LOG_OPTION,
) -> None:
    """Extract and enrich activity data from ChEMBL."""
    from library.activity.config import load_activity_config
    from library.activity.pipeline import read_activity_input, run_activity_etl, write_activity_outputs

    # Generate unique run ID
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="activity_etl")

    try:
        # Load configuration
        config_model = load_activity_config(config)
        
        # Apply CLI overrides
        overrides = {}
        if input is not None:
            overrides["io.input.activity_csv"] = input
        if output_dir is not None:
            overrides["io.output.dir"] = output_dir
        if date_tag is not None:
            overrides["runtime.date_tag"] = date_tag
        if timeout_sec is not None:
            overrides["http.global.timeout_sec"] = timeout_sec
        if retries is not None:
            overrides["http.global.retries.total"] = retries
        if workers is not None:
            overrides["runtime.workers"] = workers
        if limit is not None:
            overrides["runtime.limit"] = limit
        if dry_run is not None:
            overrides["runtime.dry_run"] = dry_run

        if overrides:
            config_model = load_activity_config(config, overrides=overrides)

        # Setup logging
        logger = configure_logging(
            level=log_level,
            file_enabled=not no_file_log,
            log_file=log_file,
        )

        logger.info("Starting activity ETL pipeline", run_id=run_id)

        # Read input data
        with bind_stage(logger, "read_input"):
            frame = read_activity_input(config_model.io.input.activity_csv)
            logger.info("Input data loaded", run_id=run_id, records=len(frame))

        # Run ETL
        with bind_stage(logger, "run_etl"):
            result = run_activity_etl(config_model, frame)
            logger.info("ETL completed", run_id=run_id, activities=len(result.activity))

        # Write outputs
        with bind_stage(logger, "write_outputs"):
            outputs = write_activity_outputs(
                result,
                config_model.io.output.dir,
                config_model.runtime.date_tag or "",
                config_model,
            )
            logger.info("Outputs written successfully", run_id=run_id, outputs=list(outputs.keys()))

        logger.info("Activity ETL pipeline completed successfully", run_id=run_id)

    except Exception as exc:
        logger.error("Activity ETL pipeline failed", run_id=run_id, error=str(exc), exc_info=True)
        typer.echo(f"Pipeline failed: {exc}", err=True)
        raise typer.Exit(1) from exc


@app.command("get-testitem-data")
def get_testitem_data(
    *,
    config: Path | None = UNIFIED_CONFIG_OPTION,
    input: Path | None = UNIFIED_INPUT_OPTION,
    output_dir: Path | None = UNIFIED_OUTPUT_DIR_OPTION,
    date_tag: str | None = DATE_TAG_OPTION,
    timeout_sec: float | None = TIMEOUT_SEC_OPTION,
    retries: int | None = RETRIES_OPTION,
    workers: int | None = WORKERS_OPTION,
    limit: int | None = LIMIT_OPTION,
    dry_run: bool | None = typer.Option(
        None,
        "--dry-run/--no-dry-run",
        help="Enable dry run mode (no actual API calls).",
    ),
    log_level: str = LOG_LEVEL_OPTION,
    log_file: Path | None = LOG_FILE_OPTION,
    no_file_log: bool = NO_FILE_LOG_OPTION,
) -> None:
    """Extract and enrich testitem data from PubChem."""
    from library.testitem.config import load_testitem_config
    from library.testitem.pipeline import read_testitem_input, run_testitem_etl, write_testitem_outputs

    # Generate unique run ID
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="testitem_etl")

    try:
        # Load configuration
        config_model = load_testitem_config(config)
        
        # Apply CLI overrides
        overrides = {}
        if input is not None:
            overrides["io.input.testitem_csv"] = input
        if output_dir is not None:
            overrides["io.output.dir"] = output_dir
        if date_tag is not None:
            overrides["runtime.date_tag"] = date_tag
        if timeout_sec is not None:
            overrides["http.global.timeout_sec"] = timeout_sec
        if retries is not None:
            overrides["http.global.retries.total"] = retries
        if workers is not None:
            overrides["runtime.workers"] = workers
        if limit is not None:
            overrides["runtime.limit"] = limit
        if dry_run is not None:
            overrides["runtime.dry_run"] = dry_run

        if overrides:
            config_model = load_testitem_config(config, overrides=overrides)

        # Setup logging
        logger = configure_logging(
            level=log_level,
            file_enabled=not no_file_log,
            log_file=log_file,
        )

        logger.info("Starting testitem ETL pipeline", run_id=run_id)

        # Read input data
        with bind_stage(logger, "read_input"):
            frame = read_testitem_input(config_model.io.input.testitem_csv)
            logger.info("Input data loaded", run_id=run_id, records=len(frame))

        # Run ETL
        with bind_stage(logger, "run_etl"):
            result = run_testitem_etl(config_model, frame)
            logger.info("ETL completed", run_id=run_id, testitems=len(result.testitems))

        # Write outputs
        with bind_stage(logger, "write_outputs"):
            outputs = write_testitem_outputs(
                result,
                config_model.io.output.dir,
                config_model.runtime.date_tag or "",
                config_model,
            )
            logger.info("Outputs written successfully", run_id=run_id, outputs=list(outputs.keys()))

        logger.info("Testitem ETL pipeline completed successfully", run_id=run_id)

    except Exception as exc:
        logger.error("Testitem ETL pipeline failed", run_id=run_id, error=str(exc), exc_info=True)
        typer.echo(f"Pipeline failed: {exc}", err=True)
        raise typer.Exit(1) from exc


@app.command()
def health(
    config: Path = CONFIG_OPTION,
    timeout: float = HEALTH_TIMEOUT_OPTION,
    json_output: bool = JSON_OUTPUT_OPTION,
    log_level: str = LOG_LEVEL_OPTION,
    log_file: Path | None = LOG_FILE_OPTION,
    no_file_log: bool = NO_FILE_LOG_OPTION,
) -> None:
    """Check health status of all configured API clients."""

    # Generate unique run ID for health check
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="health_check")

    try:
        # Load configuration
        config_model = Config.load(config)

        # Configure logging
        logger = configure_logging(
            level=log_level,
            file_enabled=not no_file_log,
            log_file=log_file,
            logging_config=config_model.logging.model_dump() if hasattr(config_model, "logging") else None,
        )
        with bind_stage(logger, "health_check", run_id=run_id) as logger:
            logger.info("Health check started", run_id=run_id, timeout=timeout)

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

            with bind_stage(logger, "api_health_checks"):
                statuses = health_checker.check_all(timeout=timeout)

            # Log results
            healthy_count = sum(1 for s in statuses if s.is_healthy)
            unhealthy_count = len(statuses) - healthy_count

            logger.info("Health check completed", run_id=run_id, total_apis=len(statuses), healthy=healthy_count, unhealthy=unhealthy_count)

            if json_output:
                # Output JSON format
                import json

                summary = health_checker.get_health_summary(statuses)
                summary["apis"] = [
                    {"name": s.name, "healthy": s.is_healthy, "response_time_ms": s.response_time_ms, "circuit_state": s.circuit_state, "error": s.error_message} for s in statuses
                ]
                typer.echo(json.dumps(summary, indent=2))
            else:
                # Output formatted table
                health_checker.print_health_report(statuses)

            # Exit with error code if any APIs are unhealthy
            if unhealthy_count > 0:
                raise typer.Exit(1)

    except Exception as exc:
        logger.error("Health check failed", error=str(exc), run_id=run_id, exc_info=True)
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
def install_completion(shell: str = typer.Argument(..., help="Shell type (bash, zsh, fish, powershell)")) -> None:
    """Install shell completion for the CLI."""

    import subprocess
    import sys

    # Get the current executable path
    executable = sys.executable

    # Get the script name
    script_name = "bioactivity-data-acquisition"

    # Validate shell parameter
    allowed_shells = {"bash", "zsh", "fish", "powershell"}
    if shell not in allowed_shells:
        typer.echo(f"❌ Unsupported shell: {shell}")
        typer.echo(f"Supported shells: {', '.join(allowed_shells)}")
        raise typer.Exit(1)

    try:
        # Generate completion script
        result = subprocess.run(  # noqa: S603
            [
                executable,
                "-m",
                "typer",
                "library.cli:app",
                f"--name={script_name}",
                f"{shell}",
                "--output-file",
                "-",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        completion_script = result.stdout

        if shell == "bash":
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
            typer.echo("  fpath=(~/.zsh/completions $fpath)")
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

    except subprocess.CalledProcessError as e:
        typer.echo(f"❌ Failed to generate completion script: {e}")
        typer.echo(f"Error output: {e.stderr}")
        raise typer.Exit(1) from e
    except Exception as e:
        typer.echo(f"❌ Failed to install completion: {e}")
        raise typer.Exit(1) from e


def main() -> None:
    """Entrypoint for ``python -m library.cli``."""
    # Автоматически установить API ключи
    _setup_api_keys_automatically()

    # Initialize telemetry
    setup_telemetry(
        service_name="bioactivity-etl",
        enable_requests_instrumentation=True,
    )

    app()


if __name__ == "__main__":  # pragma: no cover - convenience entrypoint
    main()


__all__ = ["ExitCode", "app", "get_document_data", "install_completion", "main"]
