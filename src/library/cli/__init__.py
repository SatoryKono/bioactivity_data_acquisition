"""Command line interface for running the bioactivity ETL pipeline."""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from library.activity import ActivityConfig, run_activity_etl
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
from library.target import (
    TargetHTTPError,
    TargetIOError,
    TargetQCError,
    TargetValidationError,
    load_target_config,
    read_target_input,
    run_target_etl,
    write_target_outputs,
)
from library.telemetry import setup_telemetry
from library.testitem.config import TestitemConfig
from library.testitem.pipeline import (
    TestitemHTTPError,
    TestitemIOError,
    TestitemQCError,
    TestitemValidationError,
    run_testitem_etl,
    write_testitem_outputs,
)
from library.utils.graceful_shutdown import ShutdownContext, register_shutdown_handler


def _generate_date_tag() -> str:
    """Generate date tag in YYYYMMDD format."""
    return datetime.now().strftime("%Y%m%d")


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

# Константы для часто используемых опций
DEFAULT_TIMEOUT_OPTION = typer.Option(None, "--timeout-sec", help="HTTP timeout in seconds")
DEFAULT_RETRIES_OPTION = typer.Option(None, "--retries", help="Total retry attempts")
DEFAULT_LIMIT_OPTION = typer.Option(None, "--limit", help="Limit number of records")
DEFAULT_DRY_RUN_OPTION = typer.Option(False, "--dry-run", help="Do not write outputs")
DEFAULT_LOG_LEVEL_OPTION = typer.Option("INFO", "--log-level", help="Logging level")
DEFAULT_LOG_FILE_OPTION = typer.Option(None, "--log-file", help="Path to log file")
DEFAULT_LOG_FORMAT_OPTION = typer.Option("text", "--log-format", help="Console format (text/json)")
DEFAULT_NO_FILE_LOG_OPTION = typer.Option(False, "--no-file-log", help="Disable file logging")

# Дополнительные опции для различных команд
TARGET_INPUT_OPTION = typer.Option(
    ..., "--input", help="CSV containing target_chembl_id column", resolve_path=True
)
TARGET_OUTPUT_DIR_OPTION = typer.Option(None, "--output-dir", help="Output directory")
TARGET_DATE_TAG_OPTION = typer.Option(None, "--date-tag", help="YYYYMMDD date tag")
TARGET_DEV_MODE_OPTION = typer.Option(False, "--dev-mode", help="Allow incomplete sources (dev/test only)")

# Опции для analyze-iuphar-mapping команды
ANALYZE_TARGET_CSV_OPTION = typer.Option(
    ...,
    "--target-csv",
    help="Path to target CSV file to analyze",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
)
ANALYZE_IUPHAR_DICT_OPTION = typer.Option(
    "configs/dictionary/_target/_IUPHAR/_IUPHAR_target.csv",
    "--iuphar-dict",
    help="Path to IUPHAR dictionary CSV file",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
)
ANALYZE_SAMPLE_SIZE_OPTION = typer.Option(
    10,
    "--sample-size",
    help="Number of UniProt IDs to sample for validation",
)
ANALYZE_TARGET_ID_OPTION = typer.Option(
    None,
    "--target-id",
    help="Specific IUPHAR target ID to analyze (e.g., 1386)",
)
ANALYZE_OUTPUT_FORMAT_OPTION = typer.Option(
    "table",
    "--format",
    help="Output format: table, json, csv",
)
ANALYZE_VERBOSE_OPTION = typer.Option(
    False,
    "--verbose",
    "-v",
    help="Enable verbose output",
)

# Опции для show-manifest команды
SHOW_MANIFEST_FORMAT_OPTION = typer.Option("json", "--format", "-f", help="Output format: json, yaml, table")

# Опции для get-activity-data команды
ACTIVITY_INPUT_OPTION = typer.Option(
    None, "--input", help="CSV containing filter IDs (assay_ids, molecule_ids, target_ids)", resolve_path=True
)

# Опции для pipeline команды
PIPELINE_OVERRIDES_OPTION = typer.Option([], "--set", "-s", help="Override configuration values using dotted paths (KEY=VALUE)")

# Опции для get-document-data команды
DOCUMENT_CONFIG_OPTION = typer.Option(
    None,
    "--config",
    "-c",
    help="Path to the YAML configuration file.",
    dir_okay=False,
    resolve_path=True,
)
DOCUMENT_CSV_OPTION = typer.Option(
    None,
    "--documents-csv",
    help="CSV containing document identifiers to enrich.",
    file_okay=True,
    dir_okay=False,
    resolve_path=True,
)
DOCUMENT_OUTPUT_DIR_OPTION = typer.Option(
    None,
    "--output-dir",
    help="Destination directory for generated artefacts.",
    file_okay=False,
    dir_okay=True,
    resolve_path=True,
)
DOCUMENT_DATE_TAG_OPTION = typer.Option(
    None,
    "--date-tag",
    help="Date tag (YYYYMMDD) embedded in output filenames.",
)
DOCUMENT_WORKERS_OPTION = typer.Option(
    None,
    "--workers",
    help="Number of worker threads for HTTP fetching.",
)
DOCUMENT_SOURCES_OPTION = typer.Option(
    [],
    "--source",
    help="Enable only the listed sources (repeat for multiple).",
)
DOCUMENT_ALL_SOURCES_OPTION = typer.Option(
    False,
    "--all",
    help="Enable all available sources (chembl, crossref, openalex, pubmed, semantic_scholar).",
)
DOCUMENT_DRY_RUN_OPTION = typer.Option(
    None,
    "--dry-run/--no-dry-run",
    help="Execute without writing artefacts to disk.",
)

# Опции для testitem команд
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
    help="Number of HTTP retries",
)
TESTITEM_LIMIT_OPTION = typer.Option(
    None,
    "--limit",
    help="Limit number of records to process",
)
TESTITEM_DISABLE_PUBCHEM_OPTION = typer.Option(
    False,
    "--disable-pubchem",
    help="Disable PubChem enrichment",
)
TESTITEM_DRY_RUN_OPTION = typer.Option(
    False,
    "--dry-run",
    help="Run in dry-run mode (no actual processing)",
)
TESTITEM_VERBOSE_OPTION = typer.Option(
    False,
    "--verbose",
    "-v",
    help="Enable verbose logging",
)
TESTITEM_DATE_TAG_OPTION = typer.Option(
    None,
    "--date-tag",
    help="Date tag (YYYYMMDD) embedded in output filenames",
)

# Опции для health команды
HEALTH_TIMEOUT_OPTION = typer.Option(10.0, "--timeout", "-t", help="Timeout for health checks in seconds")
HEALTH_JSON_OUTPUT_OPTION = typer.Option(False, "--json", help="Output results in JSON format")

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

# Analysis commands
@app.command("analyze-iuphar-mapping")
def analyze_iuphar_mapping(
    target_csv: Path = ANALYZE_TARGET_CSV_OPTION,
    iuphar_dict: Path = ANALYZE_IUPHAR_DICT_OPTION,
    sample_size: int = ANALYZE_SAMPLE_SIZE_OPTION,
    target_id: int | None = ANALYZE_TARGET_ID_OPTION,
    output_format: str = ANALYZE_OUTPUT_FORMAT_OPTION,
    verbose: bool = ANALYZE_VERBOSE_OPTION,
) -> None:
    """Analyze IUPHAR mapping in target data."""
    
    import json
    
    import pandas as pd
    
    console = Console()
    
    try:
        # Load target data
        console.print(f"[blue]Loading target data from: {target_csv}[/blue]")
        target_df = pd.read_csv(target_csv)
        
        # Load IUPHAR dictionary
        console.print(f"[blue]Loading IUPHAR dictionary from: {iuphar_dict}[/blue]")
        iuphar_target_df = pd.read_csv(iuphar_dict)
        
        # Basic statistics
        console.print("\n[green]=== IUPHAR Mapping Analysis ===[/green]")
        console.print(f"Total target records: {len(target_df)}")
        console.print(f"Total IUPHAR dictionary records: {len(iuphar_target_df)}")
        console.print(f"Unique UniProt IDs in IUPHAR: {iuphar_target_df['swissprot'].nunique()}")
        
        # Analyze iuphar_target_id distribution
        if 'iuphar_target_id' in target_df.columns:
            iuphar_counts = target_df['iuphar_target_id'].value_counts()
            console.print("\n[blue]IUPHAR Target ID Distribution:[/blue]")
            console.print(f"Unique IUPHAR target IDs: {len(iuphar_counts)}")
            
            if verbose:
                console.print("Top 10 IUPHAR target IDs:")
                for target_id_val, count in iuphar_counts.head(10).items():
                    console.print(f"  {target_id_val}: {count} records")
        
        # Validate UniProt ID mapping
        if 'mapping_uniprot_id' in target_df.columns:
            uniprot_ids = target_df['mapping_uniprot_id'].dropna()
            console.print("\n[blue]UniProt ID Mapping Validation:[/blue]")
            console.print(f"Records with UniProt IDs: {len(uniprot_ids)}")
            
            # Sample validation
            sample_ids = uniprot_ids.head(sample_size).tolist()
            console.print(f"Validating first {len(sample_ids)} UniProt IDs:")
            
            validation_results = []
            for uid in sample_ids:
                matches = iuphar_target_df[iuphar_target_df['swissprot'] == uid]
                if len(matches) > 0:
                    match_info = {
                        'uniprot_id': uid,
                        'found': True,
                        'target_id': matches.iloc[0]['target_id'],
                        'target_name': matches.iloc[0]['target_name']
                    }
                    validation_results.append(match_info)
                    if verbose:
                        console.print(f"  [green]✓[/green] {uid} → target_id={match_info['target_id']}, name={match_info['target_name']}")
                else:
                    match_info = {
                        'uniprot_id': uid,
                        'found': False,
                        'target_id': None,
                        'target_name': None
                    }
                    validation_results.append(match_info)
                    if verbose:
                        console.print(f"  [red]✗[/red] {uid} not found in IUPHAR dictionary")
            
            # Summary
            found_count = sum(1 for r in validation_results if r['found'])
            console.print("\n[blue]Validation Summary:[/blue]")
            console.print(f"Found in IUPHAR: {found_count}/{len(validation_results)} ({found_count/len(validation_results)*100:.1f}%)")
        
        # Analyze specific target ID if provided
        if target_id is not None:
            console.print(f"\n[blue]=== Analysis of IUPHAR Target ID {target_id} ===[/blue]")
            matches = iuphar_target_df[iuphar_target_df['target_id'] == target_id]
            
            if len(matches) > 0:
                console.print(f"Found {len(matches)} records for target_id={target_id}")
                console.print("UniProt IDs for this target:")
                uniprot_ids = matches['swissprot'].dropna().unique().tolist()  # type: ignore
                for uid in uniprot_ids:
                    console.print(f"  {uid}")
                
                # Check if any of these UniProt IDs appear in our target data
                if 'mapping_uniprot_id' in target_df.columns:
                    target_matches = target_df[target_df['mapping_uniprot_id'].isin(uniprot_ids)]
                    console.print(f"\nRecords in target data with these UniProt IDs: {len(target_matches)}")
                    if len(target_matches) > 0 and verbose:
                        console.print("Matching records:")
                        for _, row in target_matches.head(5).iterrows():
                            console.print(f"  ChEMBL ID: {row.get('target_chembl_id', 'N/A')}, UniProt: {row.get('mapping_uniprot_id', 'N/A')}")
            else:
                console.print(f"[red]No records found for target_id={target_id}[/red]")
        
        # Output results in requested format
        if output_format == "json":
            results = {
                'target_records': len(target_df),
                'iuphar_records': len(iuphar_target_df),
                'unique_uniprot_in_iuphar': iuphar_target_df['swissprot'].nunique(),
                'validation_results': validation_results if 'validation_results' in locals() else []
            }
            console.print("\n[blue]JSON Output:[/blue]")
            console.print(json.dumps(results, indent=2, ensure_ascii=False))
        
        elif output_format == "csv" and 'validation_results' in locals():
            validation_df = pd.DataFrame(validation_results)
            output_path = target_csv.parent / f"iuphar_validation_{target_csv.stem}.csv"
            validation_df.to_csv(output_path, index=False)
            console.print(f"\n[green]Validation results saved to: {output_path}[/green]")
        
        console.print("\n[green]Analysis completed successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]Error during analysis: {e}[/red]")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1) from e


# Manifest commands
@app.command("list-manifests")
def list_manifests() -> None:
    """List all available manifests."""
    console = Console()
    manifests_dir = Path("metadata/manifests")
    
    if not manifests_dir.exists():
        typer.echo("Manifests directory not found: metadata/manifests", err=True)
        raise typer.Exit(code=1)
    
    table = Table(title="Available Manifests")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Description", style="yellow")
    
    manifest_files = list(manifests_dir.glob("*.json"))
    for manifest_file in sorted(manifest_files):
        try:
            import json
            with open(manifest_file, encoding='utf-8') as f:
                data = json.load(f)
            
            name = manifest_file.stem
            manifest_type = data.get('module', 'general')
            description = data.get('description', 'No description available')
            
            table.add_row(name, manifest_type, description)
        except Exception as e:
            table.add_row(manifest_file.stem, "error", f"Failed to read: {e}")
    
    console.print(table)

@app.command("show-manifest")
def show_manifest(
    name: str = typer.Argument(..., help="Manifest name (without .json extension)"),
    format: str = SHOW_MANIFEST_FORMAT_OPTION
) -> None:
    """Show manifest contents."""
    manifest_path = Path(f"metadata/manifests/{name}.json")
    
    if not manifest_path.exists():
        typer.echo(f"Manifest not found: {manifest_path}", err=True)
        raise typer.Exit(code=1)
    
    try:
        import json
        with open(manifest_path, encoding='utf-8') as f:
            data = json.load(f)
        
        if format == "json":
            typer.echo(json.dumps(data, indent=2, ensure_ascii=False))
        elif format == "yaml":
            import yaml
            typer.echo(yaml.dump(data, default_flow_style=False, allow_unicode=True))
        elif format == "table":
            console = Console()
            table = Table(title=f"Manifest: {name}")
            table.add_column("Key", style="cyan")
            table.add_column("Value", style="green")
            
            def add_to_table(obj, prefix=""):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        full_key = f"{prefix}.{key}" if prefix else key
                        if isinstance(value, (dict, list)):
                            add_to_table(value, full_key)
                        else:
                            table.add_row(full_key, str(value))
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        full_key = f"{prefix}[{i}]" if prefix else f"[{i}]"
                        if isinstance(item, (dict, list)):
                            add_to_table(item, full_key)
                        else:
                            table.add_row(full_key, str(item))
            
            add_to_table(data)
            console.print(table)
        else:
            typer.echo(f"Unknown format: {format}", err=True)
            raise typer.Exit(code=1)
            
    except Exception as e:
        typer.echo(f"Failed to read manifest: {e}", err=True)
        raise typer.Exit(code=1) from e

@app.command("list-reports")
def list_reports() -> None:
    """List all available reports."""
    console = Console()
    reports_dir = Path("metadata/reports")
    
    if not reports_dir.exists():
        typer.echo("Reports directory not found: metadata/reports", err=True)
        raise typer.Exit(code=1)
    
    table = Table(title="Available Reports")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Size", style="yellow")
    table.add_column("Modified", style="blue")
    
    report_files = list(reports_dir.glob("*"))
    for report_file in sorted(report_files):
        if report_file.is_file():
            size = report_file.stat().st_size
            modified = datetime.fromtimestamp(report_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            table.add_row(
                report_file.name,
                report_file.suffix[1:] if report_file.suffix else "unknown",
                f"{size:,} bytes",
                modified
            )
    
    console.print(table)
# Target CLI command

@app.command("get-target-data")
def get_target_data(
    *,
    config: Path = CONFIG_OPTION,
    input: Path = TARGET_INPUT_OPTION,
    output_dir: Path | None = TARGET_OUTPUT_DIR_OPTION,
    date_tag: str | None = TARGET_DATE_TAG_OPTION,
    timeout_sec: float | None = DEFAULT_TIMEOUT_OPTION,
    retries: int | None = DEFAULT_RETRIES_OPTION,
    limit: int | None = DEFAULT_LIMIT_OPTION,
    dev_mode: bool = TARGET_DEV_MODE_OPTION,
    dry_run: bool = DEFAULT_DRY_RUN_OPTION,
    log_level: str = DEFAULT_LOG_LEVEL_OPTION,
    log_file: Path | None = DEFAULT_LOG_FILE_OPTION,
    log_format: str = DEFAULT_LOG_FORMAT_OPTION,
) -> None:
    """Extract and enrich target data from ChEMBL/UniProt/IUPHAR."""

    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="target_processing")

    overrides: dict[str, Any] = {}
    if output_dir is not None:
        _assign_path(overrides, ["io", "output", "dir"], str(output_dir))
    # Устанавливаем date_tag: из аргументов или автоматически генерируем
    _assign_path(overrides, ["runtime", "date_tag"], date_tag or _generate_date_tag())
    if timeout_sec is not None:
        _assign_path(overrides, ["http", "global", "timeout_sec"], timeout_sec)
    if retries is not None:
        _assign_path(overrides, ["http", "global", "retries", "total"], retries)
    if limit is not None:
        _assign_path(overrides, ["runtime", "limit"], limit)
    if dev_mode:
        _assign_path(overrides, ["runtime", "dev_mode"], True)
        _assign_path(overrides, ["runtime", "allow_incomplete_sources"], True)
    if dry_run:
        _assign_path(overrides, ["runtime", "dry_run"], True)

    try:
        cfg = load_target_config(config, overrides=overrides)
    except Exception as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc

    logger = configure_logging(
        level=log_level,
        file_enabled=True,
        console_format=log_format,
        log_file=log_file,
        logging_config=cfg.logging.model_dump() if hasattr(cfg, "logging") else None,
    )

    with bind_stage(logger, "target_processing", run_id=run_id) as logger:
        logger.info("Target processing started", run_id=run_id)

        try:
            input_frame = read_target_input(input)
        except TargetValidationError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc
        except TargetIOError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.IO_ERROR) from exc

        register_shutdown_handler(lambda: logger.info("Target processing shutdown", run_id=run_id))

        try:
            with ShutdownContext(timeout=60.0):
                with bind_stage(logger, "target_etl"):
                    result = run_target_etl(cfg, input_frame=input_frame)
        except TargetValidationError as exc:
            logger.error("Target validation failed", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc
        except TargetHTTPError as exc:
            logger.error("Target HTTP error", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.HTTP_ERROR) from exc
        except TargetQCError as exc:
            logger.error("Target QC error", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.QC_ERROR) from exc

        if cfg.runtime.dry_run:
            logger.info("Dry run completed; no artefacts written.", run_id=run_id)
            typer.echo("Dry run completed; no artefacts written.")
            raise typer.Exit(code=ExitCode.OK)

        outputs = write_target_outputs(
            result,
            cfg.io.output.dir,
            cfg.runtime.date_tag,
            cfg,
        )

        for name, path in outputs.items():
            typer.echo(f"{name}: {path}")


# Activity CLI command
@app.command("get-activity-data")
def get_activity_data(
    *,
    config: Path = CONFIG_OPTION,
    input: Path | None = ACTIVITY_INPUT_OPTION,
    output_dir: Path | None = TARGET_OUTPUT_DIR_OPTION,
    date_tag: str | None = TARGET_DATE_TAG_OPTION,
    timeout_sec: float | None = DEFAULT_TIMEOUT_OPTION,
    retries: int | None = DEFAULT_RETRIES_OPTION,
    limit: int | None = DEFAULT_LIMIT_OPTION,
    dry_run: bool = DEFAULT_DRY_RUN_OPTION,
    log_level: str = DEFAULT_LOG_LEVEL_OPTION,
    log_file: Path | None = DEFAULT_LOG_FILE_OPTION,
    log_format: str = DEFAULT_LOG_FORMAT_OPTION,
) -> None:
    """Extract and enrich activity data from ChEMBL."""

    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="activity_processing")

    try:
        # Load configuration
        activity_config = ActivityConfig.from_yaml(config)
        
        # Override configuration with CLI arguments
        if output_dir is not None:
            # aligned with new ActivityConfig fields
            activity_config.io.output.dir = output_dir
        if date_tag is not None:
            # Add date tag to output directory
            activity_config.runtime.date_tag = date_tag
        if timeout_sec is not None:
            activity_config.timeout_sec = timeout_sec
        if retries is not None:
            activity_config.max_retries = retries
        if limit is not None:
            activity_config.runtime.limit = limit
        if dry_run:
            activity_config.dry_run = dry_run
        
    except Exception as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc

    logger = configure_logging(
        level=log_level,
        file_enabled=True,
        console_format=log_format,
        log_file=log_file,
    )

    with bind_stage(logger, "activity_processing", run_id=run_id) as logger:
        logger.info("Activity processing started", run_id=run_id)

        register_shutdown_handler(lambda: logger.info("Activity processing shutdown", run_id=run_id))

        try:
            with ShutdownContext(timeout=60.0):
                with bind_stage(logger, "activity_etl"):
                    result = run_activity_etl(activity_config, input_csv=input, logger=logger)
                    
            logger.info("Activity processing completed", run_id=run_id, records=len(result.activities))
            
        except Exception as exc:
            logger.error("Activity processing failed", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=ExitCode.VALIDATION_ERROR) from exc

        if activity_config.dry_run:
            logger.info("Dry run completed; no artefacts written.", run_id=run_id)
            typer.echo("Dry run completed; no artefacts written.")
            raise typer.Exit(code=ExitCode.OK)

        # Write outputs
        try:
            from library.activity.writer import write_activity_outputs
            with bind_stage(logger, "write_outputs"):
                output_paths = write_activity_outputs(
                    result=result,
                    output_dir=output_dir or activity_config.get_output_path(),
                    date_tag=date_tag or (activity_config.runtime.date_tag or ""),
                    config=activity_config,
                )
                for name, path in output_paths.items():
                    typer.echo(f"{name}: {path}")

            logger.info("Outputs written successfully", run_id=run_id)

        except Exception as exc:
            logger.error("Failed to write outputs", error=str(exc), run_id=run_id, exc_info=True)
            typer.echo(f"Failed to write outputs: {exc}", err=True)
            raise typer.Exit(code=ExitCode.IO_ERROR) from exc


@app.command()
def pipeline(
    config: Path = CONFIG_OPTION,
    overrides: list[str] = PIPELINE_OVERRIDES_OPTION,
    log_level: str = DEFAULT_LOG_LEVEL_OPTION,
    log_file: Path | None = DEFAULT_LOG_FILE_OPTION,
    log_format: str = DEFAULT_LOG_FORMAT_OPTION,
    no_file_log: bool = DEFAULT_NO_FILE_LOG_OPTION,
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
        logging_config=config_model.logging.model_dump() if hasattr(config_model, 'logging') else None,
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
    config: Path | None = DOCUMENT_CONFIG_OPTION,
    documents_csv: Path | None = DOCUMENT_CSV_OPTION,
    output_dir: Path | None = DOCUMENT_OUTPUT_DIR_OPTION,
    date_tag: str | None = DOCUMENT_DATE_TAG_OPTION,
    timeout_sec: float | None = DEFAULT_TIMEOUT_OPTION,
    retries: int | None = DEFAULT_RETRIES_OPTION,
    workers: int | None = DOCUMENT_WORKERS_OPTION,
    limit: int | None = DEFAULT_LIMIT_OPTION,
    sources: list[str] = DOCUMENT_SOURCES_OPTION,
    all_sources: bool = DOCUMENT_ALL_SOURCES_OPTION,
    dry_run: bool | None = DOCUMENT_DRY_RUN_OPTION,
    log_level: str = DEFAULT_LOG_LEVEL_OPTION,
    log_file: Path | None = DEFAULT_LOG_FILE_OPTION,
    log_format: str = DEFAULT_LOG_FORMAT_OPTION,
    no_file_log: bool = DEFAULT_NO_FILE_LOG_OPTION,
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
        logging_config=config_model.logging.model_dump() if hasattr(config_model, 'logging') else None,
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
    limit: int = TESTITEM_LIMIT_OPTION,
    disable_pubchem: bool = TESTITEM_DISABLE_PUBCHEM_OPTION,
    dry_run: bool = TESTITEM_DRY_RUN_OPTION,
    verbose: bool = TESTITEM_VERBOSE_OPTION,
    date_tag: str = TESTITEM_DATE_TAG_OPTION,
) -> None:
    """Run the testitem ETL pipeline."""
    
    try:
        # Load configuration
        console.print(f"[blue]Loading configuration from: {config}[/blue]")
        testitem_config = TestitemConfig.from_file(config)
        
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
            config_output = getattr(testitem_config.io.output, 'dir', 'data/output/testitem')
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
        output_paths = write_testitem_outputs(result, output, testitem_config)
        
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
        testitem_config = TestitemConfig.from_file(config)
        
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


@app.command("analyze-results")
def analyze_results_cmd(
    csv_path: Path = typer.Argument(..., help="Path to CSV file"),
    output_format: str = typer.Option("table", help="Output format: table/json")
) -> None:
    """Analyze pipeline results and generate statistics."""
    
    console = Console()
    
    try:
        import pandas as pd
        
        console.print(f"[blue]Loading CSV file: {csv_path}[/blue]")
        df = pd.read_csv(csv_path)
        
        total_rows = len(df)
        total_columns = len(df.columns)
        
        console.print(f"[green]=== Pipeline Results Analysis ===[/green]")
        console.print(f"Total rows: {total_rows}")
        console.print(f"Total columns: {total_columns}")
        
        # Analysis by sources
        console.print("\n[blue]Analysis by Sources:[/blue]")
        
        source_columns = {
            'ChEMBL': [col for col in df.columns if col.startswith('chembl_')],
            'Crossref': [col for col in df.columns if col.startswith('crossref_')],
            'OpenAlex': [col for col in df.columns if col.startswith('openalex_')],
            'PubMed': [col for col in df.columns if col.startswith('pubmed_')],
            'Semantic Scholar': [col for col in df.columns if col.startswith('semantic_scholar_')],
            'Basic': ['document_chembl_id', 'title', 'doi', 'document_pubmed_id', 'journal', 'year', 'volume', 'issue', 'first_page', 'last_page', 'month', 'abstract', 'pubmed_authors']
        }
        
        for source, columns in source_columns.items():
            if not columns:
                continue
                
            filled_count = 0
            total_cells = 0
            for col in columns:
                if col in df.columns:
                    filled_count += int(df[col].notna().sum())
                    total_cells += total_rows
            
            if total_cells > 0:
                fill_percentage = (filled_count / total_cells) * 100
                console.print(f"{source:<20} {filled_count:4d}/{total_cells:4d} cells ({fill_percentage:5.1f}%)")
        
        # Overall statistics
        console.print("\n[blue]Overall Statistics:[/blue]")
        
        total_cells = total_rows * total_columns
        filled_cells = int(df.notna().sum().sum())
        overall_fill_percentage = (filled_cells / total_cells) * 100
        
        console.print(f"Total cells: {total_cells:,}")
        console.print(f"Filled cells: {filled_cells:,}")
        console.print(f"Overall fill percentage: {overall_fill_percentage:.1f}%")
        
        # Fully filled rows
        fully_filled_mask = df.notna().all(axis=1)
        fully_filled_rows = int(fully_filled_mask.sum())  # type: ignore
        console.print(f"Fully filled rows: {fully_filled_rows}/{total_rows} ({fully_filled_rows/total_rows*100:.1f}%)")
        
        # Fill statistics per row
        min_fill_per_row = int(df.notna().sum(axis=1).min())
        max_fill_per_row = int(df.notna().sum(axis=1).max())
        avg_fill_per_row = float(df.notna().sum(axis=1).mean())
        
        console.print(f"Min fill per row: {min_fill_per_row}/{total_columns} ({min_fill_per_row/total_columns*100:.1f}%)")
        console.print(f"Max fill per row: {max_fill_per_row}/{total_columns} ({max_fill_per_row/total_columns*100:.1f}%)")
        console.print(f"Avg fill per row: {avg_fill_per_row:.1f}/{total_columns} ({avg_fill_per_row/total_columns*100:.1f}%)")
        
        # Output results in requested format
        if output_format == "json":
            import json
            
            results = {
                'total_rows': total_rows,
                'total_columns': total_columns,
                'total_cells': total_cells,
                'filled_cells': filled_cells,
                'overall_fill_percentage': overall_fill_percentage,
                'fully_filled_rows': fully_filled_rows,
                'fill_stats_per_row': {
                    'min': min_fill_per_row,
                    'max': max_fill_per_row,
                    'avg': avg_fill_per_row
                }
            }
            console.print("\n[blue]JSON Output:[/blue]")
            console.print(json.dumps(results, indent=2, ensure_ascii=False))
        
        console.print("\n[green]Analysis completed successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]Error during analysis: {e}[/red]")
        raise typer.Exit(1) from e


@app.command("check-fields")
def check_fields_cmd(
    csv_path: Path = typer.Argument(..., help="Path to CSV file"),
    fields: list[str] = typer.Option(None, help="Specific fields to check")
) -> None:
    """Check field fill rates in pipeline output."""
    
    console = Console()
    
    try:
        import pandas as pd
        
        console.print(f"[blue]Loading CSV file: {csv_path}[/blue]")
        df = pd.read_csv(csv_path)
        
        console.print(f"[green]=== Field Fill Rate Analysis ===[/green]")
        console.print(f"Total rows: {len(df)}")
        
        # Fields to check
        fields_to_check = {
            'Crossref': ['crossref_title', 'crossref_doc_type', 'crossref_subject', 'doi_key'],
            'OpenAlex': ['openalex_title', 'openalex_doc_type', 'openalex_year', 'openalex_doi'],
            'PubMed': ['pubmed_pmid', 'pubmed_doi', 'pubmed_article_title', 'pubmed_abstract', 'pubmed_journal'],
            'Semantic Scholar': ['semantic_scholar_pmid', 'semantic_scholar_doi', 'semantic_scholar_journal', 'semantic_scholar_doc_type']
        }
        
        # If specific fields are provided, check only those
        if fields:
            fields_to_check = {'Custom': fields}
        
        for source, field_list in fields_to_check.items():
            console.print(f"\n[blue]{source.upper()}:[/blue]")
            
            for field in field_list:
                if field in df.columns:
                    filled = int(df[field].notna().sum())
                    total = len(df)
                    percentage = (filled / total) * 100
                    console.print(f"  {field:<30} {filled:2d}/{total:2d} ({percentage:5.1f}%)")
                else:
                    console.print(f"  {field:<30} NOT FOUND")
        
        console.print("\n[green]Field analysis completed successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]Error during field analysis: {e}[/red]")
        raise typer.Exit(1) from e


@app.command()
def health(
    config: Path = CONFIG_OPTION,
    timeout: float = HEALTH_TIMEOUT_OPTION,
    json_output: bool = HEALTH_JSON_OUTPUT_OPTION,
    log_level: str = DEFAULT_LOG_LEVEL_OPTION,
    log_file: Path | None = DEFAULT_LOG_FILE_OPTION,
    no_file_log: bool = DEFAULT_NO_FILE_LOG_OPTION,
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
            logging_config=config_model.logging.model_dump() if hasattr(config_model, 'logging') else None,
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
            
            logger.info(
                "Health check completed",
                run_id=run_id,
                total_apis=len(statuses),
                healthy=healthy_count,
                unhealthy=unhealthy_count
            )
            
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
    
    # Validate shell parameter
    allowed_shells = {"bash", "zsh", "fish", "powershell"}
    if shell not in allowed_shells:
        typer.echo(f"❌ Unsupported shell: {shell}")
        typer.echo(f"Supported shells: {', '.join(allowed_shells)}")
        raise typer.Exit(1)
    
    try:
        # Generate completion script
        # Security: executable is validated above, script_name is validated, shell is from allowed list
        # All parameters are controlled and validated to prevent command injection
        cmd = [
            executable, "-m", "typer", "library.cli:app", 
            f"--name={script_name}", 
            shell,
            "--output-file", "-"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)  # noqa: S603
        
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
