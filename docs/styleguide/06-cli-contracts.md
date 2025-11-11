# CLI Contracts

This document defines the standards for command-line interface (CLI) contracts in the `bioetl` project. All CLI commands **MUST** follow these standards.

## Principles

- **Explicit Flags**: Use explicit flags instead of positional arguments where ambiguity exists.
- **Input Validation**: All inputs **MUST** be validated before processing.
- **Clear Error Messages**: Error messages **MUST** be clear and actionable.
- **Exit Codes**: Use standardized exit codes for different failure modes.
- **Idempotency**: Commands **SHOULD** be idempotent where possible.

## Typer-Based Interface

All CLI commands **MUST** use Typer for consistency:

```python
import typer
from pathlib import Path

app = typer.Typer()

@app.command()
def run_pipeline(
    config_path: Path = typer.Option(..., "--config", "-c", help="Path to config file"),
    output_dir: Path = typer.Option(..., "--output", "-o", help="Output directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
) -> None:
    """Run pipeline with specified configuration."""
    # Command implementation
    pass
```

## Explicit Flags

### Preferred: Named Arguments

Use explicit flags for clarity, especially for optional parameters:

```python
@app.command()
def extract_activity(
    source: str = typer.Option(..., "--source", "-s", help="Data source"),
    batch_size: int = typer.Option(1000, "--batch-size", "-b", help="Batch size"),
    output: Path = typer.Option(..., "--output", "-o", help="Output path"),
) -> None:
    """Extract activity data."""
    pass
```

### Avoid: Ambiguous Positional Arguments

```python
# Invalid: ambiguous positional arguments
@app.command()
def extract_activity(source: str, batch_size: int, output: Path) -> None:
    """Extract activity data."""  # SHALL NOT use ambiguous positionals
    pass
```

## Input Validation

All inputs **MUST** be validated before processing:

### Valid Examples

```python
from pathlib import Path
import typer

@app.command()
def run_pipeline(
    config_path: Path = typer.Option(..., "--config"),
    output_dir: Path = typer.Option(..., "--output"),
) -> None:
    """Run pipeline with validation."""
    # Validate config file exists
    if not config_path.exists():
        raise typer.BadParameter(f"Config file not found: {config_path}")

    # Validate output directory is writable
    if not output_dir.parent.exists():
        raise typer.BadParameter(f"Output directory parent does not exist: {output_dir.parent}")

    # Process pipeline
    process_pipeline(config_path, output_dir)
```

## Exit Codes

CLI commands **MUST** use standardized exit codes:

- **0**: Success
- **1**: General error (validation, processing failure)
- **2**: Configuration error (invalid config, missing files)
- **3**: External dependency error (API failure, network error)

### Valid Examples

```python
import sys
import typer

@app.command()
def run_pipeline(config_path: Path) -> None:
    """Run pipeline with proper exit codes."""
    try:
        # Validate config
        if not config_path.exists():
            typer.echo(f"Error: Config file not found: {config_path}", err=True)
            raise typer.Exit(code=2)

        # Process pipeline
        result = process_pipeline(config_path)
        if not result.success:
            typer.echo(f"Error: Pipeline failed: {result.error}", err=True)
            raise typer.Exit(code=1)

        typer.echo("Pipeline completed successfully")
        raise typer.Exit(code=0)

    except APIError as e:
        typer.echo(f"Error: API failure: {e}", err=True)
        raise typer.Exit(code=3)
```

## Error Messages

Error messages **MUST** be clear and actionable:

### Valid Examples

```python
# Valid: clear error message
if not config_path.exists():
    typer.echo(
        f"Error: Configuration file not found: {config_path}\n"
        f"Please provide a valid path using --config or -c flag.",
        err=True
    )
    raise typer.Exit(code=2)
```

### Invalid Examples

```python
# Invalid: unclear error message
if not config_path.exists():
    typer.echo("Error: File not found", err=True)  # Too vague
    raise typer.Exit(code=1)
```

## Idempotency

Commands **SHOULD** be idempotent where possible:

```python
@app.command()
def export_data(
    output_path: Path = typer.Option(..., "--output"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing file"),
) -> None:
    """Export data (idempotent)."""
    if output_path.exists() and not overwrite:
        typer.echo(f"File already exists: {output_path}")
        typer.echo("Use --overwrite to replace it")
        raise typer.Exit(code=1)

    export_data_to_file(output_path)
```

## Help Text

All commands and options **MUST** have clear help text:

```python
@app.command(
    name="extract-activity",
    help="Extract activity data from ChEMBL database"
)
def extract_activity(
    source: str = typer.Option(
        ...,
        "--source",
        "-s",
        help="Data source identifier (e.g., 'chembl', 'pubchem')"
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        "-b",
        help="Number of records per batch (default: 1000)"
    ),
) -> None:
    """Extract activity data."""
    pass
```

## Progress Indication

Long-running commands **SHOULD** provide progress indication:

```python
import typer
from rich.progress import Progress

@app.command()
def process_large_dataset(input_path: Path) -> None:
    """Process large dataset with progress bar."""
    with Progress() as progress:
        task = progress.add_task("Processing...", total=100)
        process_data(input_path, progress_callback=lambda p: progress.update(task, completed=p))
```

## Command Structure

### Valid Examples

```python
import typer
from pathlib import Path

app = typer.Typer(help="BioETL command-line interface")

@app.command()
def run_pipeline(
    config: Path = typer.Option(..., "--config", "-c", help="Path to configuration file"),
    output: Path = typer.Option(..., "--output", "-o", help="Output directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Run ETL pipeline with specified configuration."""
    try:
        # Validation
        validate_config(config)
        validate_output_dir(output)

        # Execution
        result = execute_pipeline(config, output, verbose=verbose)

        if result.success:
            typer.echo("Pipeline completed successfully")
            raise typer.Exit(code=0)
        else:
            typer.echo(f"Pipeline failed: {result.error}", err=True)
            raise typer.Exit(code=1)

    except ValidationError as e:
        typer.echo(f"Validation error: {e}", err=True)
        raise typer.Exit(code=2)
    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
```

## References

- CLI documentation: [`docs/cli/`](../cli/)
- Typer documentation: https://typer.tiangolo.com/
- Exit codes: [`docs/cli/02-cli-exit-codes.md`](../cli/02-cli-exit-codes.md)
