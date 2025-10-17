# CLI Integration Snippet for Typer Commands
# This shows how to integrate the new logging system with CLI commands

import typer
from pathlib import Path
from library.logging_setup import configure_logging, generate_run_id, set_run_context, bind_stage

# Example CLI command with logging integration
@app.command()
def pipeline(
    config: Path = typer.Option(..., "--config", "-c", help="Path to configuration file"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"),
    log_file: Path | None = typer.Option(None, "--log-file", help="Path to log file"),
    log_format: str = typer.Option("text", "--log-format", help="Console format (text or json)"),
    no_file_log: bool = typer.Option(False, "--no-file-log", help="Disable file logging"),
    overrides: list[str] = typer.Option([], "--set", "-s", help="Override configuration values"),
) -> None:
    """Execute the ETL pipeline with enhanced logging."""
    
    # Generate unique run ID for this execution
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="cli_startup")
    
    # Configure logging with CLI parameters
    logger = configure_logging(
        level=log_level,
        file_enabled=not no_file_log,
        console_format=log_format,
        log_file=log_file,
    )
    
    # Bind logger with run context
    logger = bind_stage(logger, "pipeline", run_id=run_id)
    
    logger.info("Pipeline started", run_id=run_id, config=str(config))
    
    try:
        # Load configuration
        config_model = Config.load(config, overrides=override_dict)
        
        # Update logging level from config if not overridden by CLI
        if log_level == "INFO":  # Default value, check config
            logger = configure_logging(
                level=config_model.logging.level,
                file_enabled=not no_file_log,
                console_format=log_format,
                log_file=log_file,
            )
            logger = bind_stage(logger, "pipeline", run_id=run_id)
        
        # Execute pipeline
        with bind_stage(logger, "etl_execution"):
            output = run_pipeline(config_model, logger)
            
        logger.info("Pipeline completed successfully", output=str(output), run_id=run_id)
        typer.echo(f"Pipeline completed. Output written to {output}")
        
    except Exception as exc:
        logger.error("Pipeline failed", error=str(exc), run_id=run_id, exc_info=True)
        typer.echo(f"Pipeline failed: {exc}", err=True)
        raise typer.Exit(1) from exc


# Example of integrating with existing document processing command
@app.command("get-document-data")
def get_document_data(
    config: Path | None = typer.Option(None, "--config", "-c", help="Path to configuration file"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
    log_file: Path | None = typer.Option(None, "--log-file", help="Path to log file"),
    no_file_log: bool = typer.Option(False, "--no-file-log", help="Disable file logging"),
    # ... other existing parameters
) -> None:
    """Collect and enrich document metadata with enhanced logging."""
    
    # Generate run ID and set context
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="document_processing")
    
    # Configure logging
    logger = configure_logging(
        level=log_level,
        file_enabled=not no_file_log,
        log_file=log_file,
    )
    logger = bind_stage(logger, "document_processing", run_id=run_id)
    
    logger.info("Document processing started", run_id=run_id)
    
    try:
        # Load configuration
        config_model = load_document_config(config, overrides=overrides)
        
        # Process documents with stage binding
        with bind_stage(logger, "document_etl"):
            result = run_document_etl(config_model, input_frame)
            
        logger.info("Document processing completed", run_id=run_id, records=len(result.documents))
        
    except Exception as exc:
        logger.error("Document processing failed", error=str(exc), run_id=run_id, exc_info=True)
        raise typer.Exit(1) from exc


# Example of health check command with logging
@app.command()
def health(
    config: Path = typer.Option(..., "--config", "-c", help="Path to configuration file"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
    timeout: float = typer.Option(10.0, "--timeout", "-t", help="Timeout for health checks"),
    json_output: bool = typer.Option(False, "--json", help="Output results in JSON format"),
) -> None:
    """Check health status with enhanced logging."""
    
    # Generate run ID for health check
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="health_check")
    
    # Configure logging
    logger = configure_logging(level=log_level, file_enabled=True)
    logger = bind_stage(logger, "health_check", run_id=run_id)
    
    logger.info("Health check started", run_id=run_id, timeout=timeout)
    
    try:
        # Load configuration and perform health checks
        config_model = Config.load(config)
        health_checker = create_health_checker_from_config(api_configs)
        
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
        
        # Output results
        if json_output:
            import json
            summary = health_checker.get_health_summary(statuses)
            typer.echo(json.dumps(summary, indent=2))
        else:
            health_checker.print_health_report(statuses)
            
        # Exit with error code if any APIs are unhealthy
        if unhealthy_count > 0:
            raise typer.Exit(1)
            
    except Exception as exc:
        logger.error("Health check failed", error=str(exc), run_id=run_id, exc_info=True)
        raise typer.Exit(1) from exc
