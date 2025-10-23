#!/usr/bin/env python3
"""CLI для извлечения данных документов из различных источников."""

import argparse
import sys
from pathlib import Path
from typing import Any

from library.documents import load_document_config, DocumentPipeline, write_document_outputs
from library.logging_setup import configure_logging


def _load_document_data(input_path: Path, id_column: str | None = None) -> list[dict[str, Any]]:
    """Load document data from CSV file.
    
    Args:
        input_path: Path to CSV file
        id_column: Name of column containing document IDs (if None, checks for document_chembl_id, doi, title)
    """
    import pandas as pd
    
    if input_path.suffix.lower() == '.csv':
        df = pd.read_csv(input_path)
        
        # Если указана конкретная колонка, проверяем только её
        if id_column:
            if id_column not in df.columns:
                raise ValueError(f"CSV file must contain '{id_column}' column: {input_path}. Found: {list(df.columns)}")
        else:
            # Проверяем наличие обязательных колонок (проверяем как doi, так и DOI)
            required_columns = {'document_chembl_id'}
            missing_columns = required_columns - set(df.columns)
            
            # Проверяем наличие колонки DOI (в любом регистре)
            doi_column = None
            for col in df.columns:
                if col.lower() == 'doi':
                    doi_column = col
                    break
            
            if not doi_column:
                missing_columns.add('doi')
            
            if missing_columns:
                raise ValueError(f"CSV file must contain columns: {', '.join(missing_columns)}. Found: {list(df.columns)}")
        
        return df.to_dict('records')
    
    else:
        raise ValueError(f"Unsupported file format: {input_path.suffix}")


def _generate_date_tag() -> str:
    """Generate date tag for output files."""
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract document data from various sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract documents from CSV file
  python get_document_data.py --input documents.csv --config configs/config_documents_full.yaml
  
  # Limit number of documents
  python get_document_data.py --input documents.csv --limit 100 --config configs/config_documents_full.yaml
  
  # Use specific sources only
  python get_document_data.py --input documents.csv --sources chembl,pubmed --config configs/config_documents_full.yaml
  
  # Enable correlation analysis
  python get_document_data.py --input documents.csv --correlation --config configs/config_documents_full.yaml
        """
    )
    
    # Input data
    parser.add_argument(
        "--input", 
        type=Path,
        required=True,
        help="Path to CSV file with document data (must contain document_chembl_id, doi columns)"
    )
    
    # Sources
    parser.add_argument(
        "--sources",
        type=str,
        help="Comma-separated list of sources to use (chembl,crossref,openalex,pubmed,semantic_scholar)"
    )
    
    # Limitations
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of documents to process"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Request timeout in seconds"
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=10,
        help="Number of retry attempts"
    )
    
    # Post-processing options
    parser.add_argument(
        "--correlation",
        action="store_true",
        help="Enable correlation analysis"
    )
    parser.add_argument(
        "--journal-normalization",
        action="store_true",
        help="Enable journal name normalization"
    )
    parser.add_argument(
        "--citation-formatting",
        action="store_true",
        help="Enable citation formatting"
    )
    
    # Output format
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        help="Output format (overrides config)"
    )
    
    # Configuration
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config_documents_full.yaml"),
        help="Configuration file path"
    )
    
    # Output directory
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory (overrides config)"
    )
    
    # Date tag
    parser.add_argument(
        "--date-tag",
        type=str,
        help="Date tag for output files (default: auto-generated)"
    )
    
    # Logging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level"
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file (default: stdout)"
    )
    parser.add_argument(
        "--log-format",
        choices=["text", "json"],
        default="text",
        help="Console log format (default: text)"
    )
    
    # Dry run
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate execution without making API calls"
    )
    
    args = parser.parse_args()
    
    try:
        # Setup logging
        configure_logging(
            level=args.log_level,
            file_enabled=args.log_file is not None,
            console_format=args.log_format,
            log_file=args.log_file,
        )
        
        # Load configuration
        config = load_document_config(args.config)
        
        # Apply CLI overrides
        if args.timeout:
            config.http.global_.timeout_sec = args.timeout
        if args.retries:
            config.http.global_.retries.total = args.retries
        if args.limit:
            config.runtime.limit = args.limit
        if args.format:
            config.io.output.format = args.format
        if args.output_dir:
            config.io.output.dir = args.output_dir
        if args.dry_run:
            config.runtime.dry_run = True
        
        # Apply source overrides
        if args.sources:
            source_list = [s.strip() for s in args.sources.split(',')]
            valid_sources = {'chembl', 'crossref', 'openalex', 'pubmed', 'semantic_scholar'}
            invalid_sources = set(source_list) - valid_sources
            if invalid_sources:
                print(f"Error: Invalid sources: {', '.join(invalid_sources)}. Valid sources: {', '.join(valid_sources)}", file=sys.stderr)
                return 2
            
            # Disable all sources first
            for source in valid_sources:
                if hasattr(config.sources, source):
                    getattr(config.sources, source).enabled = False
            
            # Enable only specified sources
            for source in source_list:
                if hasattr(config.sources, source):
                    getattr(config.sources, source).enabled = True
        
        # Apply post-processing overrides
        if args.correlation:
            config.postprocess.correlation.enabled = True
        if args.journal_normalization:
            config.postprocess.journal_normalization.enabled = True
        if args.citation_formatting:
            config.postprocess.citation_formatting.enabled = True
        
        # Generate date tag if not provided
        date_tag = args.date_tag or _generate_date_tag()
        
        # Validate date tag format
        if not date_tag.isdigit() or len(date_tag) != 8:
            print(f"Error: Invalid date tag format '{date_tag}'. Expected YYYYMMDD.", file=sys.stderr)
            return 2
        
        # Load input data
        try:
            id_column = getattr(config.io.input, 'document_id_column', None)
            document_data = _load_document_data(args.input, id_column=id_column)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 2
        if not document_data:
            print("Error: No document data found in input file.", file=sys.stderr)
            return 2
        
        # Apply limit to input data if specified
        if config.runtime.limit and config.runtime.limit > 0:
            original_count = len(document_data)
            document_data = document_data[:config.runtime.limit]
            print(f"Processing {len(document_data)} documents from {args.input} (limited from {original_count})")
        else:
            print(f"Processing {len(document_data)} documents from {args.input}")
        
        # Convert to DataFrame
        import pandas as pd
        frame = pd.DataFrame(document_data)
        
        # Run pipeline
        pipeline = DocumentPipeline(config)
        result = pipeline.run(input_data=frame)
        
        # Write outputs
        if not config.runtime.dry_run:
            output_paths = write_document_outputs(
                result=result,
                output_dir=config.io.output.dir,
                date_tag=date_tag,
                config=config
            )
            
            print("\nPipeline completed successfully. Output files:")
            for name, path in output_paths.items():
                print(f"  {name}: {path}")
            
            # Print summary
            print("\nSummary:")
            print(f"  Total documents: {len(result.data)}")
            if result.metadata:
                pipeline_info = result.metadata.pipeline
                print(f"  Pipeline version: {pipeline_info.get('version', 'unknown')}")
                enabled_sources = [source.name for source in result.metadata.sources]
                print(f"  Enabled sources: {', '.join(enabled_sources) if enabled_sources else 'none'}")
            else:
                print("  Pipeline version: unknown")
                print("  Enabled sources: unknown")
            print(f"  Date tag: {date_tag}")
            
            # Print source statistics
            if result.metadata:
                print("\nSource statistics:")
                for source in result.metadata.sources:
                    print(f"  {source.name}: {source.records_fetched} records")
        else:
            print("Dry run completed. No files were written.")
        
        return 0
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 3  # API error


if __name__ == "__main__":
    sys.exit(main())