#!/usr/bin/env python3
"""CLI для извлечения данных молекул из ChEMBL с обогащением из PubChem."""

import argparse
import sys
from pathlib import Path
from typing import Any

from library.testitem.config import TestitemConfig
from library.testitem.pipeline import run_testitem_etl, write_testitem_outputs
from library.logging_setup import configure_logging


def _load_testitem_data(input_path: Path) -> list[dict[str, Any]]:
    """Load testitem data from CSV file."""
    import pandas as pd
    
    if input_path.suffix.lower() == '.csv':
        df = pd.read_csv(input_path)
        
        # Проверяем наличие обязательных колонок (хотя бы одна должна быть)
        required_columns = {'molecule_chembl_id', 'molregno'}
        has_required = any(col in df.columns for col in required_columns)
        if not has_required:
            raise ValueError(f"CSV file must contain at least one of: {', '.join(required_columns)}. Found: {list(df.columns)}")
        
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
        description="Extract molecule data from ChEMBL with PubChem enrichment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract molecules from CSV file
  python get_testitem_data.py --input molecules.csv --config configs/config_testitem_full.yaml
  
  # Limit number of molecules
  python get_testitem_data.py --input molecules.csv --limit 100 --config configs/config_testitem_full.yaml
  
  # Disable PubChem enrichment
  python get_testitem_data.py --input molecules.csv --disable-pubchem --config configs/config_testitem_full.yaml
  
  # Use custom output directory
  python get_testitem_data.py --input molecules.csv --output-dir results/ --config configs/config_testitem_full.yaml
        """
    )
    
    # Input data
    parser.add_argument(
        "--input", 
        type=Path,
        required=True,
        help="Path to CSV file with molecule data (must contain molecule_chembl_id or molregno columns)"
    )
    
    # PubChem options
    parser.add_argument(
        "--disable-pubchem",
        action="store_true",
        help="Disable PubChem enrichment"
    )
    
    # Limitations
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of molecules to process"
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
    
    # Cache directories
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help="Cache directory for ChEMBL data"
    )
    parser.add_argument(
        "--pubchem-cache-dir",
        type=Path,
        help="Cache directory for PubChem data"
    )
    
    # Configuration
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config_testitem_full.yaml"),
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
    
    # Dry run
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate execution without making API calls"
    )
    
    args = parser.parse_args()
    
    try:
        # Setup logging
        configure_logging(level=args.log_level)
        
        # Load configuration
        config = TestitemConfig.from_file(args.config)
        
        # Apply CLI overrides
        if args.timeout:
            config.http.global_.timeout_sec = args.timeout
        if args.retries:
            config.http.global_.retries.total = args.retries
        if args.limit:
            config.runtime.limit = args.limit
        if args.output_dir:
            # Update output paths to use the new directory
            config.io.output.data_path = args.output_dir / "testitem.csv"
            config.io.output.qc_report_path = args.output_dir / "testitem_qc.csv"
            config.io.output.correlation_path = args.output_dir / "testitem_correlation.csv"
        if args.cache_dir:
            config.runtime.cache_dir = str(args.cache_dir)
        if args.pubchem_cache_dir:
            config.runtime.pubchem_cache_dir = str(args.pubchem_cache_dir)
        if args.disable_pubchem:
            config.enable_pubchem = False
        if args.dry_run:
            config.runtime.dry_run = True
        
        # Generate date tag if not provided
        date_tag = args.date_tag or _generate_date_tag()
        
        # Validate date tag format
        if not date_tag.isdigit() or len(date_tag) != 8:
            print(f"Error: Invalid date tag format '{date_tag}'. Expected YYYYMMDD.", file=sys.stderr)
            return 2
        
        # Load input data
        molecule_data = _load_testitem_data(args.input)
        if not molecule_data:
            print("Error: No molecule data found in input file.", file=sys.stderr)
            return 2
        
        # Apply limit to input data if specified
        if config.runtime.limit and config.runtime.limit > 0:
            original_count = len(molecule_data)
            molecule_data = molecule_data[:config.runtime.limit]
            print(f"Processing {len(molecule_data)} molecules from {args.input} (limited from {original_count})")
        else:
            print(f"Processing {len(molecule_data)} molecules from {args.input}")
        
        # Convert to DataFrame
        import pandas as pd
        frame = pd.DataFrame(molecule_data)
        
        # Run pipeline
        result = run_testitem_etl(config=config, input_path=args.input)
        
        # Write outputs
        if not config.runtime.dry_run:
            output_paths = write_testitem_outputs(
                result=result,
                output_dir=Path(config.io.output.data_path).parent,
                config=config
            )
            
            print("\nPipeline completed successfully. Output files:")
            for name, path in output_paths.items():
                print(f"  {name}: {path}")
            
            # Print summary
            print("\nSummary:")
            # Используем фактический размер результирующего датафрейма
            print(f"  Total molecules: {len(result.testitems)}")
            print(f"  Pipeline version: {result.meta.get('pipeline_version', 'unknown')}")
            print(f"  ChEMBL release: {result.meta.get('chembl_release', 'unknown')}")
            # Отображаем состояние PubChem по конфигурации запуска
            print(f"  PubChem enabled: {config.enable_pubchem}")
            print(f"  Date tag: {date_tag}")
            
            # Print source statistics
            source_counts = result.meta.get('source_counts', {})
            print("\nSource statistics:")
            for source, count in source_counts.items():
                print(f"  {source}: {count} records")
            
            # Print PubChem enrichment statistics
            pubchem_stats = result.meta.get('pubchem_enrichment', {})
            if pubchem_stats.get('enabled', False):
                enrichment_rate = pubchem_stats.get('enrichment_rate', 0)
                records_with_pubchem = pubchem_stats.get('records_with_pubchem_data', 0)
                print(f"  PubChem enrichment rate: {enrichment_rate:.1%}")
                print(f"  Records with PubChem data: {records_with_pubchem}")
            
            # Print data quality metrics
            data_quality = result.meta.get('data_quality', {})
            if data_quality:
                error_rate = data_quality.get('error_rate', 0)
                records_with_errors = data_quality.get('records_with_errors', 0)
                print(f"  Data quality - Error rate: {error_rate:.1%}")
                print(f"  Data quality - Records with errors: {records_with_errors}")
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