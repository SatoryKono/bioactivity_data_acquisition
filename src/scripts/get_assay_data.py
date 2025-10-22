#!/usr/bin/env python3
"""CLI для извлечения данных ассев из ChEMBL."""

import argparse
import sys
from pathlib import Path
from typing import Any

from library.assay import AssayConfig, AssayPipeline, load_assay_config, write_assay_outputs
from library.logging_setup import configure_logging


def _load_assay_ids(input_path: Path, id_column: str | None = None) -> list[str]:
    """Load assay IDs from CSV/JSON file.
    
    Args:
        input_path: Path to CSV/JSON file
        id_column: Name of column containing assay IDs (default: 'assay_chembl_id')
    """
    import pandas as pd
    
    if input_path.suffix.lower() == '.csv':
        df = pd.read_csv(input_path)
        column_name = id_column or 'assay_chembl_id'
        if column_name not in df.columns:
            raise ValueError(f"CSV file must contain '{column_name}' column: {input_path}")
        return df[column_name].dropna().astype(str).tolist()
    
    elif input_path.suffix.lower() == '.json':
        import json
        with open(input_path) as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return [str(item) for item in data if item is not None]
        elif isinstance(data, dict) and 'assay_chembl_id' in data:
            return [str(data['assay_chembl_id'])]
        else:
            raise ValueError(f"JSON file must contain list of IDs or 'assay_chembl_id' field: {input_path}")
    
    else:
        raise ValueError(f"Unsupported file format: {input_path.suffix}")


def _get_filter_profile(profile_name: str, config: AssayConfig) -> dict[str, Any] | None:
    """Get filter profile by name."""
    if not profile_name:
        return None
    
    profile = config.filter_profiles.get(profile_name)
    if not profile:
        available_profiles = list(config.filter_profiles.keys())
        raise ValueError(f"Unknown filter profile '{profile_name}'. Available: {available_profiles}")
    
    # Convert profile to dict, excluding None values
    filters = {}
    for key, value in profile.model_dump().items():
        if value is not None:
            filters[key] = value
    
    return filters if filters else None


def _generate_date_tag() -> str:
    """Generate date tag for output files."""
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract assay data from ChEMBL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract specific assays by ID
  python get_assay_data.py --input assay_ids.csv --config configs/config_assay_full.yaml
  
  # Extract assays for a specific target
  python get_assay_data.py --target CHEMBL231 --config configs/config_assay_full.yaml
  
  # Use filter profile
  python get_assay_data.py --target CHEMBL231 --filters human_single_protein --config configs/config_assay_full.yaml
  
  # Limit number of assays
  python get_assay_data.py --target CHEMBL231 --limit 100 --config configs/config_assay_full.yaml
        """
    )
    
    # Input data
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input", 
        type=Path,
        help="Path to CSV/JSON file with assay IDs"
    )
    input_group.add_argument(
        "--target",
        type=str,
        help="Target ChEMBL ID for filtering assays"
    )
    
    # Filters
    parser.add_argument(
        "--filters",
        type=str,
        help="Filter profile name (e.g., 'human_single_protein')"
    )
    
    # Variant options
    parser.add_argument(
        "--variants-only",
        action="store_true",
        help="Fetch only assays with variants (variant_sequence__isnull=false)"
    )
    parser.add_argument(
        "--include-wildtype",
        action="store_true",
        default=True,
        help="Include wildtype (non-variant) assays in the output"
    )
    
    # Limitations
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of assays to process"
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
    
    # Cache and release
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help="Cache directory path"
    )
    parser.add_argument(
        "--chembl-release",
        type=str,
        help="Specific ChEMBL release to use"
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
        default=Path("configs/config_assay_full.yaml"),
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
        config = load_assay_config(args.config)
        
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
        if args.cache_dir:
            config.cache.directory = args.cache_dir
        if args.dry_run:
            config.runtime.dry_run = True
        
        # Generate date tag if not provided
        date_tag = args.date_tag or _generate_date_tag()
        
        # Validate date tag format
        if not date_tag.isdigit() or len(date_tag) != 8:
            print(f"Error: Invalid date tag format '{date_tag}'. Expected YYYYMMDD.", file=sys.stderr)
            return 2
        
        # Run pipeline
        if args.input:
            # Extract by assay IDs
            try:
                id_column = getattr(config.io.input, 'assay_id_column', None)
                assay_ids = _load_assay_ids(args.input, id_column=id_column)
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                return 2
            if not assay_ids:
                print("Error: No assay IDs found in input file.", file=sys.stderr)
                return 2
            
            # Apply limit to input data if specified
            if config.runtime.limit and config.runtime.limit > 0:
                original_count = len(assay_ids)
                assay_ids = assay_ids[:config.runtime.limit]
                print(f"Extracting {len(assay_ids)} assays from {args.input} (limited from {original_count})")
            else:
                print(f"Extracting {len(assay_ids)} assays from {args.input}")
            
            # Apply variant flags
            if args.variants_only:
                config.variants.filters.include_wildtype = False
                print("Variants-only mode: excluding wildtype assays")
            elif not args.include_wildtype:
                config.variants.filters.include_wildtype = False
                print("Excluding wildtype assays")
            
            import pandas as pd
            pipeline = AssayPipeline(config)
            input_data = pd.DataFrame({"assay_chembl_id": assay_ids})
            result = pipeline.run(input_data=input_data)
            
        elif args.target:
            # Extract by target
            filters = _get_filter_profile(args.filters, config)
            print(f"Extracting assays for target: {args.target}")
            if filters:
                print(f"Using filter profile: {args.filters}")
            
            # Apply variant flags
            if args.variants_only:
                config.variants.filters.include_wildtype = False
                print("Variants-only mode: excluding wildtype assays")
            elif not args.include_wildtype:
                config.variants.filters.include_wildtype = False
                print("Excluding wildtype assays")
            
            import pandas as pd
            pipeline = AssayPipeline(config)
            input_data = pd.DataFrame({"target_chembl_id": [args.target]})
            result = pipeline.run(input_data=input_data)
        
        # Write outputs
        if not config.runtime.dry_run:
            output_paths = write_assay_outputs(
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
            print(f"  Total assays: {len(result.data)}")
            print(f"  ChEMBL release: {result.meta.get('chembl_release', 'unknown') if result.meta else 'unknown'}")
            print(f"  Date tag: {date_tag}")
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