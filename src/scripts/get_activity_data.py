"""CLI для извлечения данных активностей (согласован с интерфейсом ассев)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
 
from library.activity.config import ConfigLoadError, load_activity_config
from library.activity.pipeline import ActivityPipeline
from library.activity.writer import write_activity_outputs
from library.logging_setup import configure_logging


def _generate_date_tag() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract activity data from ChEMBL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python get_activity_data.py --input data/input/activity.csv --config configs/config_activity_full.yaml
  python get_activity_data.py --config configs/config_activity_full.yaml --date-tag 20250101
        """,
    )

    parser.add_argument("--input", type=Path, help="Path to CSV with filter IDs", required=True)
    parser.add_argument("--limit", type=int, help="Maximum number of activities", required=False)
    parser.add_argument("--timeout", type=float, default=60.0, help="Request timeout in seconds")
    parser.add_argument("--retries", type=int, default=5, help="Number of retry attempts")
    parser.add_argument("--cache-dir", type=Path, help="Cache directory path")
    parser.add_argument("--format", choices=["csv", "parquet"], help="Output format (overrides config)")
    parser.add_argument("--config", type=Path, default=Path("configs/config_activity_full.yaml"), help="Config file path")
    parser.add_argument("--output-dir", type=Path, help="Output directory (overrides config)")
    parser.add_argument("--date-tag", type=str, help="Date tag for outputs (YYYYMMDD)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO")
    parser.add_argument("--log-file", type=Path, help="Path to log file (default: stdout)")
    parser.add_argument("--log-format", choices=["text", "json"], default="text", help="Console log format (default: text)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate execution without writing files")

    args = parser.parse_args()

    try:
        cfg = load_activity_config(args.config)
        configure_logging(
            level=args.log_level,
            file_enabled=args.log_file is not None,
            console_format=args.log_format,
            log_file=args.log_file,
            logging_config=cfg.logging.model_dump() if hasattr(cfg, "logging") else None,
        )

        # Overrides
        if args.timeout:
            cfg.timeout_sec = args.timeout
        if args.retries is not None:
            cfg.max_retries = args.retries
        if args.limit:
            cfg.runtime.limit = args.limit
        if args.output_dir:
            cfg.io.output.dir = args.output_dir
        if args.cache_dir:
            cfg.io.cache_dir = args.cache_dir
        if args.format:
            cfg.io.output.format = args.format
        if args.dry_run:
            cfg.runtime.dry_run = True

        date_tag = args.date_tag or _generate_date_tag()
        if not date_tag.isdigit() or len(date_tag) != 8:
            print(f"Error: Invalid date tag format '{date_tag}'. Expected YYYYMMDD.", file=sys.stderr)
            return 2

        import pandas as pd
        pipeline = ActivityPipeline(cfg)
        input_data = pd.read_csv(args.input)
        
        # Note: Limit is applied in pipeline.extract() method, not here
        print(f"Processing {len(input_data)} activities from {args.input}")
        
        result = pipeline.run(input_data=input_data)

        if not cfg.runtime.dry_run:
            outputs = write_activity_outputs(
                result=result,
                output_dir=cfg.io.output.dir,
                date_tag=date_tag,
                config=cfg,
            )
            print("\nPipeline completed successfully. Output files:")
            for name, path in outputs.items():
                print(f"  {name}: {path}")

            print("\nSummary:")
            print(f"  Total activities: {len(result.data)}")
            print(f"  ChEMBL release: {getattr(result.metadata, 'chembl_release', 'unknown') if result.metadata else 'unknown'}")
            print(f"  Date tag: {date_tag}")
        else:
            print("Dry run completed. No files were written.")

        return 0

    except ConfigLoadError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 3


if __name__ == "__main__":
    sys.exit(main())

