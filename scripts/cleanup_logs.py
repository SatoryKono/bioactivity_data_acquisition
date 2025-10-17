#!/usr/bin/env python3
"""Script to clean up old log files."""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from library.logging_setup import cleanup_old_logs, configure_logging, generate_run_id, set_run_context


def main() -> None:
    """Main entry point for log cleanup script."""
    parser = argparse.ArgumentParser(description="Clean up old log files")
    parser.add_argument(
        "--older-than",
        type=int,
        default=14,
        help="Remove logs older than this many days (default: 14)"
    )
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=Path("logs"),
        help="Directory to clean (default: logs/)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Set up logging for the cleanup script
    run_id = generate_run_id()
    set_run_context(run_id=run_id, stage="log_cleanup")
    
    logger = configure_logging(
        level="DEBUG" if args.verbose else "INFO",
        file_enabled=False,  # Don't create logs for cleanup script
    )
    logger = logger.bind(run_id=run_id, stage="log_cleanup")
    
    logger.info(
        "Starting log cleanup",
        older_than_days=args.older_than,
        logs_dir=str(args.logs_dir),
        dry_run=args.dry_run
    )
    
    if not args.logs_dir.exists():
        logger.warning("Logs directory does not exist", logs_dir=str(args.logs_dir))
        return
    
    # Find old log files
    cutoff_date = datetime.now() - timedelta(days=args.older_than)
    old_files = []
    
    for log_file in args.logs_dir.rglob("*.log*"):
        file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
        if file_mtime < cutoff_date:
            old_files.append((log_file, file_mtime))
    
    if not old_files:
        logger.info("No old log files found to clean up")
        return
    
    # Sort by modification time (oldest first)
    old_files.sort(key=lambda x: x[1])
    
    logger.info(f"Found {len(old_files)} old log files to clean up")
    
    if args.dry_run:
        logger.info("DRY RUN - Files that would be deleted:")
        for log_file, file_mtime in old_files:
            logger.info(
                "Would delete",
                file=str(log_file),
                modified=file_mtime.isoformat(),
                age_days=(datetime.now() - file_mtime).days
            )
    else:
        # Actually delete the files
        deleted_count = 0
        failed_count = 0
        
        for log_file, file_mtime in old_files:
            try:
                log_file.unlink()
                deleted_count += 1
                logger.info(
                    "Deleted log file",
                    file=str(log_file),
                    modified=file_mtime.isoformat(),
                    age_days=(datetime.now() - file_mtime).days
                )
            except OSError as e:
                failed_count += 1
                logger.error(
                    "Failed to delete log file",
                    file=str(log_file),
                    error=str(e)
                )
        
        logger.info(
            "Log cleanup completed",
            deleted=deleted_count,
            failed=failed_count,
            total=len(old_files)
        )
        
        if failed_count > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
