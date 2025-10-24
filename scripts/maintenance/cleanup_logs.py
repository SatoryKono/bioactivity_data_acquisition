#!/usr/bin/env python3
"""Cleanup script for log files to resolve Windows file locking issues."""

import os
import sys
import time
import shutil
from pathlib import Path
from typing import List


def cleanup_logs(logs_dir: Path = Path("logs"), force: bool = False) -> None:
    """Clean up log files to resolve Windows file locking issues.
    
    Args:
        logs_dir: Directory containing log files
        force: Force cleanup even if files are in use
    """
    if not logs_dir.exists():
        print(f"Logs directory {logs_dir} does not exist")
        return
    
    print(f"Cleaning up logs in {logs_dir}")
    
    # List of log files to clean up
    log_files = [
        "app.log",
        "app.log.1", 
        "app.log.2",
        "app.log.3",
        "app.log.4",
        "app.log.5",
        "app.log.6",
        "app.log.7",
        "app.log.8",
        "app.log.9",
        "app.log.10"
    ]
    
    # Add dated log files
    for i in range(1, 8):  # Last 7 days
        log_files.append(f"app.log.{i:04d}-01-01")  # Example date format
    
    cleaned_count = 0
    failed_count = 0
    
    for log_file in log_files:
        file_path = logs_dir / log_file
        if file_path.exists():
            try:
                if force:
                    # Try to remove immediately
                    file_path.unlink()
                    print(f"Removed: {file_path}")
                    cleaned_count += 1
                else:
                    # Try to rename first (safer approach)
                    backup_name = f"{log_file}.backup.{int(time.time())}"
                    backup_path = logs_dir / backup_name
                    
                    try:
                        shutil.move(str(file_path), str(backup_path))
                        print(f"Moved {file_path} to {backup_path}")
                        cleaned_count += 1
                    except (OSError, PermissionError) as e:
                        print(f"Failed to move {file_path}: {e}")
                        failed_count += 1
                        
            except (OSError, PermissionError) as e:
                print(f"Failed to remove {file_path}: {e}")
                failed_count += 1
    
    print(f"\nCleanup completed:")
    print(f"  - Files cleaned: {cleaned_count}")
    print(f"  - Files failed: {failed_count}")
    
    if failed_count > 0:
        print("\nSome files could not be cleaned up. This might be because:")
        print("  - Files are currently in use by the application")
        print("  - Files are locked by another process")
        print("  - Insufficient permissions")
        print("\nTry running the script with --force flag or restart the application first.")


def main():
    """Main function for the cleanup script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Clean up log files to resolve Windows file locking issues")
    parser.add_argument("--logs-dir", type=Path, default=Path("logs"), 
                      help="Directory containing log files (default: logs)")
    parser.add_argument("--force", action="store_true", 
                      help="Force cleanup even if files are in use")
    
    args = parser.parse_args()
    
    try:
        cleanup_logs(args.logs_dir, args.force)
    except KeyboardInterrupt:
        print("\nCleanup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during cleanup: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()