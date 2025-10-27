#!/usr/bin/env python3
"""Check logs for ChEMBL extraction."""
import glob

# Find recent log files
log_files = glob.glob("data/logs/*.log") + glob.glob("logs/*.log")
print(f"Found {len(log_files)} log files")

# Check for ChEMBL mentions
for log_file in log_files[:5]:  # Check first 5
    print(f"\n=== {log_file} ===")
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            chembl_lines = [line for line in lines if 'chembl' in line.lower()]
            if chembl_lines:
                print(f"Found {len(chembl_lines)} ChEMBL-related lines:")
                for line in chembl_lines[-20:]:  # Last 20
                    print(f"  {line.strip()}")
            else:
                print("No ChEMBL mentions found")
    except Exception as e:
        print(f"Error reading {log_file}: {e}")

