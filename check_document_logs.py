#!/usr/bin/env python3
"""Check logs for document extraction mentions."""
import glob

# Find recent log files
log_files = glob.glob("data/logs/*.log") + glob.glob("logs/*.log")

for log_file in log_files[:5]:
    print(f"\n=== {log_file} ===")
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            # Look for document-related entries
            doc_lines = [line for line in lines if 'document' in line.lower() and 'extracting' in line.lower()]
            if doc_lines:
                print(f"Found {len(doc_lines)} document extraction lines:")
                for line in doc_lines[:30]:
                    print(f"  {line.strip()}")
            else:
                print("No document extraction mentions found")
                
            # Also check for "Processing batch"
            batch_lines = [line for line in lines if 'processing batch' in line.lower()]
            if batch_lines:
                print(f"\nFound {len(batch_lines)} batch processing lines:")
                for line in batch_lines[:10]:
                    print(f"  {line.strip()}")
    except Exception as e:
        print(f"Error: {e}")

