# Debug Scripts

This directory contains debug and development scripts that are not part of the production codebase.

## Scripts

- `debug_*.py` - Various debug scripts for troubleshooting pipeline issues
- `test_correlation_demo.py` - Demo script for testing correlation functionality

## Usage

These scripts are intended for development and debugging purposes only. They may not be maintained or documented for production use.

## Moving Scripts

If you need to add a new debug script:

1. Place it in this directory with a `debug_` prefix
2. Update this README if the script becomes important for development workflow
3. Consider moving useful functionality to the main `src/library/tools/` directory if it becomes production-ready
