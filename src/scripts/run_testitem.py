#!/usr/bin/env python3
"""CLI entrypoint for executing the test item pipeline."""

from scripts.run_chembl_testitem import app  # re-export for backwards compatibility


if __name__ == "__main__":
    app()

