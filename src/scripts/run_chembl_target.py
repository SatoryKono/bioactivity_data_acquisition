#!/usr/bin/env python3
"""CLI entrypoint for executing the chembl_target pipeline."""

from scripts.run_target import app  # re-export for backwards compatibility


if __name__ == "__main__":
    app()
