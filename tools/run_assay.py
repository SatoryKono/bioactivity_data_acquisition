#!/usr/bin/env python3
"""CLI entrypoint for executing the assay pipeline."""

from __future__ import annotations

from scripts.run_chembl_assay import app  # re-export for backwards compatibility

if __name__ == "__main__":
    app()
