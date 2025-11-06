"""Lightweight smoke tests used by the CI pipeline."""

from __future__ import annotations

import importlib


def test_bioetl_importable() -> None:
    """Ensure the main package can be imported in CI."""
    module = importlib.import_module("bioetl")
    assert module.__name__ == "bioetl"
    assert hasattr(module, "PipelineConfig")


def test_cli_entrypoint_exposed() -> None:
    """Verify that the CLI entrypoint can be imported for smoke checks."""
    cli_module = importlib.import_module("bioetl.cli.main")
    assert hasattr(cli_module, "app")
