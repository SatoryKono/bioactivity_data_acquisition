"""Unit tests for materialization helpers and format utilities."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bioetl.config.models import MaterializationPaths
from bioetl.core.materialization import MaterializationManager
from bioetl.core.output_writer import extension_for_format, normalise_output_format


@pytest.mark.parametrize(
    ("value", "kwargs", "expected"),
    [
        ("CSV", {}, "csv"),
        (" parquet ", {}, "parquet"),
        (None, {}, "csv"),
        (None, {"default": None}, None),
    ],
)
def test_normalise_output_format_normalises_and_defaults(
    value: str | None, kwargs: dict[str, str | None], expected: str | None
) -> None:
    """The public helper normalises case and honours the default parameter."""

    assert normalise_output_format(value, **kwargs) == expected


@pytest.mark.parametrize("value", ["json", "", "xml"])
def test_normalise_output_format_rejects_unknown_formats(value: str) -> None:
    """Unsupported formats raise a ``ValueError`` with a helpful message."""

    with pytest.raises(ValueError, match="Unsupported output format"):
        normalise_output_format(value)


@pytest.mark.parametrize(
    ("format_name", "expected_extension"),
    [
        ("csv", ".csv"),
        ("PARQUET", ".parquet"),
    ],
)
def test_extension_for_format_returns_expected_suffix(format_name: str, expected_extension: str) -> None:
    """The exported helper returns canonical file extensions for supported formats."""

    assert extension_for_format(format_name) == expected_extension


def test_extension_for_format_rejects_unknown_format() -> None:
    """Unsupported formats surface a ``ValueError`` during extension lookup."""

    with pytest.raises(ValueError, match="Unsupported output format"):
        extension_for_format("feather")


def test_materialization_manager_raises_for_unsupported_format() -> None:
    """Materialization entrypoints delegate format validation to the shared helper."""

    manager = MaterializationManager(paths=MaterializationPaths())

    with pytest.raises(ValueError, match="Unsupported output format"):
        manager.materialize_gold(
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            format="feather",
        )


@pytest.mark.parametrize("format_name", ["csv", "parquet"])
def test_default_stage_path_uses_shared_extension_logic(format_name: str) -> None:
    """Default path resolution relies on the extension helper for consistency."""

    base = Path("output")
    resolved = MaterializationManager._resolve_default_stage_path(base, format_name, fallback_stem="dataset")
    assert resolved.name.endswith(extension_for_format(format_name))
