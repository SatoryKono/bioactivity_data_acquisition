"""Unit tests for ChemblReleaseMixin behaviour."""

from __future__ import annotations

import pytest

from bioetl.pipelines.common.release_tracker import ChemblReleaseMixin


class _DummyTracker(ChemblReleaseMixin):
    """Helper реализация mixin для тестов."""

    def __init__(self) -> None:
        super().__init__()


@pytest.mark.unit
def test_release_defaults_to_none() -> None:
    tracker = _DummyTracker()

    assert tracker.chembl_release is None


@pytest.mark.unit
def test_release_updates_and_resets() -> None:
    tracker = _DummyTracker()

    tracker._update_release("CHEMBL_36")  # noqa: SLF001
    assert tracker.chembl_release == "CHEMBL_36"

    tracker._update_release("")  # noqa: SLF001
    assert tracker.chembl_release is None

    tracker._update_release(None)  # noqa: SLF001
    assert tracker.chembl_release is None

