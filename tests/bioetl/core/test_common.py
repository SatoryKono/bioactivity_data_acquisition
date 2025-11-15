"""Tests for shared core utilities."""

from __future__ import annotations

from bioetl.core.common import ChemblReleaseMixin


class _BaseStub:
    def __init__(self, *, marker: str) -> None:
        self.marker = marker
        super().__init__()


class _ChemblReleaseStub(ChemblReleaseMixin, _BaseStub):
    def __init__(self, *, marker: str) -> None:
        super().__init__(marker=marker)


def test_chembl_release_mixin_normalises_values() -> None:
    """Ensure the mixin initialises and normalises release values."""

    stub = _ChemblReleaseStub(marker="ok")

    assert stub.marker == "ok"
    assert stub.chembl_release is None

    stub._set_chembl_release("  34  ")  # noqa: SLF001
    assert stub.chembl_release == "34"

    stub._set_chembl_release(35)  # noqa: SLF001
    assert stub.chembl_release == "35"

    stub._set_chembl_release("   ")  # noqa: SLF001
    assert stub.chembl_release is None
