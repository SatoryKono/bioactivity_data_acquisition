from __future__ import annotations

from importlib import import_module
import warnings

import pytest

import bioetl.core as core
from bioetl.chembl.common import join_activity_with_molecule as chembl_join
from bioetl.chembl.common.release_tracker import (
    ChemblReleaseMixin as DomainChemblReleaseMixin,
)


@pytest.mark.unit
def test_deprecated_shims_resolve_to_domain_implementations() -> None:
    assert core.join_activity_with_molecule is chembl_join
    assert core.ChemblReleaseMixin is DomainChemblReleaseMixin


@pytest.mark.unit
def test_deprecated_shims_still_available_via_core_utils() -> None:
    core_utils = import_module("bioetl.core.utils")

    assert core_utils.join_activity_with_molecule is chembl_join


@pytest.mark.unit
def test_deprecated_shims_emit_deprecation_warning() -> None:
    with warnings.catch_warnings(record=True) as caught_join:
        warnings.simplefilter("always", DeprecationWarning)
        _ = core.join_activity_with_molecule

    assert any(
        issubclass(warning.category, DeprecationWarning)
        for warning in caught_join
    )

    with warnings.catch_warnings(record=True) as caught_mixin:
        warnings.simplefilter("always", DeprecationWarning)
        _ = core.ChemblReleaseMixin

    assert any(
        issubclass(warning.category, DeprecationWarning)
        for warning in caught_mixin
    )
