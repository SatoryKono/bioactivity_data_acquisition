"""Shim for :mod:`bioactivity.clients.chembl`."""
from __future__ import annotations

from library._compat import reexport

reexport(
    "library.clients.chembl",
    "bioactivity.clients.chembl",
    globals(),
    aliases={"ChemblClient": "ChEMBLClient"},
)
