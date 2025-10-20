"""Compatibility shim re-exporting testitem clients.

This preserves imports like `from library.testitem.clients import PubChemClient`.
"""

from __future__ import annotations

from library.clients.chembl import TestitemChEMBLClient  # re-export
from library.clients.pubchem import PubChemClient  # re-export

__all__ = ["TestitemChEMBLClient", "PubChemClient"]


