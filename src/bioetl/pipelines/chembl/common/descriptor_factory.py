"""Factories for building ChemblExtractionServiceDescriptor instances.

Re-exports classes from bioetl.clients.chembl.descriptor_factory to maintain
backward compatibility and avoid circular imports.
"""

from __future__ import annotations

from bioetl.clients.chembl.descriptor_factory import (
    ChemblContextFacade,
    ChemblDescriptorFactory,
    FetcherStrategy,
)

__all__ = [
    "ChemblContextFacade",
    "ChemblDescriptorFactory",
    "FetcherStrategy",
]

