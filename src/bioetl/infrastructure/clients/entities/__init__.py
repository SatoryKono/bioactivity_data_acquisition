"""Клиенты ChEMBL entities."""

__all__ = [
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblDocumentClient",
    "CHEMBL_ALLOWED_ENTITIES",
    "ChemblEntity",
    "ChemblEntityClient",
    "ChemblEntityClientFactory",
    "ChemblTargetClient",
    "ChemblTestItemClient",
]


def __getattr__(name: str):
    if name in __all__:
        from bioetl.infrastructure.clients.entities import common

        return getattr(common, name)
    raise AttributeError(name)
