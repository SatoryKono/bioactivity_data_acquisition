"""Deprecated shim for :mod:`bioetl.clients.entities.client_document`."""

from __future__ import annotations

from bioetl.clients.entities.client_document import ChemblDocumentClient as _ChemblDocumentClient

__all__ = ["ChemblDocumentClient"]

ChemblDocumentClient = _ChemblDocumentClient
ChemblDocumentClient.__module__ = __name__
