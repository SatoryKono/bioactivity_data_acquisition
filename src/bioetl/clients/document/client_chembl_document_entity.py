"""Deprecated shim for :mod:`bioetl.clients.entities.client_document_term`."""

from __future__ import annotations

from bioetl.clients.entities.client_document_term import (
    ChemblDocumentTermEntityClient as _ChemblDocumentTermEntityClient,
)

__all__ = ["ChemblDocumentTermEntityClient"]

ChemblDocumentTermEntityClient = _ChemblDocumentTermEntityClient
ChemblDocumentTermEntityClient.__module__ = __name__
