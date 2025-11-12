"""Deprecated shim for :mod:`bioetl.clients.entities.client_activity`."""

from __future__ import annotations

from bioetl.clients.entities.client_activity import ChemblActivityClient as _ChemblActivityClient

__all__ = ["ChemblActivityClient"]

ChemblActivityClient = _ChemblActivityClient
ChemblActivityClient.__module__ = __name__
