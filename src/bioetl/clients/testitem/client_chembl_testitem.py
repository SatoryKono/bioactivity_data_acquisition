"""Deprecated shim for :mod:`bioetl.clients.entities.client_testitem`."""

from __future__ import annotations

from bioetl.clients.entities.client_testitem import ChemblTestitemClient as _ChemblTestitemClient

__all__ = ["ChemblTestitemClient"]

ChemblTestitemClient = _ChemblTestitemClient
ChemblTestitemClient.__module__ = __name__
