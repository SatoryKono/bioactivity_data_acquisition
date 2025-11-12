"""Deprecated shim for :mod:`bioetl.clients.entities.client_target`."""

from __future__ import annotations

from bioetl.clients.entities.client_target import ChemblTargetClient as _ChemblTargetClient

__all__ = ["ChemblTargetClient"]

ChemblTargetClient = _ChemblTargetClient
ChemblTargetClient.__module__ = __name__
