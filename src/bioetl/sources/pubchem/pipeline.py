"""Compatibility shim exposing the standalone PubChem pipeline."""

from bioetl.pipelines.pubchem import PubChemPipeline

__all__ = ["PubChemPipeline"]
