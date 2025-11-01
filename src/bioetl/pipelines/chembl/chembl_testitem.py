"""ChEMBL-only TestItem Pipeline without PubChem enrichment."""

from pathlib import Path
from typing import Any

from bioetl.config import PipelineConfig
from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline as BaseTestItemPipeline


class TestItemPipeline(BaseTestItemPipeline):
    """ChEMBL-only testitem pipeline without PubChem enrichment.

    This pipeline works only with ChEMBL molecule data, without PubChem
    enrichment.
    """

    def _init_external_adapters(self) -> None:
        """Override to disable PubChem enrichment for ChEMBL-only mode."""
        # Do not initialize PubChem client - ChEMBL-only mode
        self.pubchem_client = None
        self._pubchem_api_client = None
        # Keep normalizer initialized but unused
        from bioetl.sources.pubchem.normalizer.pubchem_normalizer import PubChemNormalizer

        self.pubchem_normalizer = PubChemNormalizer()


__all__ = ["TestItemPipeline"]

