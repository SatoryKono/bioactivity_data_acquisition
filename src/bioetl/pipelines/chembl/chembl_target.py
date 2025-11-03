"""ChEMBL-only Target Pipeline without external enrichment."""

from pathlib import Path
from typing import Any

from bioetl.config import PipelineConfig
from bioetl.sources.chembl.target.pipeline import TargetPipeline as BaseTargetPipeline


class TargetPipeline(BaseTargetPipeline):
    """ChEMBL-only target pipeline without UniProt/IUPHAR enrichment.

    This pipeline works only with ChEMBL target data, without UniProt
    or IUPHAR enrichment.
    """

    def __init__(self, config: PipelineConfig, run_id: str):
        """Initialize ChEMBL-only target pipeline.

        Overrides initialization to disable external enrichment sources.
        """
        super().__init__(config, run_id)
        # Force external clients to None for ChEMBL-only mode
        self.uniprot_client = None
        self.uniprot_idmapping_client = None
        self.uniprot_orthologs_client = None
        self.iuphar_client = None
        self.iuphar_paginator = None

        # Disable enrichment services
        if hasattr(self, "uniprot_search_client"):
            self.uniprot_search_client = None
        if hasattr(self, "uniprot_id_mapping_client"):
            self.uniprot_id_mapping_client = None
        if hasattr(self, "uniprot_ortholog_client"):
            self.uniprot_ortholog_client = None
        if hasattr(self, "iuphar_service"):
            self.iuphar_service = None
        if hasattr(self, "enricher"):
            # Keep enricher but it will skip enrichment if clients are None
            pass


__all__ = ["TargetPipeline"]
