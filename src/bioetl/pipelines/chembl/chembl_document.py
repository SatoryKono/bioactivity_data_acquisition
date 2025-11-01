"""ChEMBL-only Document Pipeline without external sources."""

from pathlib import Path
from typing import Any

from bioetl.config import PipelineConfig
from bioetl.sources.chembl.document.pipeline import DocumentPipeline as BaseDocumentPipeline


class DocumentPipeline(BaseDocumentPipeline):
    """ChEMBL-only document pipeline without external enrichment.

    This pipeline works only with ChEMBL data, without PubMed, Crossref,
    OpenAlex, or Semantic Scholar enrichment.
    """

    def __init__(self, config: PipelineConfig, run_id: str):
        """Initialize ChEMBL-only document pipeline.

        Overrides mode to 'chembl' to disable external sources.
        """
        # Force ChEMBL-only mode
        if not hasattr(config.cli, "mode") or config.cli.mode != "chembl":
            # Create a copy of config with mode set to 'chembl'
            from dataclasses import replace

            config = replace(config, cli=replace(config.cli, mode="chembl"))

        super().__init__(config, run_id)


__all__ = ["DocumentPipeline"]

