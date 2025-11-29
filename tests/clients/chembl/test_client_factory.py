"""Tests for ChEMBL client factory."""

import sys
import os

from bioetl.pipelines.chembl.common.descriptor_factory import (
    ChemblDescriptorFactory,
)
from bioetl.pipelines.chembl.common.descriptor_factory_builder import (
    build_pipeline_chembl_factory,
)

# Ensure correct Python path for IDE environments
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src")
)


def test_factory_builds_descriptor_with_configured_fetcher() -> None:
    """Test that factory builds descriptor with configured fetcher."""
    config = {
        "sources": {
            "chembl": {
                "client": object(),
                "activity_fetcher": lambda batch: [
                    {"chembl_id": chembl_id, "value": 1}
                    for chembl_id in (batch or [])
                ]
            }
        }
    }

    factory = build_pipeline_chembl_factory(config)

    descriptor_factory = factory.create("activity")

    assert isinstance(descriptor_factory, ChemblDescriptorFactory)

    descriptor = descriptor_factory.build("activity")
    context = descriptor.build_context(None)
    fetcher = descriptor.fetcher_factory(context)

    assert callable(fetcher)
    assert fetcher(["CHEMBL1"]) == [
        {"chembl_id": "CHEMBL1", "value": 1}
    ]
