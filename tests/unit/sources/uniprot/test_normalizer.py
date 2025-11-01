from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.sources.uniprot import UniProtEnrichmentResult, UniProtService


def test_enrich_targets_populates_enrichment(sample_uniprot_entry: dict[str, object]) -> None:
    search_client = MagicMock()
    search_client.request_json.return_value = {"results": [sample_uniprot_entry]}
    service = UniProtService(search_client=search_client)

    df = pd.DataFrame({"uniprot_accession": ["P12345"], "gene_symbol": ["ABC1"]})

    result = service.enrich_targets(df)

    assert isinstance(result, UniProtEnrichmentResult)
    assert result.dataframe.loc[0, "uniprot_canonical_accession"] == "P12345"
    assert result.metrics["enrichment_success.uniprot"] == pytest.approx(1.0)
    assert result.silver.loc[0, "gene_primary"] == "ABC1"
    assert result.components.loc[0, "isoform_accession"] == "P12345"


def test_enrich_targets_records_missing_mapping_when_unresolved() -> None:
    search_client = MagicMock()
    search_client.request_json.return_value = {"results": []}
    service = UniProtService(search_client=search_client)

    df = pd.DataFrame({"uniprot_accession": ["UNKNOWN"]})

    result = service.enrich_targets(df)

    assert result.missing_mappings, "Expected unresolved accession to be recorded"
    record = result.missing_mappings[0]
    assert record["status"] == "unresolved"
    assert result.validation_issues[0]["metric"] == "enrichment.uniprot.unresolved"


def test_enrich_targets_uses_gene_symbol_fallback(sample_uniprot_entry: dict[str, object]) -> None:
    search_client = MagicMock()
    search_client.request_json.side_effect = [
        {"results": []},
        {"results": [sample_uniprot_entry]},
    ]
    service = UniProtService(search_client=search_client)

    df = pd.DataFrame(
        {
            "uniprot_accession": ["OBSOLETE"],
            "gene_symbol": ["ABC1"],
            "organism": ["Homo sapiens"],
        }
    )

    result = service.enrich_targets(df)

    assert result.dataframe.loc[0, "uniprot_merge_strategy"] == "gene_symbol"
    assert result.metrics["fallback.gene_symbol.count"] == 1
    record = result.missing_mappings[0]
    assert record["resolution"] == "gene_symbol"
    assert "ABC1" in record["details"]
    gene_query = search_client.request_json.call_args_list[1].kwargs["params"]["query"]
    assert "gene_exact:ABC1" in gene_query
    assert "organism_name:\"Homo sapiens\"" in gene_query
