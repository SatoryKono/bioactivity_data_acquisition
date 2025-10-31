from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.sources.uniprot.merge.service import UniProtService


@pytest.fixture()
def sample_entry() -> dict[str, object]:
    return {
        "primaryAccession": "P12345",
        "genes": [
            {
                "geneName": {"value": "ABC1"},
                "synonyms": [{"value": "DEF"}, {"value": "GHI"}],
            }
        ],
        "proteinDescription": {
            "recommendedName": {"fullName": {"value": "Protein ABC"}},
        },
        "sequence": {"length": 512},
        "organism": {
            "taxonId": 9606,
            "scientificName": "Homo sapiens",
            "lineage": ["Eukaryota", "Metazoa"],
        },
        "secondaryAccession": ["Q99999"],
        "comments": [
            {
                "commentType": "ALTERNATIVE PRODUCTS",
                "isoforms": [
                    {
                        "isoformIds": ["P12345-2"],
                        "names": [{"value": "Isoform 2"}],
                        "sequence": {"length": 300},
                    }
                ],
            }
        ],
    }


def test_enrich_targets_populates_enrichment(sample_entry: dict[str, object]) -> None:
    search_client = MagicMock()
    search_client.request_json.return_value = {"results": [sample_entry]}
    service = UniProtService(search_client=search_client)

    df = pd.DataFrame({"uniprot_accession": ["P12345"], "gene_symbol": ["ABC1"]})

    result = service.enrich_targets(df)

    assert result.dataframe.loc[0, "uniprot_canonical_accession"] == "P12345"
    assert result.metrics["enrichment_success.uniprot"] == pytest.approx(1.0)
    assert result.silver.loc[0, "gene_primary"] == "ABC1"
    assert "fallback." not in "".join(result.metrics.keys())
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


def test_run_id_mapping_returns_canonical_accession() -> None:
    id_client = MagicMock()
    id_client.request_json.side_effect = [
        {"jobId": "abc"},
        {"jobStatus": "FINISHED"},
        {"results": [{"from": "OLD", "to": {"primaryAccession": "NEW"}}]},
    ]
    service = UniProtService(id_mapping_client=id_client)

    result = service.run_id_mapping(["OLD"])

    assert result.loc[0, "canonical_accession"] == "NEW"
    assert id_client.request_json.call_count == 3
