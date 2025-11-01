from __future__ import annotations

from unittest.mock import MagicMock

from bioetl.sources.uniprot import UniProtService


def test_fetch_entries_batches_and_extracts_primary() -> None:
    search_client = MagicMock()
    search_client.request_json.side_effect = [
        {"results": [{"primaryAccession": "P12345", "extra": "ignored"}]},
        {"results": [{"accession": "Q99999"}]},
    ]
    service = UniProtService(search_client=search_client, batch_size=1)

    entries = service.fetch_entries(["P12345", "Q99999"])

    assert set(entries) == {"P12345", "Q99999"}
    first_params = search_client.request_json.call_args_list[0].kwargs["params"]
    second_params = search_client.request_json.call_args_list[1].kwargs["params"]
    assert first_params["query"] == "(accession:P12345)"
    assert second_params["query"] == "(accession:Q99999)"


def test_run_id_mapping_requests_until_finished() -> None:
    id_client = MagicMock()
    id_client.request_json.side_effect = [
        {"jobId": "abc123"},
        {"jobStatus": "RUNNING"},
        {"jobStatus": "FINISHED"},
        {
            "results": [
                {
                    "from": "OLD1",
                    "to": {"primaryAccession": "NEW1", "isoformAccession": "NEW1-1"},
                },
                {"from": "OLD2", "to": "NEW2"},
            ]
        },
    ]
    service = UniProtService(id_mapping_client=id_client, id_mapping_poll_interval=0.0)

    mapped = service.run_id_mapping(["OLD1", "OLD2"])

    assert mapped.shape[0] == 2
    assert mapped.loc[0, "canonical_accession"] == "NEW1"
    assert mapped.loc[0, "isoform_accession"] == "NEW1-1"
    assert mapped.loc[1, "canonical_accession"] == "NEW2"
    assert id_client.request_json.call_count == 4


def test_fetch_orthologs_prioritizes_taxonomy_match() -> None:
    payload = {
        "results": [
            {
                "primaryAccession": "O11111",
                "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
            },
            {
                "primaryAccession": "O22222",
                "organism": {"scientificName": "Mus musculus", "taxonId": 10090},
            },
        ]
    }
    search_client = MagicMock()
    search_client.request_json.return_value = payload
    service = UniProtService(search_client=search_client)

    orthologs = service.fetch_orthologs("P12345", taxonomy_id=9606)

    assert orthologs.loc[0, "ortholog_accession"] == "O11111"
    assert orthologs.loc[0, "priority"] == -1
    query = search_client.request_json.call_args.kwargs["params"]["query"]
    assert "relationship_type:ortholog" in query
    assert "accession:P12345" in query


def test_search_by_gene_includes_organism_filter() -> None:
    search_client = MagicMock()
    search_client.request_json.return_value = {
        "results": [{"primaryAccession": "P12345", "genes": []}]
    }
    service = UniProtService(search_client=search_client)

    entry = service.search_by_gene("ABC1", organism="Homo sapiens")

    assert entry is not None
    params = search_client.request_json.call_args.kwargs["params"]
    assert "gene_exact:ABC1" in params["query"]
    assert "organism_name:\"Homo sapiens\"" in params["query"]
    assert params["size"] == 1
