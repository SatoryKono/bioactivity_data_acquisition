from __future__ import annotations

from bioetl.sources.uniprot import UniProtService


def test_expand_isoforms_yields_canonical_and_alternatives(sample_uniprot_entry: dict[str, object]) -> None:
    service = UniProtService()

    isoforms = service.expand_isoforms(sample_uniprot_entry)

    assert isoforms.shape[0] == 2
    canonical = isoforms.iloc[0]
    alternative = isoforms.iloc[1]
    assert canonical["canonical_accession"] == "P12345"
    assert bool(canonical["is_canonical"]) is True
    assert alternative["isoform_accession"] == "P12345-2"
    assert alternative["source"] == "isoform"


def test_extract_helpers_return_normalised_values(sample_uniprot_entry: dict[str, object]) -> None:
    service = UniProtService()

    assert service._extract_gene_primary(sample_uniprot_entry) == "ABC1"
    assert service._extract_gene_synonyms(sample_uniprot_entry) == ["DEF", "GHI"]
    assert service._extract_protein_name(sample_uniprot_entry) == "Protein ABC"
    assert service._extract_sequence_length(sample_uniprot_entry) == 512
    assert service._extract_taxonomy_id(sample_uniprot_entry) == 9606
    assert service._extract_organism(sample_uniprot_entry) == "Homo sapiens"
    assert service._extract_lineage(sample_uniprot_entry) == ["Eukaryota", "Metazoa"]
    assert service._extract_secondary(sample_uniprot_entry) == ["Q99999"]


def test_missing_mapping_record_serialises_details() -> None:
    record = UniProtService._missing_mapping_record(
        stage="uniprot",
        target_id="CHEMBL1",
        accession="OLD1",
        resolution="gene_symbol",
        status="resolved",
        resolved_accession="NEW1",
        details={"gene": "ABC1", "organism": "Homo sapiens"},
    )

    assert record["stage"] == "uniprot"
    assert record["resolved_accession"] == "NEW1"
    assert record["details"] == "{\"gene\": \"ABC1\", \"organism\": \"Homo sapiens\"}"
