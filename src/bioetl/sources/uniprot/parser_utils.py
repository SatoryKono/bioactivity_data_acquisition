"""Helpers for parsing UniProt API responses."""

from __future__ import annotations

from typing import Any, Iterable

import pandas as pd


def extract_gene_primary(entry: dict[str, Any]) -> str | None:
    genes = entry.get("genes") or []
    if isinstance(genes, list):
        for gene in genes:
            primary = gene.get("geneName") if isinstance(gene, dict) else None
            if isinstance(primary, dict):
                value = primary.get("value")
                if value:
                    return str(value)
    return None


def extract_gene_synonyms(entry: dict[str, Any]) -> list[str]:
    genes = entry.get("genes") or []
    synonyms: list[str] = []
    if isinstance(genes, list):
        for gene in genes:
            names = gene.get("synonyms") if isinstance(gene, dict) else None
            if not names:
                continue
            if isinstance(names, list):
                for name in names:
                    value = name.get("value") if isinstance(name, dict) else name
                    if value:
                        synonyms.append(str(value))
    return synonyms


def extract_secondary_accessions(entry: dict[str, Any]) -> list[str]:
    secondary = entry.get("secondaryAccessions") or entry.get("secondaryAccession")
    if isinstance(secondary, list):
        return [str(value) for value in secondary if value]
    if secondary:
        return [str(secondary)]
    return []


def extract_protein_name(entry: dict[str, Any]) -> str | None:
    protein = entry.get("proteinDescription") or entry.get("protein")
    if isinstance(protein, dict):
        recommended = protein.get("recommendedName") or protein.get("recommendedname")
        if isinstance(recommended, dict):
            value = recommended.get("fullName") or recommended.get("fullname")
            if isinstance(value, dict):
                return str(value.get("value") or value.get("text") or value.get("label"))
            if value:
                return str(value)
    return None


def extract_sequence_length(entry: dict[str, Any]) -> int | None:
    sequence = entry.get("sequence")
    if isinstance(sequence, dict):
        length = sequence.get("length")
        try:
            return int(length) if length is not None else None
        except (TypeError, ValueError):
            return None
    return None


def extract_taxonomy_id(entry: dict[str, Any]) -> int | None:
    organism = entry.get("organism", {})
    if isinstance(organism, dict):
        taxon = organism.get("taxonId") or organism.get("taxonIdentifier")
        try:
            return int(taxon) if taxon is not None else None
        except (TypeError, ValueError):
            return None
    return None


def extract_organism(entry: dict[str, Any]) -> str | None:
    organism = entry.get("organism", {})
    if isinstance(organism, dict):
        name = organism.get("scientificName") or organism.get("commonName")
        if name:
            return str(name)
    return None


def extract_lineage(entry: dict[str, Any]) -> list[str]:
    organism = entry.get("organism", {})
    lineage = organism.get("lineage") if isinstance(organism, dict) else None
    if isinstance(lineage, list):
        return [str(value) for value in lineage if value]
    return []


def build_silver_record(canonical: str, entry: dict[str, Any]) -> dict[str, Any]:
    """Return a normalized record for the silver UniProt artefact."""

    return {
        "canonical_accession": canonical,
        "protein_name": extract_protein_name(entry),
        "gene_primary": extract_gene_primary(entry),
        "gene_synonyms": "; ".join(extract_gene_synonyms(entry)),
        "organism_name": extract_organism(entry),
        "organism_id": extract_taxonomy_id(entry),
        "lineage": "; ".join(extract_lineage(entry)),
        "sequence_length": extract_sequence_length(entry),
        "secondary_accessions": "; ".join(extract_secondary_accessions(entry)),
    }


def expand_isoforms(entry: dict[str, Any]) -> pd.DataFrame:
    """Expand isoform information from an entry into a dataframe."""

    canonical = entry.get("primaryAccession") or entry.get("accession")
    sequence_length = extract_sequence_length(entry)
    records: list[dict[str, Any]] = [
        {
            "canonical_accession": canonical,
            "isoform_accession": canonical,
            "isoform_name": extract_protein_name(entry),
            "is_canonical": True,
            "sequence_length": sequence_length,
            "source": "canonical",
        }
    ]

    comments = entry.get("comments", []) or []
    for comment in comments:
        if comment.get("commentType") != "ALTERNATIVE PRODUCTS":
            continue
        for isoform in comment.get("isoforms", []) or []:
            isoform_ids = isoform.get("isoformIds") or isoform.get("isoformId")
            if isinstance(isoform_ids, list) and isoform_ids:
                isoform_acc = isoform_ids[0]
            else:
                isoform_acc = isoform_ids
            if not isoform_acc:
                continue
            names = isoform.get("names") or isoform.get("name") or []
            if isinstance(names, dict):
                name_values: Iterable[Any] = [names]
            else:
                name_values = names if isinstance(names, Iterable) else []
            isoform_name: str | None = None
            for name in name_values:
                value = name.get("value") if isinstance(name, dict) else name
                if value:
                    isoform_name = str(value)
                    break
            sequence = isoform.get("sequence") or {}
            isoform_length = None
            if isinstance(sequence, dict):
                try:
                    isoform_length = (
                        int(sequence.get("length")) if sequence.get("length") is not None else None
                    )
                except (TypeError, ValueError):
                    isoform_length = None
            records.append(
                {
                    "canonical_accession": canonical,
                    "isoform_accession": isoform_acc,
                    "isoform_name": isoform_name,
                    "is_canonical": False,
                    "sequence_length": isoform_length,
                    "source": "alternative",
                }
            )

    return pd.DataFrame(records).convert_dtypes()

