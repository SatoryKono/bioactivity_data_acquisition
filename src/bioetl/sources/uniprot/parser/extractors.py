"""Low-level field extractors for UniProt entries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def _extract_gene_primary(entry: Mapping[str, Any]) -> str | None:
    genes = entry.get("genes") or []
    if isinstance(genes, Sequence):
        for gene in genes:
            if not isinstance(gene, Mapping):
                continue
            primary = gene.get("geneName") or gene.get("name")
            if isinstance(primary, Mapping):
                value = primary.get("value") or primary.get("label")
                if value:
                    return str(value)
    return None


def _extract_gene_synonyms(entry: Mapping[str, Any]) -> list[str]:
    genes = entry.get("genes") or []
    synonyms: list[str] = []
    if isinstance(genes, Sequence):
        for gene in genes:
            if not isinstance(gene, Mapping):
                continue
            names = gene.get("synonyms") or gene.get("synonym")
            if isinstance(names, Mapping):
                names = [names]
            if not isinstance(names, Sequence):
                continue
            for name in names:
                if isinstance(name, Mapping):
                    value = name.get("value") or name.get("label")
                else:
                    value = name
                if value:
                    synonyms.append(str(value))
    return synonyms


def _extract_secondary(entry: Mapping[str, Any]) -> list[str]:
    secondary = entry.get("secondaryAccessions") or entry.get("secondaryAccession")
    if isinstance(secondary, Mapping):
        secondary = list(secondary.values())
    if isinstance(secondary, Sequence) and not isinstance(secondary, (str, bytes)):
        return [str(value) for value in secondary if value]
    if secondary:
        return [str(secondary)]
    return []


def _extract_protein_name(entry: Mapping[str, Any]) -> str | None:
    protein = entry.get("proteinDescription") or entry.get("protein")
    if isinstance(protein, Mapping):
        recommended = protein.get("recommendedName") or protein.get("recommendedname")
        if isinstance(recommended, Mapping):
            value = recommended.get("fullName") or recommended.get("fullname")
            if isinstance(value, Mapping):
                resolved = value.get("value") or value.get("text") or value.get("label")
                if resolved:
                    return str(resolved)
            elif value:
                return str(value)
    return None


def _extract_sequence_length(entry: Mapping[str, Any]) -> int | None:
    sequence = entry.get("sequence")
    if isinstance(sequence, Mapping):
        length = sequence.get("length")
        try:
            return int(length) if length is not None else None
        except (TypeError, ValueError):
            return None
    return None


def _extract_taxonomy_id(entry: Mapping[str, Any]) -> int | None:
    organism = entry.get("organism")
    if isinstance(organism, Mapping):
        taxon = organism.get("taxonId") or organism.get("taxonIdentifier")
        try:
            return int(taxon) if taxon is not None else None
        except (TypeError, ValueError):
            return None
    return None


def _extract_organism(entry: Mapping[str, Any]) -> str | None:
    organism = entry.get("organism")
    if isinstance(organism, Mapping):
        name = organism.get("scientificName") or organism.get("commonName")
        if name:
            return str(name)
    elif isinstance(organism, str):
        return organism
    return None


def _extract_lineage(entry: Mapping[str, Any]) -> list[str]:
    organism = entry.get("organism")
    lineage = None
    if isinstance(organism, Mapping):
        lineage = organism.get("lineage")
    if isinstance(lineage, Sequence) and not isinstance(lineage, (str, bytes)):
        return [str(value) for value in lineage if value]
    return []


def parse_isoforms(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Extract canonical and isoform records from an entry."""

    canonical = entry.get("primaryAccession") or entry.get("accession")
    canonical_str = str(canonical) if canonical else None
    sequence_length = _extract_sequence_length(entry)
    records: list[dict[str, Any]] = [
        {
            "canonical_accession": canonical_str,
            "isoform_accession": canonical_str,
            "isoform_name": _extract_protein_name(entry),
            "is_canonical": True,
            "sequence_length": sequence_length,
            "source": "canonical",
        }
    ]

    comments = entry.get("comments") or []
    if isinstance(comments, Sequence):
        for comment in comments:
            if not isinstance(comment, Mapping):
                continue
            if comment.get("commentType") != "ALTERNATIVE PRODUCTS":
                continue
            isoforms = comment.get("isoforms") or comment.get("isoform") or []
            if isinstance(isoforms, Mapping):
                isoforms = [isoforms]
            if not isinstance(isoforms, Sequence):
                continue
            for isoform in isoforms:
                if not isinstance(isoform, Mapping):
                    continue
                isoform_ids = isoform.get("isoformIds") or isoform.get("isoformId")
                if isinstance(isoform_ids, Sequence) and not isinstance(isoform_ids, (str, bytes)):
                    isoform_acc = isoform_ids[0] if isoform_ids else None
                else:
                    isoform_acc = isoform_ids
                if not isoform_acc:
                    continue
                names = isoform.get("names") or isoform.get("name")
                if isinstance(names, Mapping):
                    names = [names]
                iso_name = None
                if isinstance(names, Sequence):
                    for name in names:
                        if isinstance(name, Mapping):
                            value = name.get("value") or name.get("label")
                        else:
                            value = name
                        if value:
                            iso_name = str(value)
                            break
                sequence = isoform.get("sequence")
                if isinstance(sequence, Mapping):
                    length = sequence.get("length")
                else:
                    length = None
                try:
                    length_value = int(length) if length is not None else None
                except (TypeError, ValueError):
                    length_value = None
                records.append(
                    {
                        "canonical_accession": canonical_str,
                        "isoform_accession": str(isoform_acc),
                        "isoform_name": iso_name,
                        "is_canonical": False,
                        "sequence_length": length_value,
                        "source": "isoform",
                    }
                )
    return records
