"""UniProt enrichment normalizer used by pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger

from .client import (
    UniProtIdMappingClient,
    UniProtOrthologsClient,
    UniProtSearchClient,
)

UniProtOrthologClient = UniProtOrthologsClient
from .parser_utils import (
    build_silver_record,
    expand_isoforms,
    extract_gene_primary,
    extract_gene_synonyms,
    extract_lineage,
    extract_organism,
    extract_protein_name,
    extract_secondary_accessions,
    extract_sequence_length,
    extract_taxonomy_id,
)

logger = UnifiedLogger.get(__name__)


@dataclass(slots=True)
class UniProtEnrichmentResult:
    """Container holding UniProt enrichment artefacts."""

    dataframe: pd.DataFrame
    silver: pd.DataFrame
    components: pd.DataFrame
    metrics: dict[str, Any]
    missing_mappings: list[dict[str, Any]] = field(default_factory=list)
    validation_issues: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class UniProtNormalizer:
    """Service class orchestrating UniProt enrichment and normalization."""

    search_client: UniProtSearchClient | None = None
    id_mapping_client: UniProtIdMappingClient | None = None
    ortholog_client: UniProtOrthologClient | None = None

    def enrich_targets(
        self,
        df: pd.DataFrame,
        *,
        accession_column: str = "uniprot_accession",
        target_id_column: str = "target_chembl_id",
        gene_symbol_column: str = "gene_symbol",
        organism_column: str = "organism",
        taxonomy_column: str | None = "taxonomy_id",
    ) -> UniProtEnrichmentResult:
        """Enrich ``df`` with UniProt metadata using configured clients."""

        working_df = df.reset_index(drop=True).copy()
        working_df = working_df.convert_dtypes()

        if accession_column not in working_df.columns:
            return UniProtEnrichmentResult(
                dataframe=working_df,
                silver=pd.DataFrame(),
                components=pd.DataFrame(),
                metrics={},
            )

        accessions = {
            str(value).strip()
            for value in working_df[accession_column].dropna().tolist()
            if str(value).strip()
        }
        if not accessions:
            logger.info("uniprot_enrichment_skipped", reason="empty_accessions")
            return UniProtEnrichmentResult(
                dataframe=working_df,
                silver=pd.DataFrame(),
                components=pd.DataFrame(),
                metrics={},
            )

        logger.info("uniprot_enrichment_started", accession_count=len(accessions))

        entries: dict[str, dict[str, Any]] = {}
        search_client = self.search_client
        if search_client is not None:
            entries = search_client.fetch_entries(accessions)

        secondary_index: dict[str, dict[str, Any]] = {}
        for entry in entries.values():
            for field in ("secondaryAccession", "secondaryAccessions"):
                secondary_values = entry.get(field, [])
                if isinstance(secondary_values, str):
                    secondary_values = [secondary_values]
                for secondary in secondary_values or []:
                    secondary_index[str(secondary)] = entry

        used_entries: dict[str, dict[str, Any]] = {}
        unresolved: dict[int, str] = {}
        metrics: dict[str, Any] = {}
        fallback_counts: dict[str, int] = {}
        resolved_rows = 0
        total_with_accession = 0
        missing_records: list[dict[str, Any]] = []
        validation_issues: list[dict[str, Any]] = []

        for idx, row in working_df.iterrows():
            accession = row.get(accession_column)
            if pd.isna(accession) or not str(accession).strip():
                continue

            accession_str = str(accession).strip()
            total_with_accession += 1
            entry = entries.get(accession_str)
            merge_strategy = "direct"

            if entry is None:
                entry = secondary_index.get(accession_str)
                if entry is not None:
                    merge_strategy = "secondary_accession"
                    resolved = entry.get("primaryAccession") or entry.get("accession")
                    missing_records.append(
                        self._missing_mapping_record(
                            stage="uniprot",
                            target_id=self._row_value(row, target_id_column),
                            accession=accession_str,
                            resolved_accession=resolved,
                            resolution="secondary_accession",
                            status="resolved",
                        )
                    )
                    fallback_counts["secondary_accession"] = (
                        fallback_counts.get("secondary_accession", 0) + 1
                    )

            if entry is None:
                unresolved[idx] = accession_str
                continue

            resolved_rows += 1
            canonical = entry.get("primaryAccession") or entry.get("accession")
            if canonical:
                used_entries[str(canonical)] = entry
            self._apply_entry_enrichment(working_df, int(idx), entry, merge_strategy)

        if unresolved and self.id_mapping_client is not None:
            mapping_df = self.id_mapping_client.map_accessions(unresolved.values())
            if not mapping_df.empty:
                for idx, submitted in list(unresolved.items()):
                    mapped_rows = mapping_df[mapping_df["submitted_id"] == submitted]
                    if mapped_rows.empty:
                        continue
                    mapped_row = mapped_rows.iloc[0]
                    canonical_acc = mapped_row.get("canonical_accession")
                    if not canonical_acc:
                        continue
                    entry = entries.get(canonical_acc)
                    if entry is None and canonical_acc not in used_entries:
                        if search_client is not None:
                            extra_entries = search_client.fetch_entries([canonical_acc])
                        else:
                            extra_entries = {}
                        if extra_entries:
                            entries.update(extra_entries)
                            entry = extra_entries.get(canonical_acc)
                    if entry is None:
                        continue
                    used_entries[str(canonical_acc)] = entry
                    fallback_counts["idmapping"] = fallback_counts.get("idmapping", 0) + 1
                    missing_records.append(
                        self._missing_mapping_record(
                            stage="uniprot",
                            target_id=self._safe_loc(working_df, idx, target_id_column),
                            accession=submitted,
                            resolved_accession=canonical_acc,
                            resolution="idmapping",
                            status="resolved",
                        )
                    )
                    self._apply_entry_enrichment(working_df, idx, entry, "idmapping")
                    resolved_rows += 1
                    unresolved.pop(idx, None)

        if unresolved and search_client is not None:
            gene_cache: dict[str, dict[str, Any] | None] = {}
            for idx, accession in list(unresolved.items()):
                gene_symbol = self._safe_loc(working_df, idx, gene_symbol_column)
                organism = self._safe_loc(working_df, idx, organism_column)
                if not gene_symbol:
                    continue
                cache_key = f"{gene_symbol}|{organism}" if organism else str(gene_symbol)
                if cache_key not in gene_cache:
                    gene_cache[cache_key] = search_client.search_by_gene(
                        str(gene_symbol), organism
                    )
                entry = gene_cache[cache_key]
                if entry is None:
                    continue
                canonical_acc = entry.get("primaryAccession") or entry.get("accession")
                if not canonical_acc:
                    continue
                used_entries[str(canonical_acc)] = entry
                fallback_counts["gene_symbol"] = fallback_counts.get("gene_symbol", 0) + 1
                missing_records.append(
                    self._missing_mapping_record(
                        stage="uniprot",
                        target_id=self._safe_loc(working_df, idx, target_id_column),
                        accession=accession,
                        resolved_accession=canonical_acc,
                        resolution="gene_symbol",
                        status="resolved",
                        details={"gene_symbol": gene_symbol, "organism": organism},
                    )
                )
                self._apply_entry_enrichment(working_df, idx, entry, "gene_symbol")
                resolved_rows += 1
                unresolved.pop(idx, None)

        if unresolved and self.ortholog_client is not None:
            for idx, accession in list(unresolved.items()):
                canonical = self._safe_loc(working_df, idx, accession_column)
                taxonomy_id = None
                if taxonomy_column and taxonomy_column in working_df.columns:
                    taxonomy_id = self._safe_loc(working_df, idx, taxonomy_column)
                orthologs = self.ortholog_client.fetch(str(canonical), taxonomy_id)
                if orthologs.empty:
                    continue
                orthologs = orthologs.sort_values(by="priority").reset_index(drop=True)
                best = orthologs.iloc[0]
                ortholog_acc = best.get("ortholog_accession")
                if not ortholog_acc:
                    continue
                entry = entries.get(ortholog_acc)
                if entry is None:
                    if search_client is not None:
                        extra_entries = search_client.fetch_entries([ortholog_acc])
                    else:
                        extra_entries = {}
                    if extra_entries:
                        entries.update(extra_entries)
                        entry = extra_entries.get(ortholog_acc)
                if entry is None:
                    continue
                used_entries[str(ortholog_acc)] = entry
                fallback_counts["ortholog"] = fallback_counts.get("ortholog", 0) + 1
                missing_records.append(
                    self._missing_mapping_record(
                        stage="uniprot",
                        target_id=self._safe_loc(working_df, idx, target_id_column),
                        accession=accession,
                        resolved_accession=ortholog_acc,
                        resolution="ortholog",
                        status="resolved",
                        details={"ortholog_source": best.get("organism")},
                    )
                )
                self._apply_entry_enrichment(working_df, int(idx), entry, "ortholog")
                working_df.at[idx, "ortholog_source"] = best.get("organism")
                resolved_rows += 1
                unresolved.pop(idx, None)

        if unresolved:
            for idx, accession in unresolved.items():
                logger.warning("uniprot_enrichment_unresolved", accession=accession)
                target_id = self._safe_loc(working_df, idx, target_id_column)
                missing_records.append(
                    self._missing_mapping_record(
                        stage="uniprot",
                        target_id=target_id,
                        accession=accession,
                        resolution="unresolved",
                        status="unresolved",
                    )
                )
                validation_issues.append(
                    {
                        "metric": "enrichment.uniprot.unresolved",
                        "issue_type": "missing_mapping",
                        "severity": "warning",
                        "target_chembl_id": target_id,
                        "accession": accession,
                    }
                )

        coverage = resolved_rows / total_with_accession if total_with_accession else 0.0
        metrics["enrichment_success.uniprot"] = coverage
        metrics["enrichment.total_with_accession"] = total_with_accession
        metrics["enrichment.resolved"] = resolved_rows
        unresolved_count = len(unresolved)
        if unresolved_count:
            metrics["enrichment.uniprot.unresolved"] = unresolved_count
            metrics["enrichment.uniprot.unresolved_rate"] = (
                unresolved_count / total_with_accession if total_with_accession else 0.0
            )

        for name, count in fallback_counts.items():
            metrics[f"fallback.{name}.count"] = count
            if total_with_accession:
                metrics[f"fallback.{name}.rate"] = count / total_with_accession

        logger.info(
            "uniprot_enrichment_completed",
            resolved=resolved_rows,
            total=total_with_accession,
            coverage=coverage,
            fallback_counts=dict(fallback_counts),
        )

        silver_records: list[dict[str, Any]] = []
        component_records: list[dict[str, Any]] = []
        for canonical, entry in used_entries.items():
            silver_records.append(build_silver_record(canonical, entry))
            isoforms = expand_isoforms(entry)
            if not isoforms.empty:
                component_records.extend(isoforms.to_dict("records"))  # type: ignore[arg-type]

        silver_df = pd.DataFrame(silver_records).convert_dtypes()
        component_df = pd.DataFrame(component_records).convert_dtypes()

        return UniProtEnrichmentResult(
            dataframe=working_df,
            silver=silver_df,
            components=component_df,
            metrics=metrics,
            missing_mappings=missing_records,
            validation_issues=validation_issues,
        )

    def _apply_entry_enrichment(
        self,
        df: pd.DataFrame,
        idx: int,
        entry: dict[str, Any],
        strategy: str,
    ) -> None:
        canonical = entry.get("primaryAccession") or entry.get("accession")
        df.at[idx, "uniprot_canonical_accession"] = canonical
        df.at[idx, "uniprot_merge_strategy"] = strategy
        df.at[idx, "uniprot_gene_primary"] = extract_gene_primary(entry)
        df.at[idx, "uniprot_gene_synonyms"] = "; ".join(extract_gene_synonyms(entry))
        df.at[idx, "uniprot_protein_name"] = extract_protein_name(entry)
        df.at[idx, "uniprot_sequence_length"] = extract_sequence_length(entry)
        df.at[idx, "uniprot_taxonomy_id"] = extract_taxonomy_id(entry)
        df.at[idx, "uniprot_taxonomy_name"] = extract_organism(entry)
        df.at[idx, "uniprot_lineage"] = "; ".join(extract_lineage(entry))
        df.at[idx, "uniprot_secondary_accessions"] = "; ".join(
            extract_secondary_accessions(entry)
        )

    @staticmethod
    def _row_value(row: pd.Series, column: str | None) -> Any:
        if column is None:
            return None
        return row.get(column)

    @staticmethod
    def _safe_loc(df: pd.DataFrame, idx: int, column: str | None) -> Any:
        if column is None or column not in df.columns or idx not in df.index:
            return None
        return df.at[idx, column]

    @staticmethod
    def _missing_mapping_record(
        *,
        stage: str,
        target_id: Any | None,
        accession: Any | None,
        resolution: str,
        status: str,
        resolved_accession: Any | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "stage": stage,
            "target_id": target_id,
            "accession": accession,
            "resolved_accession": resolved_accession,
            "resolution": resolution,
            "status": status,
        }
        if details:
            try:
                record["details"] = json.dumps(details, ensure_ascii=False, sort_keys=True)
            except TypeError:
                record["details"] = str(details)
        return record

