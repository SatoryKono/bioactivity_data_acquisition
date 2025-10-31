"""Reusable UniProt enrichment service shared across pipelines."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

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
class UniProtService:
    """Service encapsulating UniProt enrichment logic."""

    search_client: UnifiedAPIClient | None = None
    id_mapping_client: UnifiedAPIClient | None = None
    orthologs_client: UnifiedAPIClient | None = None
    batch_size: int = 50
    id_mapping_batch_size: int = 200
    id_mapping_poll_interval: float = 2.0
    id_mapping_max_wait: float = 120.0
    uniprot_fields: str = (
        "accession,primaryAccession,secondaryAccession,protein_name,gene_primary,gene_synonym,"
        "organism_name,organism_id,lineage,sequence_length,cc_subcellular_location,cc_ptm,features"
    )
    ortholog_fields: str = "accession,organism_name,organism_id,protein_name"
    ortholog_priority: dict[str, int] | None = None

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

        entries = self.fetch_entries(accessions)
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

        if unresolved:
            mapping_df = self.run_id_mapping(unresolved.values())
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
                        extra_entries = self.fetch_entries([canonical_acc])
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

        if unresolved:
            gene_cache: dict[str, dict[str, Any] | None] = {}
            for idx, accession in list(unresolved.items()):
                gene_symbol = self._safe_loc(working_df, idx, gene_symbol_column)
                organism = self._safe_loc(working_df, idx, organism_column)
                if not gene_symbol:
                    continue
                cache_key = f"{gene_symbol}|{organism}" if organism else str(gene_symbol)
                if cache_key not in gene_cache:
                    gene_cache[cache_key] = self.search_by_gene(str(gene_symbol), organism)
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

        if unresolved:
            for idx, accession in list(unresolved.items()):
                canonical = self._safe_loc(working_df, idx, accession_column)
                taxonomy_id = None
                if taxonomy_column and taxonomy_column in working_df.columns:
                    taxonomy_id = self._safe_loc(working_df, idx, taxonomy_column)
                orthologs = self.fetch_orthologs(str(canonical), taxonomy_id)
                if orthologs.empty:
                    continue
                orthologs = orthologs.sort_values(by="priority").reset_index(drop=True)
                best = orthologs.iloc[0]
                ortholog_acc = best.get("ortholog_accession")
                if not ortholog_acc:
                    continue
                entry = entries.get(ortholog_acc)
                if entry is None:
                    extra_entries = self.fetch_entries([ortholog_acc])
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
            record = {
                "canonical_accession": canonical,
                "protein_name": self._extract_protein_name(entry),
                "gene_primary": self._extract_gene_primary(entry),
                "gene_synonyms": "; ".join(self._extract_gene_synonyms(entry)),
                "organism_name": self._extract_organism(entry),
                "organism_id": self._extract_taxonomy_id(entry),
                "lineage": "; ".join(self._extract_lineage(entry)),
                "sequence_length": self._extract_sequence_length(entry),
                "secondary_accessions": "; ".join(self._extract_secondary(entry)),
            }
            silver_records.append(record)

            isoforms = self.expand_isoforms(entry)
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

    def fetch_entries(self, accessions: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Retrieve UniProt entries for provided accessions."""

        if self.search_client is None:
            return {}

        entries: dict[str, dict[str, Any]] = {}
        for chunk in self._chunked(accessions, self.batch_size):
            query_terms = [f"(accession:{acc})" for acc in chunk]
            params = {
                "query": " OR ".join(query_terms),
                "fields": self.uniprot_fields,
                "format": "json",
                "size": max(len(chunk), 25),
            }
            try:
                payload = self.search_client.request_json("/search", params=params)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_search_failed", error=str(exc), query=params["query"])
                continue

            results = payload.get("results") or payload.get("entries") or []
            for result in results:
                primary = result.get("primaryAccession") or result.get("accession")
                if not primary:
                    continue
                entries[str(primary)] = result

        return entries

    def run_id_mapping(self, accessions: Iterable[str]) -> pd.DataFrame:
        """Resolve obsolete accessions via UniProt ID mapping service."""

        if self.id_mapping_client is None:
            return pd.DataFrame(columns=["submitted_id", "canonical_accession"])

        rows: list[dict[str, Any]] = []
        for chunk in self._chunked(accessions, self.id_mapping_batch_size):
            payload = {
                "from": "UniProtKB_AC-ID",
                "to": "UniProtKB",
                "ids": ",".join(chunk),
            }
            try:
                response = self.id_mapping_client.request_json(
                    "/run", method="POST", data=payload
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_submission_failed", error=str(exc))
                continue

            job_id = response.get("jobId") or response.get("job_id")
            if not job_id:
                logger.warning("uniprot_idmapping_no_job", payload=payload)
                continue

            job_result = self._poll_id_mapping(job_id)
            if not job_result:
                continue

            results = job_result.get("results") or job_result.get("mappedTo") or []
            for item in results:
                submitted = item.get("from") or item.get("accession") or item.get("input")
                to_entry = item.get("to") or item.get("mappedTo") or {}
                if isinstance(to_entry, dict):
                    canonical = to_entry.get("primaryAccession") or to_entry.get("accession")
                    isoform = to_entry.get("isoformAccession") or to_entry.get("isoform")
                else:
                    canonical = to_entry
                    isoform = None
                rows.append(
                    {
                        "submitted_id": submitted,
                        "canonical_accession": canonical,
                        "isoform_accession": isoform,
                    }
                )

        return pd.DataFrame(rows).convert_dtypes()

    def fetch_orthologs(self, accession: str, taxonomy_id: Any | None = None) -> pd.DataFrame:
        """Fetch ortholog relationships for ``accession`` if configured."""

        client = self.orthologs_client or self.search_client
        if client is None:
            return pd.DataFrame(
                columns=["source_accession", "ortholog_accession", "organism_id", "organism", "priority"]
            )

        params = {
            "query": f"relationship_type:ortholog AND (accession:{accession} OR xref:{accession})",
            "fields": self.ortholog_fields,
            "format": "json",
            "size": 200,
        }
        try:
            payload = client.request_json("/", params=params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("ortholog_fetch_failed", accession=accession, error=str(exc))
            return pd.DataFrame(
                columns=["source_accession", "ortholog_accession", "organism_id", "organism", "priority"]
            )

        results = payload.get("results") or payload.get("entries") or []
        records: list[dict[str, Any]] = []
        for item in results:
            ortholog_acc = item.get("primaryAccession") or item.get("accession")
            organism = item.get("organism", {}) if isinstance(item.get("organism"), dict) else item.get("organism")
            if isinstance(organism, dict):
                organism_name = organism.get("scientificName") or organism.get("commonName")
                organism_id = organism.get("taxonId") or organism.get("taxonIdentifier")
            else:
                organism_name = organism
                organism_id = item.get("organismId")
            organism_id_str = str(organism_id) if organism_id is not None else None
            priority = self._ortholog_priority().get(organism_id_str or "", 99)
            records.append(
                {
                    "source_accession": accession,
                    "ortholog_accession": ortholog_acc,
                    "organism": organism_name,
                    "organism_id": organism_id,
                    "priority": priority,
                }
            )

        if taxonomy_id is not None:
            try:
                taxonomy_str = str(int(taxonomy_id))
            except (TypeError, ValueError):
                taxonomy_str = None
            if taxonomy_str:
                for record in records:
                    if str(record.get("organism_id")) == taxonomy_str:
                        record["priority"] = min(record.get("priority", 99), -1)

        return pd.DataFrame(records).convert_dtypes()

    def search_by_gene(self, gene_symbol: str, organism: Any | None = None) -> dict[str, Any] | None:
        """Search UniProt entries by gene symbol as fallback."""

        if self.search_client is None:
            return None

        query = f"gene_exact:{gene_symbol}"
        if organism:
            query = f"{query} AND organism_name:\"{organism}\""

        params = {
            "query": query,
            "fields": self.uniprot_fields,
            "format": "json",
            "size": 1,
        }
        try:
            payload = self.search_client.request_json("/search", params=params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("uniprot_gene_search_failed", gene=gene_symbol, error=str(exc))
            return None

        results = payload.get("results") or payload.get("entries") or []
        if not results:
            return None
        return results[0]

    def expand_isoforms(self, entry: dict[str, Any]) -> pd.DataFrame:
        """Expand isoform records from a UniProt entry."""

        canonical = entry.get("primaryAccession") or entry.get("accession")
        sequence_length = self._extract_sequence_length(entry)
        records: list[dict[str, Any]] = [
            {
                "canonical_accession": canonical,
                "isoform_accession": canonical,
                "isoform_name": self._extract_protein_name(entry),
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
                names = isoform.get("names") or []
                if isinstance(names, list):
                    iso_name = next(
                        (n.get("value") for n in names if isinstance(n, dict) and n.get("value")),
                        None,
                    )
                elif isinstance(names, dict):
                    iso_name = names.get("value")
                else:
                    iso_name = None
                sequence = isoform.get("sequence")
                if isinstance(sequence, dict):
                    length = sequence.get("length")
                else:
                    length = None
                records.append(
                    {
                        "canonical_accession": canonical,
                        "isoform_accession": isoform_acc,
                        "isoform_name": iso_name,
                        "is_canonical": False,
                        "sequence_length": length,
                        "source": "isoform",
                    }
                )

        return pd.DataFrame(records).convert_dtypes()

    @staticmethod
    def _chunked(values: Iterable[str], size: int) -> Iterable[list[str]]:
        batch: list[str] = []
        for value in values:
            batch.append(value)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _poll_id_mapping(self, job_id: str) -> dict[str, Any] | None:
        if self.id_mapping_client is None:
            return None

        elapsed = 0.0
        while elapsed < self.id_mapping_max_wait:
            try:
                status = self.id_mapping_client.request_json(f"/status/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_status_failed", job_id=job_id, error=str(exc))
                return None

            job_status = status.get("jobStatus") or status.get("status")
            if job_status in {"RUNNING", "PENDING"}:
                time.sleep(self.id_mapping_poll_interval)
                elapsed += self.id_mapping_poll_interval
                continue

            if job_status not in {"FINISHED", "finished", "SUCCESS"}:
                logger.warning("uniprot_idmapping_failed", job_id=job_id, status=job_status)
                return None

            try:
                return self.id_mapping_client.request_json(f"/results/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_results_failed", job_id=job_id, error=str(exc))
                return None

        logger.warning("uniprot_idmapping_timeout", job_id=job_id)
        return None

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
        df.at[idx, "uniprot_gene_primary"] = self._extract_gene_primary(entry)
        df.at[idx, "uniprot_gene_synonyms"] = "; ".join(self._extract_gene_synonyms(entry))
        df.at[idx, "uniprot_protein_name"] = self._extract_protein_name(entry)
        df.at[idx, "uniprot_sequence_length"] = self._extract_sequence_length(entry)
        df.at[idx, "uniprot_taxonomy_id"] = self._extract_taxonomy_id(entry)
        df.at[idx, "uniprot_taxonomy_name"] = self._extract_organism(entry)
        df.at[idx, "uniprot_lineage"] = "; ".join(self._extract_lineage(entry))
        df.at[idx, "uniprot_secondary_accessions"] = "; ".join(self._extract_secondary(entry))

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

    @staticmethod
    def _extract_gene_primary(entry: dict[str, Any]) -> str | None:
        genes = entry.get("genes") or []
        if isinstance(genes, list):
            for gene in genes:
                primary = gene.get("geneName") if isinstance(gene, dict) else None
                if isinstance(primary, dict):
                    value = primary.get("value")
                    if value:
                        return str(value)
        return None

    @staticmethod
    def _extract_gene_synonyms(entry: dict[str, Any]) -> list[str]:
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

    @staticmethod
    def _extract_secondary(entry: dict[str, Any]) -> list[str]:
        secondary = entry.get("secondaryAccessions") or entry.get("secondaryAccession")
        if isinstance(secondary, list):
            return [str(value) for value in secondary if value]
        if secondary:
            return [str(secondary)]
        return []

    @staticmethod
    def _extract_protein_name(entry: dict[str, Any]) -> str | None:
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

    @staticmethod
    def _extract_sequence_length(entry: dict[str, Any]) -> int | None:
        sequence = entry.get("sequence")
        if isinstance(sequence, dict):
            length = sequence.get("length")
            try:
                return int(length) if length is not None else None
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def _extract_taxonomy_id(entry: dict[str, Any]) -> int | None:
        organism = entry.get("organism", {})
        if isinstance(organism, dict):
            taxon = organism.get("taxonId") or organism.get("taxonIdentifier")
            try:
                return int(taxon) if taxon is not None else None
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def _extract_organism(entry: dict[str, Any]) -> str | None:
        organism = entry.get("organism", {})
        if isinstance(organism, dict):
            name = organism.get("scientificName") or organism.get("commonName")
            if name:
                return str(name)
        return None

    @staticmethod
    def _extract_lineage(entry: dict[str, Any]) -> list[str]:
        organism = entry.get("organism", {})
        lineage = organism.get("lineage") if isinstance(organism, dict) else None
        if isinstance(lineage, list):
            return [str(value) for value in lineage if value]
        return []

    def _ortholog_priority(self) -> dict[str, int]:
        if self.ortholog_priority is not None:
            return self.ortholog_priority
        return {
            "9606": 0,
            "10090": 1,
            "10116": 2,
        }
