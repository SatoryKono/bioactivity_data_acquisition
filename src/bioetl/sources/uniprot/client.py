"""HTTP client wrappers for interacting with the UniProt REST API."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


def _chunked(values: Iterable[str], size: int) -> Iterable[list[str]]:
    """Yield ``values`` in lists of ``size`` items."""

    batch: list[str] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


@dataclass(slots=True)
class UniProtSearchClient:
    """Client wrapper for UniProtKB search endpoints."""

    client: UnifiedAPIClient | None
    fields: str
    batch_size: int = 50

    def fetch_entries(self, accessions: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Retrieve UniProt entries for the given ``accessions``."""

        if self.client is None:
            return {}

        entries: dict[str, dict[str, Any]] = {}
        for chunk in _chunked(accessions, max(self.batch_size, 1)):
            if not chunk:
                continue
            query_terms = [f"(accession:{acc})" for acc in chunk]
            params = {
                "query": " OR ".join(query_terms),
                "fields": self.fields,
                "format": "json",
                "size": max(len(chunk), 25),
            }
            try:
                payload = self.client.request_json("/search", params=params)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "uniprot_search_failed", error=str(exc), query=params["query"]
                )
                continue

            results = payload.get("results") or payload.get("entries") or []
            for result in results:
                primary = result.get("primaryAccession") or result.get("accession")
                if not primary:
                    continue
                entries[str(primary)] = result

        return entries

    def search_by_gene(self, gene_symbol: str, organism: Any | None = None) -> dict[str, Any] | None:
        """Search for an entry by ``gene_symbol`` optionally scoped to ``organism``."""

        if self.client is None:
            return None

        query = f"gene_exact:{gene_symbol}"
        if organism:
            query = f"{query} AND organism_name:\"{organism}\""

        params = {
            "query": query,
            "fields": self.fields,
            "format": "json",
            "size": 1,
        }
        try:
            payload = self.client.request_json("/search", params=params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "uniprot_gene_search_failed", gene=gene_symbol, error=str(exc)
            )
            return None

        results = payload.get("results") or payload.get("entries") or []
        if not results:
            return None
        return results[0]


@dataclass(slots=True)
class UniProtIdMappingClient:
    """Client helper for the UniProt ID mapping API."""

    client: UnifiedAPIClient | None
    batch_size: int = 200
    poll_interval: float = 2.0
    max_wait: float = 120.0

    def map_accessions(self, accessions: Iterable[str]) -> pd.DataFrame:
        """Resolve historical accessions to their canonical counterparts."""

        if self.client is None:
            return pd.DataFrame(
                columns=["submitted_id", "canonical_accession", "isoform_accession"]
            )

        rows: list[dict[str, Any]] = []
        for chunk in _chunked(accessions, max(self.batch_size, 1)):
            if not chunk:
                continue
            payload = {
                "from": "UniProtKB_AC-ID",
                "to": "UniProtKB",
                "ids": ",".join(chunk),
            }
            try:
                response = self.client.request_json("/run", method="POST", data=payload)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_submission_failed", error=str(exc))
                continue

            job_id = response.get("jobId") or response.get("job_id")
            if not job_id:
                logger.warning("uniprot_idmapping_no_job", payload=payload)
                continue

            job_result = self._poll_job(job_id)
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

    def _poll_job(self, job_id: str) -> dict[str, Any] | None:
        """Poll the mapping job until completion or timeout."""

        if self.client is None:
            return None

        elapsed = 0.0
        while elapsed < self.max_wait:
            try:
                status = self.client.request_json(f"/status/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "uniprot_idmapping_status_failed", job_id=job_id, error=str(exc)
                )
                return None

            job_status = status.get("jobStatus") or status.get("status")
            if job_status in {"RUNNING", "PENDING"}:
                time.sleep(self.poll_interval)
                elapsed += self.poll_interval
                continue

            if job_status not in {"FINISHED", "finished", "SUCCESS"}:
                logger.warning(
                    "uniprot_idmapping_failed", job_id=job_id, status=job_status
                )
                return None

            try:
                return self.client.request_json(f"/results/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning(
                    "uniprot_idmapping_results_failed", job_id=job_id, error=str(exc)
                )
                return None

        logger.warning("uniprot_idmapping_timeout", job_id=job_id)
        return None


@dataclass(slots=True)
class UniProtOrthologClient:
    """Client helper for fetching UniProt ortholog relationships."""

    client: UnifiedAPIClient | None
    fields: str
    priority_map: dict[str, int]

    def fetch(self, accession: str, taxonomy_id: Any | None = None) -> pd.DataFrame:
        """Return ortholog relationships for ``accession`` if supported."""

        if self.client is None:
            return pd.DataFrame(
                columns=["source_accession", "ortholog_accession", "organism_id", "organism", "priority"]
            )

        params = {
            "query": f"relationship_type:ortholog AND (accession:{accession} OR xref:{accession})",
            "fields": self.fields,
            "format": "json",
            "size": 200,
        }
        try:
            payload = self.client.request_json("/", params=params)
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
            priority = self.priority_map.get(organism_id_str or "", 99)
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

