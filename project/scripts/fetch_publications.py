"""CLI entry point for the publications ETL."""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import typer

from library.clients.base import SessionConfig, SessionManager
from library.clients.chembl import ChemblClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semscholar import SemanticScholarClient
from library.io.normalize import coerce_text, normalise_doi, to_lc_stripped
from library.io.read_write import read_input_csv, write_output_csv
from library.utils.errors import SourceRequestError
from library.utils.joins import flatten_payloads
from library.utils.logging import configure_logging, get_logger, write_source_error
from library.validation.input_schema import InputColumns, input_schema
from library.validation.output_schema import OutputColumns, output_schema

app = typer.Typer(help="Fetch and consolidate publication metadata.")


@app.command()
def run(
    input: Path = typer.Option(..., exists=True, readable=True, help="Input CSV path."),
    output: Path = typer.Option(..., help="Output CSV path."),
    error_dir: Path = typer.Option(Path("logs"), help="Directory for *.error files."),
    cache_name: str = typer.Option(".http_cache", help="Cache name for HTTP session."),
) -> None:
    """Execute the ETL pipeline."""
    run_id = uuid.uuid4().hex
    configure_logging(run_id)
    logger = get_logger(stage="run")
    logger.info("starting", msg="pipeline starting", run_id=run_id)

    session_manager = SessionManager(SessionConfig(cache_name=cache_name))
    chembl_client = ChemblClient(session_manager)
    pubmed_client = PubMedClient(session_manager)
    semscholar_client = SemanticScholarClient(session_manager)
    crossref_client = CrossrefClient(session_manager)
    openalex_client = OpenAlexClient(session_manager)

    input_df = read_input_csv(input, input_schema)

    documents: List[Dict[str, object]] = []
    pmids: List[str] = []

    for row in input_df.itertuples(index=False):
        document_id = coerce_text(getattr(row, InputColumns.DOCUMENT_ID)) or ""
        doi_candidates = [getattr(row, InputColumns.DOI, None)]
        pmid_candidates = [getattr(row, InputColumns.PMID, None)]

        chembl_logger = get_logger(stage="extract", source="chembl", doc_id=document_id)
        try:
            chembl_payload = chembl_client.fetch_by_doc_id(document_id)
        except SourceRequestError as exc:
            chembl_logger.error("chembl_fetch_failed", msg=str(exc))
            write_source_error("chembl", f"{document_id}: {exc}", directory=error_dir)
            chembl_payload = {"document_chembl_id": document_id}
        doi_candidates.append(chembl_payload.get("doi"))
        pmid_candidates.append(chembl_payload.get("pmid"))

        doi_key = next((normalise_doi(coerce_text(doi)) for doi in doi_candidates if doi), None)
        pmid = next((to_lc_stripped(coerce_text(pmid)) for pmid in pmid_candidates if pmid), None)
        if pmid:
            pmids.append(pmid)

        documents.append(
            {
                "document_id": document_id,
                "doi_key": doi_key,
                "pmid": pmid,
                "sources": {"chembl": chembl_payload},
            }
        )

    unique_pmids = sorted({pmid for pmid in pmids if pmid})

    if unique_pmids:
        pubmed_logger = get_logger(stage="extract", source="pubmed")
        try:
            pubmed_payloads = pubmed_client.fetch_batch(unique_pmids)
            pubmed_logger.info("pubmed_batch", msg="fetched pubmed batch", count=len(pubmed_payloads))
        except SourceRequestError as exc:
            pubmed_logger.error("pubmed_fetch_failed", msg=str(exc))
            write_source_error("pubmed", str(exc), directory=error_dir)
            pubmed_payloads = {}

        sem_logger = get_logger(stage="extract", source="semscholar")
        try:
            semscholar_payloads = semscholar_client.fetch_batch(unique_pmids)
            sem_logger.info("semscholar_batch", msg="fetched semantic scholar batch", count=len(semscholar_payloads))
        except SourceRequestError as exc:
            sem_logger.error("semscholar_fetch_failed", msg=str(exc))
            write_source_error("semscholar", str(exc), directory=error_dir)
            semscholar_payloads = {}
    else:
        pubmed_payloads = {}
        semscholar_payloads = {}

    rows: List[Dict[str, object]] = []

    for doc in documents:
        sources: Dict[str, Dict[str, object]] = doc["sources"]  # type: ignore[assignment]
        pmid = doc["pmid"]
        doi_key = doc["doi_key"]

        if pmid and pmid in pubmed_payloads:
            sources["pubmed"] = pubmed_payloads[pmid]
            doi_key = doi_key or normalise_doi(pubmed_payloads[pmid].get("doi"))
        if pmid and pmid in semscholar_payloads:
            sources["semscholar"] = semscholar_payloads[pmid]
            doi_key = doi_key or normalise_doi(semscholar_payloads[pmid].get("doi"))

        crossref_logger = get_logger(stage="extract", source="crossref", doc_id=doc["document_id"], pmid=pmid, doi=doi_key)
        try:
            crossref_payload: Dict[str, Optional[str]] = {}
            if doi_key:
                crossref_payload = crossref_client.fetch_by_doi(doi_key)
            elif pmid:
                crossref_payload = crossref_client.fetch_by_pmid(pmid)
            if crossref_payload:
                sources["crossref"] = crossref_payload
                doi_key = doi_key or normalise_doi(crossref_payload.get("doi"))
        except SourceRequestError as exc:
            crossref_logger.error("crossref_fetch_failed", msg=str(exc))
            write_source_error("crossref", f"{doc['document_id']}: {exc}", directory=error_dir)

        openalex_logger = get_logger(stage="extract", source="openalex", doc_id=doc["document_id"], pmid=pmid, doi=doi_key)
        try:
            openalex_payload: Dict[str, Optional[str]] = {}
            if doi_key:
                openalex_payload = openalex_client.fetch_by_doi(doi_key)
            elif pmid:
                openalex_payload = openalex_client.fetch_by_pmid(pmid)
            if openalex_payload:
                sources["openalex"] = openalex_payload
                doi_key = doi_key or normalise_doi(openalex_payload.get("doi"))
        except SourceRequestError as exc:
            openalex_logger.error("openalex_fetch_failed", msg=str(exc))
            write_source_error("openalex", f"{doc['document_id']}: {exc}", directory=error_dir)

        doc["doi_key"] = doi_key

        flattened = flatten_payloads(sources)
        row = {
            OutputColumns.DOCUMENT_ID: doc["document_id"],
            OutputColumns.DOI_KEY: doi_key,
            OutputColumns.PMID: pmid,
        }
        row.update(flattened)
        rows.append(row)

    output_df = pd.DataFrame(rows)
    output_df = output_schema.validate(output_df)

    column_order = [OutputColumns.DOCUMENT_ID, OutputColumns.DOI_KEY, OutputColumns.PMID]
    other_columns = sorted(col for col in output_df.columns if col not in column_order)
    column_order.extend(other_columns)

    write_output_csv(output_df, output, column_order=column_order, sort_by=column_order[:3])
    logger.info("completed", msg="pipeline finished", rows=len(output_df))


if __name__ == "__main__":
    app()
