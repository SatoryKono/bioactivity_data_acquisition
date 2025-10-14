"""CLI entrypoint orchestrating the publications enrichment pipeline."""
from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import List, Optional

import pandas as pd
import typer
from pandera.errors import SchemaErrors

from project.library.clients.base import create_session
from project.library.clients.chembl import ChemblClient
from project.library.clients.crossref import CrossrefClient
from project.library.clients.openalex import OpenAlexClient
from project.library.clients.pubmed import PubMedClient
from project.library.clients.semscholar import SemanticScholarClient
from project.library.io.normalize import coerce_text, normalise_doi
from project.library.io.read_write import read_input_csv, write_output_csv
from project.library.utils.errors import PipelineExecutionError, SchemaMismatchError
from project.library.utils.joins import merge_records
from project.library.utils.logging import get_logger, setup_logging
from project.library.utils.rate_limit import build_rate_limiter
from project.library.validation.input_schema import validate_input
from project.library.validation.output_schema import OUTPUT_COLUMNS, validate_output

app = typer.Typer(help="Fetch and enrich publication metadata from multiple sources.")


def _first_non_empty(values: List[Optional[str]]) -> Optional[str]:
    for value in values:
        if value not in (None, ""):
            return value
    return None


@app.command()
def main(
    input_path: Path = typer.Argument(..., exists=True, readable=True, help="Path to input CSV."),
    output_path: Path = typer.Argument(..., help="Path to write the enriched CSV."),
    run_id: Optional[str] = typer.Option(None, help="Optional run identifier."),
    global_rps: Optional[float] = typer.Option(10.0, help="Global requests-per-second limit."),
) -> None:
    setup_logging()
    resolved_run_id = run_id or uuid.uuid4().hex
    pipeline_logger = get_logger(resolved_run_id, stage="pipeline", source="cli")
    pipeline_logger.info("starting pipeline", extra={"extra_fields": {"input": str(input_path), "output": str(output_path)}})

    try:
        df_input = read_input_csv(input_path)
        df_validated = validate_input(df_input)
    except SchemaErrors as exc:
        raise SchemaMismatchError("Input data failed validation") from exc

    session = create_session()
    global_limiter = build_rate_limiter(global_rps)
    chembl = ChemblClient(run_id=resolved_run_id, session=session, global_limiter=global_limiter)
    crossref = CrossrefClient(run_id=resolved_run_id, session=session, global_limiter=global_limiter)
    openalex = OpenAlexClient(run_id=resolved_run_id, session=session, global_limiter=global_limiter)
    pubmed = PubMedClient(run_id=resolved_run_id, session=session, global_limiter=global_limiter)
    semscholar = SemanticScholarClient(run_id=resolved_run_id, session=session, global_limiter=global_limiter)

    enriched_rows = []
    for row in df_validated.itertuples(index=False):
        doc_id = coerce_text(getattr(row, "document_chembl_id", None))
        raw_doi = normalise_doi(getattr(row, "doi", None))
        raw_pmid = coerce_text(getattr(row, "pmid", None))
        row_logger = get_logger(resolved_run_id, stage="extract", source="pipeline", doc_id=doc_id, doi=raw_doi, pmid=raw_pmid)
        row_logger.info("processing row")

        chembl_data = {}
        try:
            chembl_data = chembl.fetch_by_doc_id(doc_id)
        except Exception:
            # Errors already logged by the client-specific logger.
            chembl_data = {}

        doi_candidates = [raw_doi, chembl_data.get("chembl.doi")]
        pmid_candidates = [raw_pmid, chembl_data.get("chembl.pmid")]

        crossref_data = {}
        doi_lookup = _first_non_empty(doi_candidates)
        try:
            if doi_lookup:
                crossref_data = crossref.fetch_by_doi(doi_lookup)
            else:
                crossref_data = crossref.fetch_by_pmid(_first_non_empty(pmid_candidates))
        except Exception:
            crossref_data = {}
        doi_candidates.append(crossref_data.get("crossref.doi"))
        pmid_candidates.append(crossref_data.get("crossref.pmid"))

        openalex_data = {}
        try:
            if doi_lookup:
                openalex_data = openalex.fetch_by_doi(doi_lookup)
            if not openalex_data:
                openalex_data = openalex.fetch_by_pmid(_first_non_empty(pmid_candidates))
        except Exception:
            openalex_data = {}
        doi_candidates.append(openalex_data.get("openalex.doi"))
        pmid_candidates.append(openalex_data.get("openalex.pmid"))

        pmid_lookup = _first_non_empty(pmid_candidates)
        pubmed_data = {}
        semscholar_data = {}
        if pmid_lookup:
            try:
                pubmed_data = pubmed.fetch_by_pmid(pmid_lookup)
            except Exception:
                pubmed_data = {}
            try:
                semscholar_data = semscholar.fetch_by_pmid(pmid_lookup)
            except Exception:
                semscholar_data = {}
        doi_candidates.extend([
            pubmed_data.get("pubmed.doi"),
            semscholar_data.get("semscholar.doi"),
        ])
        pmid_candidates.extend([
            pubmed_data.get("pubmed.pmid"),
            semscholar_data.get("semscholar.pmid"),
        ])

        doi_key = _first_non_empty([normalise_doi(v) for v in doi_candidates])
        pmid_value = _first_non_empty([coerce_text(v) for v in pmid_candidates])

        base_record = {
            "chembl.document_chembl_id": doc_id,
            "doi_key": doi_key,
            "pmid": pmid_value,
        }
        merged = merge_records(base_record, [chembl_data, crossref_data, openalex_data, pubmed_data, semscholar_data])
        merged["doi_key"] = doi_key
        merged["pmid"] = pmid_value
        enriched_rows.append(merged)

    df_output = pd.DataFrame(enriched_rows)
    if not df_output.empty:
        df_output = df_output.sort_values(
            by=["chembl.document_chembl_id", "doi_key", "pmid"],
            na_position="last",
        ).reset_index(drop=True)

    try:
        validate_output(df_output)
    except SchemaErrors as exc:
        raise SchemaMismatchError("Output data failed validation") from exc

    write_output_csv(df_output, output_path, columns=OUTPUT_COLUMNS)
    pipeline_logger.info("pipeline finished", extra={"extra_fields": {"rows": len(df_output)}})


if __name__ == "__main__":
    try:
        app()
    except PipelineExecutionError as exc:  # pragma: no cover - CLI exit handling
        typer.echo(str(exc), err=True)
        sys.exit(1)
