"""CLI entry point orchestrating the publication fetching pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import typer

from bioactivity.config import APIClientConfig, Config
from library.clients.chembl import ChEMBLClient
from library.clients.crossref import CrossrefClient
from library.clients.pubmed import PubMedClient
from library.io.normalize import (
    coerce_text,
    normalise_doi,
    parse_chembl_response,
    parse_crossref_response,
    parse_pubmed_response,
)
from library.utils.logging import setup_logging
from library.validation.input_schema import INPUT_SCHEMA
from library.validation.output_schema import OUTPUT_SCHEMA

app = typer.Typer(help="Fetch and consolidate publication metadata for ChEMBL documents.")

CONFIG_OPTION = typer.Option(
    None,
    "--config",
    "-c",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    resolve_path=True,
)

OVERRIDE_OPTION = typer.Option(
    [],
    "--set",
    "-s",
    help="Override configuration values using dotted paths",
)


def _resolve_source(config: Config, name: str) -> APIClientConfig:
    try:
        source = config.sources[name]
    except KeyError as exc:  # pragma: no cover - defensive configuration guard
        raise typer.BadParameter(f"Source '{name}' is not configured") from exc
    return source.to_client_config(config.http.global_)


def _read_input(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    return INPUT_SCHEMA.validate(df)


def _merge_titles(base: pd.DataFrame, titles: Iterable[tuple[str, str]], column: str, key: str) -> pd.DataFrame:
    frame = pd.DataFrame(list(titles), columns=[key, column]) if titles else pd.DataFrame(columns=[key, column])
    return base.merge(frame, on=key, how="left") if not frame.empty else base.assign(**{column: pd.NA})


@app.command()
def run(
    input_path: Path = typer.Argument(..., help="Path to the CSV file with document_chembl_id column."),
    output_path: Path = typer.Argument(..., help="Where to store the resulting CSV."),
    run_id: str = typer.Option("local", help="Identifier for the current pipeline run."),
    log_dir: Path = typer.Option(Path("logs"), help="Directory for JSON logs and *.error files."),
    config: Path | None = CONFIG_OPTION,
    overrides: list[str] = OVERRIDE_OPTION,
) -> None:
    parsed_overrides = _parse_override_args(overrides)
    config_model = Config.load(config, overrides=parsed_overrides)
    chembl_cfg = _resolve_source(config_model, "chembl")
    crossref_cfg = _resolve_source(config_model, "crossref")
    pubmed_cfg = _resolve_source(config_model, "pubmed")

    chembl_url = chembl_cfg.resolved_base_url
    crossref_url = crossref_cfg.resolved_base_url
    pubmed_url = pubmed_cfg.resolved_base_url
    logger = setup_logging(log_dir=log_dir).bind(run_id=run_id)
    logger.info("starting pipeline", stage="cli")

    input_df = _read_input(input_path)
    logger.info("input loaded", stage="extract", rows=len(input_df))

    base = input_df.copy()
    base["document_chembl_id"] = base["document_chembl_id"].apply(coerce_text)

    if "doi" not in base:
        base["doi"] = pd.NA
    base["doi"] = base["doi"].apply(coerce_text)
    base["doi_key"] = base["doi"].apply(normalise_doi)

    if "pmid" not in base:
        base["pmid"] = pd.NA
    base["pmid"] = base["pmid"].apply(coerce_text)

    chembl_client = ChEMBLClient(chembl_url)
    crossref_client = CrossrefClient(crossref_url)
    pubmed_client = PubMedClient(pubmed_url)

    chembl_records: list[dict[str, str | None]] = []
    for doc_id in base["document_chembl_id"].astype(str):
        bound = logger.bind(stage="fetch", source="chembl", doc_id=doc_id)
        try:
            raw = chembl_client.fetch_document(doc_id)
            parsed = parse_chembl_response(raw)
            if not parsed:
                bound.warning("no records returned")
            chembl_records.extend(parsed)
        except Exception as exc:  # pragma: no cover - defensive logging
            bound.error("chembl request failed", exc=str(exc))

    chembl_df = pd.DataFrame(chembl_records)
    if chembl_df.empty:
        chembl_df = pd.DataFrame(
            columns=["document_chembl_id", "doi_key", "doi", "pmid", "title"]
        )
    chembl_df = chembl_df.rename(
        columns={
            "doi_key": "chembl_doi_key",
            "doi": "chembl_doi",
            "pmid": "chembl_pmid",
            "title": "chembl_title",
        }
    )

    merged = base.merge(chembl_df, on="document_chembl_id", how="left")
    merged["doi_key"] = merged["chembl_doi_key"].combine_first(merged["doi_key"])
    merged["pmid"] = merged["chembl_pmid"].combine_first(merged.get("pmid"))
    merged["chembl_doi"] = merged["chembl_doi"].combine_first(merged.get("doi")) if "doi" in merged else merged["chembl_doi"]

    doi_keys = sorted({key for key in merged["doi_key"].dropna().unique() if key})
    crossref_titles: list[tuple[str, str | None]] = []
    for doi in doi_keys:
        bound = logger.bind(stage="fetch", source="crossref", doi=doi)
        try:
            raw = crossref_client.fetch_by_doi(doi)
            parsed = parse_crossref_response(raw)
            for record in parsed:
                crossref_titles.append((record.get("doi_key") or doi, record.get("title")))
        except Exception as exc:  # pragma: no cover - defensive logging
            bound.error("crossref request failed", exc=str(exc))

    merged = _merge_titles(merged, crossref_titles, "crossref_title", "doi_key")

    pmids = sorted({key for key in merged["pmid"].dropna().unique() if key})
    pubmed_titles: list[tuple[str, str | None]] = []
    for pmid in pmids:
        bound = logger.bind(stage="fetch", source="pubmed", pmid=pmid)
        try:
            raw = pubmed_client.fetch_by_pmid(pmid)
            parsed = parse_pubmed_response(raw)
            for record in parsed:
                pubmed_titles.append((record.get("pmid") or pmid, record.get("title")))
        except Exception as exc:  # pragma: no cover - defensive logging
            bound.error("pubmed request failed", exc=str(exc))

    merged = _merge_titles(merged, pubmed_titles, "pubmed_title", "pmid")

    final_df = merged[[
        "document_chembl_id",
        "doi_key",
        "pmid",
        "chembl_title",
        "chembl_doi",
        "crossref_title",
        "pubmed_title",
    ]].copy()

    final_df = final_df.sort_values(
        by=["document_chembl_id", "doi_key", "pmid"],
        na_position="last",
    ).reset_index(drop=True)

    validated = OUTPUT_SCHEMA.validate(final_df)
    logger.info("writing output", stage="load", rows=len(validated))
    validated.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("pipeline finished", stage="cli", output=str(output_path))


if __name__ == "__main__":
    app()


def _parse_override_args(values: list[str]) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise typer.BadParameter("Overrides must be in KEY=VALUE format")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter("Override key must not be empty")
        assignments[key] = value
    return assignments
