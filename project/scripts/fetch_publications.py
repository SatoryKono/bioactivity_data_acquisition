"""CLI entry point for the publications ETL pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import typer
import yaml
from dotenv import load_dotenv
from library.clients import BasePublicationsClient, ClientConfig
from library.clients.chembl import ChemblClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubmedClient
from library.clients.semscholar import SemanticScholarClient
from library.io.normalize import coerce_text, normalise_doi, normalize_publication_frame
from library.io.read_write import empty_publications_frame, read_queries, write_publications
from library.utils.errors import ConfigError, ExtractionError
from library.utils.errors import ValidationError as PipelineValidationError
from library.utils.joins import safe_left_join
from library.utils.logging import bind_context, configure_logging, get_logger, log_error_to_file
from library.utils.rate_limit import RateLimiter
from library.validation.input_schema import INPUT_SCHEMA
from library.validation.output_schema import OUTPUT_SCHEMA
from pydantic import BaseModel, Field, ValidationError

app = typer.Typer(help="Fetch and normalize publication metadata from multiple public APIs.")


@dataclass
class PipelineClients:
    chembl: ChemblClient
    pubmed: PubmedClient | None
    semscholar: SemanticScholarClient | None
    crossref: CrossrefClient | None
    openalex: OpenAlexClient | None


CLIENT_FACTORIES: Mapping[str, type[BasePublicationsClient]] = {
    "chembl": ChemblClient,
    "pubmed": PubmedClient,
    "semscholar": SemanticScholarClient,
    "crossref": CrossrefClient,
    "openalex": OpenAlexClient,
}


class LoggingConfig(BaseModel):
    level: str = Field(default="INFO")


class EtlConfig(BaseModel):
    strict_validation: bool = Field(default=True, alias="strict_validation")
    output_dir: Path | None = Field(default=None, alias="output_dir")
    batch_size: int = Field(default=50, alias="batch_size")
    global_rps: float | None = Field(default=None, alias="global_rps")

    class Config:
        allow_population_by_field_name = True


class SourceConfig(BaseModel):
    base_url: str
    api_key: str | None = None
    rate_limit_per_minute: int | None = Field(default=None, alias="rate_limit_per_minute")
    rate_limit_per_second: float | None = Field(default=None, alias="rate_limit_per_second")
    cache_expiry: int | None = Field(default=None, alias="cache_expiry")
    timeout: float | None = Field(default=None, alias="timeout")
    headers: dict[str, str] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True


class PipelineConfig(BaseModel):
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    etl: EtlConfig = Field(default_factory=EtlConfig)
    sources: dict[str, SourceConfig] = Field(default_factory=dict)


def load_config(path: Path) -> PipelineConfig:
    """Read and validate a pipeline configuration file."""

    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    try:
        return PipelineConfig.parse_obj(payload)
    except ValidationError as exc:  # type: ignore[reportUnknownVariableType]
        raise ConfigError(f"Invalid configuration: {exc}") from exc


def build_clients(config: PipelineConfig) -> PipelineClients:
    """Instantiate clients defined in the configuration."""

    global_limiter = (
        RateLimiter(rate_per_second=config.etl.global_rps)
        if config.etl.global_rps and config.etl.global_rps > 0
        else None
    )

    constructed: dict[str, BasePublicationsClient] = {}
    for name, source_config in config.sources.items():
        factory = CLIENT_FACTORIES.get(name)
        if not factory:
            continue
        default_timeout = ClientConfig.__dataclass_fields__["timeout"].default  # type: ignore[index]
        client_config = ClientConfig(
            name=name,
            base_url=source_config.base_url,
            api_key=source_config.api_key,
            rate_limit_per_minute=source_config.rate_limit_per_minute,
            rate_limit_per_second=source_config.rate_limit_per_second,
            cache_expiry=source_config.cache_expiry,
            timeout=source_config.timeout if source_config.timeout is not None else default_timeout,
            extra_headers=source_config.headers,
        )
        kwargs: dict[str, Any] = {"global_limiter": global_limiter}
        if name in {"pubmed", "semscholar"}:
            kwargs["batch_size"] = config.etl.batch_size
        constructed[name] = factory(client_config, **kwargs)

    if "chembl" not in constructed:
        raise ConfigError("ChEMBL client configuration is required")

    return PipelineClients(
        chembl=constructed["chembl"],
        pubmed=constructed.get("pubmed"),
        semscholar=constructed.get("semscholar"),
        crossref=constructed.get("crossref"),
        openalex=constructed.get("openalex"),
    )


def extract_publications(queries: pd.DataFrame, clients: PipelineClients) -> pd.DataFrame:
    """Fetch publications for each query using the configured clients."""

    logger = get_logger("pipeline")
    if queries.empty:
        return empty_publications_frame()

    chembl_records: list[dict[str, Any]] = []
    for doc_id in queries["document_chembl_id"].dropna().unique():
        try:
            record = dict(clients.chembl.fetch_by_doc_id(str(doc_id)))
            chembl_records.append(record)
        except ExtractionError as exc:
            bind_context(logger, stage="extract", source="chembl", doc_id=str(doc_id)).error(
                "chembl_fetch_failed", error=str(exc)
            )
            log_error_to_file(
                "chembl",
                {"error": str(exc), "doc_id": str(doc_id)},
            )

    chembl_df = pd.DataFrame.from_records(chembl_records)
    chembl_df.rename(
        columns={
            "document_chembl_id": "chembl.document_chembl_id",
            "title": "chembl.title",
            "doi": "chembl.doi",
            "pmid": "chembl.pmid",
            "journal": "chembl.journal",
            "year": "chembl.year",
        },
        inplace=True,
    )
    if chembl_df.empty:
        chembl_df = pd.DataFrame(
            columns=[
                "chembl.document_chembl_id",
                "chembl.title",
                "chembl.doi",
                "chembl.pmid",
                "chembl.journal",
                "chembl.year",
            ]
        )

    base = queries.rename(
        columns={
            "document_chembl_id": "chembl.document_chembl_id",
            "doi": "input.doi",
            "pmid": "input.pmid",
        }
    )

    merged = safe_left_join(base, chembl_df, on="chembl.document_chembl_id", validate="one_to_one")
    merged["chembl.document_chembl_id"] = merged["chembl.document_chembl_id"].map(coerce_text)
    merged["chembl.doi"] = merged.get("chembl.doi", pd.Series(dtype="object")).map(normalise_doi)
    merged["chembl.pmid"] = merged.get("chembl.pmid", pd.Series(dtype="object")).map(coerce_text)
    merged["input.doi"] = merged.get("input.doi", pd.Series(dtype="object")).map(normalise_doi)
    merged["input.pmid"] = merged.get("input.pmid", pd.Series(dtype="object")).map(coerce_text)

    merged["doi_key"] = merged["input.doi"].combine_first(merged["chembl.doi"])
    merged["pmid"] = merged["input.pmid"].combine_first(merged["chembl.pmid"])
    merged["pmid"] = merged["pmid"].map(coerce_text)

    pmid_values = [value for value in merged["pmid"].dropna().unique() if value]

    if clients.pubmed and pmid_values:
        pubmed_records = clients.pubmed.fetch_batch_by_pmid(pmid_values)
        if pubmed_records:
            pubmed_df = pd.DataFrame(pubmed_records.values())
            pubmed_df.rename(
                columns={
                    "title": "pubmed.title",
                    "journal": "pubmed.journal",
                    "pub_date": "pubmed.pub_date",
                    "doi": "pubmed.doi",
                },
                inplace=True,
            )
            merged = safe_left_join(merged, pubmed_df, on="pmid", validate="one_to_one")
            merged["pubmed.doi"] = merged.get("pubmed.doi", pd.Series(dtype="object")).map(
                normalise_doi
            )
            merged["doi_key"] = merged["doi_key"].combine_first(merged["pubmed.doi"])

    if clients.semscholar and pmid_values:
        sem_records = clients.semscholar.fetch_batch_by_pmid(pmid_values)
        if sem_records:
            sem_df = pd.DataFrame(sem_records.values())
            sem_df.rename(
                columns={
                    "title": "semscholar.title",
                    "year": "semscholar.year",
                    "paper_id": "semscholar.paper_id",
                    "doi": "semscholar.doi",
                },
                inplace=True,
            )
            merged = safe_left_join(merged, sem_df, on="pmid", validate="one_to_one")
            merged["semscholar.doi"] = merged.get("semscholar.doi", pd.Series(dtype="object")).map(
                normalise_doi
            )
            merged["doi_key"] = merged["doi_key"].combine_first(merged["semscholar.doi"])

    doi_values = [value for value in merged["doi_key"].dropna().unique() if value]

    if clients.crossref and doi_values:
        crossref_records: list[dict[str, Any]] = []
        for doi in doi_values:
            try:
                record = dict(clients.crossref.fetch_by_doi(doi))
                if record:
                    record.setdefault("doi", normalise_doi(doi))
                    crossref_records.append(record)
            except ExtractionError as exc:
                bind_context(logger, stage="extract", source="crossref", doi=doi).error(
                    "crossref_fetch_failed", error=str(exc)
                )
                log_error_to_file("crossref", {"error": str(exc), "doi": doi})
        if crossref_records:
            crossref_df = pd.DataFrame(crossref_records)
            crossref_df.rename(
                columns={
                    "title": "crossref.title",
                    "issued": "crossref.issued",
                    "publisher": "crossref.publisher",
                    "type": "crossref.type",
                    "doi": "crossref.doi",
                },
                inplace=True,
            )
            crossref_df["crossref.doi"] = crossref_df["crossref.doi"].map(normalise_doi)
            crossref_df["doi_key"] = crossref_df["crossref.doi"]
            merged = safe_left_join(merged, crossref_df, on="doi_key", validate="one_to_one")
            merged["doi_key"] = merged["crossref.doi"].combine_first(merged["doi_key"])

    if clients.openalex:
        openalex_doi_records: list[dict[str, Any]] = []
        for doi in doi_values:
            try:
                record = dict(clients.openalex.fetch_by_doi(doi))
                if record:
                    record.setdefault("doi", normalise_doi(doi))
                    openalex_doi_records.append(record)
            except ExtractionError as exc:
                bind_context(logger, stage="extract", source="openalex", doi=doi).error(
                    "openalex_fetch_failed", error=str(exc)
                )
                log_error_to_file("openalex", {"error": str(exc), "doi": doi})
        if openalex_doi_records:
            openalex_df = pd.DataFrame(openalex_doi_records)
            openalex_df.rename(
                columns={
                    "title": "openalex.title",
                    "publication_year": "openalex.publication_year",
                    "doi": "openalex.doi",
                    "pmid": "openalex.pmid",
                },
                inplace=True,
            )
            openalex_df["openalex.doi"] = openalex_df["openalex.doi"].map(normalise_doi)
            openalex_df["doi_key"] = openalex_df["openalex.doi"]
            merged = safe_left_join(merged, openalex_df, on="doi_key", validate="one_to_one")
            merged["doi_key"] = merged["openalex.doi"].combine_first(merged["doi_key"])

        missing_doi_pmids = [
            value
            for value in merged.loc[merged["doi_key"].isna(), "pmid"].dropna().unique()
            if value
        ]
        if missing_doi_pmids:
            pmid_records: list[dict[str, Any]] = []
            for pmid in missing_doi_pmids:
                try:
                    record = dict(clients.openalex.fetch_by_pmid(pmid))
                    if record:
                        record.setdefault("pmid", pmid)
                        pmid_records.append(record)
                except ExtractionError as exc:
                    bind_context(logger, stage="extract", source="openalex", pmid=pmid).error(
                        "openalex_fetch_failed", error=str(exc)
                    )
                    log_error_to_file("openalex", {"error": str(exc), "pmid": pmid})
            if pmid_records:
                openalex_pmid_df = pd.DataFrame(pmid_records)
                openalex_pmid_df.rename(
                    columns={
                        "title": "openalex.title",
                        "publication_year": "openalex.publication_year",
                        "doi": "openalex.doi",
                        "pmid": "pmid",
                    },
                    inplace=True,
                )
                openalex_pmid_df["openalex.doi"] = openalex_pmid_df["openalex.doi"].map(normalise_doi)
                merged = safe_left_join(merged, openalex_pmid_df, on="pmid", validate="one_to_one")
                merged["doi_key"] = merged["doi_key"].combine_first(merged["openalex.doi"])

    merged = normalize_publication_frame(merged)
    return merged


@app.command()
def extract(
    config: Path = typer.Option(  # noqa: B008
        ..., exists=True, readable=True, help="Path to the YAML configuration file."
    ),
    input: Path = typer.Option(..., exists=True, readable=True, help="CSV file with queries."),  # noqa: B008
) -> None:
    """Extract publication metadata without writing to disk."""

    _execute_pipeline(config, input, output=None)


@app.command()
def run(
    config: Path = typer.Option(  # noqa: B008
        ..., exists=True, readable=True, help="Path to the YAML configuration file."
    ),
    input: Path = typer.Option(..., exists=True, readable=True, help="CSV file with queries."),  # noqa: B008
    output: Path = typer.Option(..., writable=True, help="Destination CSV for the publications."),  # noqa: B008
) -> None:
    """Run the full ETL pipeline and persist the normalized output."""

    _execute_pipeline(config, input, output=output)


def _execute_pipeline(config_path: Path, input_path: Path, output: Path | None) -> pd.DataFrame:
    load_dotenv()
    config = load_config(config_path)
    run_id = configure_logging(config.logging.level)
    logger = get_logger("pipeline")

    queries = read_queries(input_path)
    try:
        validated_queries = INPUT_SCHEMA.validate(queries, lazy=config.etl.strict_validation)
    except Exception as exc:  # noqa: BLE001
        raise PipelineValidationError(f"Input validation failed: {exc}") from exc

    clients = build_clients(config)
    publications = extract_publications(validated_queries, clients)

    try:
        validated_publications = OUTPUT_SCHEMA.validate(
            publications,
            lazy=config.etl.strict_validation,
        )
    except Exception as exc:  # noqa: BLE001
        raise PipelineValidationError(f"Output validation failed: {exc}") from exc

    if output:
        target = output
        if target.is_dir():
            target = target / "publications.csv"
        write_publications(validated_publications, target)
        bind_context(logger, stage="load").info(
            "pipeline_completed",
            output=str(target),
            records=len(validated_publications),
            run_id=run_id,
        )

    return validated_publications


if __name__ == "__main__":
    app()
