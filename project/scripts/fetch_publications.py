"""CLI entry point for the publications ETL pipeline."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

import pandas as pd
import typer
import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

import sys
from pathlib import Path

# Add the project directory to the Python path
project_dir = Path(__file__).parent.parent
sys.path.insert(0, str(project_dir))

from library.clients import BasePublicationsClient, ClientConfig
from library.clients.chembl import ChemblClient
from library.clients.crossref import CrossrefClient
from library.clients.openalex import OpenAlexClient
from library.clients.pubmed import PubMedClient
from library.clients.semscholar import SemanticScholarClient
from library.io.normalize import normalize_publication_frame
from library.io.read_write import empty_publications_frame, read_queries, write_publications
from library.utils.errors import ConfigError, ExtractionError
from library.utils.errors import ValidationError as PipelineValidationError
from library.utils.logging import configure_logging, get_logger
from library.validation.input_schema import INPUT_SCHEMA
from library.validation.output_schema import OUTPUT_SCHEMA

app = typer.Typer(help="Fetch and normalize publication metadata from multiple public APIs.")

CLIENT_FACTORIES: Mapping[str, type[BasePublicationsClient]] = {
    "chembl": ChemblClient,
    "pubmed": PubMedClient,
    "semscholar": SemanticScholarClient,
    "crossref": CrossrefClient,
    "openalex": OpenAlexClient,
}


class LoggingConfig(BaseModel):
    level: str = Field(default="INFO")


class EtlConfig(BaseModel):
    strict_validation: bool = Field(default=True, alias="strict_validation")
    output_dir: Path | None = Field(default=None, alias="output_dir")

    class Config:
        allow_population_by_field_name = True


class SourceConfig(BaseModel):
    base_url: str
    api_key: str | None = None
    rate_limit_per_minute: int | None = Field(default=None, alias="rate_limit_per_minute")
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
    except ValidationError as exc:
        raise ConfigError(f"Invalid configuration: {exc}") from exc

def build_clients(config: PipelineConfig) -> dict[str, BasePublicationsClient]:
    """Instantiate clients defined in the configuration."""

    clients: dict[str, BasePublicationsClient] = {}
    for name, source_config in config.sources.items():
        factory = CLIENT_FACTORIES.get(name)
        if not factory:
            continue
        client_config = ClientConfig(
            name=name,
            base_url=source_config.base_url,
            api_key=source_config.api_key,
            rate_limit_per_minute=source_config.rate_limit_per_minute,
            extra_headers=source_config.headers,
        )
        clients[name] = factory(client_config)
    return clients


def extract_publications(
    queries: pd.DataFrame,
    clients: Mapping[str, BasePublicationsClient],
) -> pd.DataFrame:
    """Fetch publications for each query using the configured clients."""

    logger = get_logger("extract")
    if queries.empty:
        return empty_publications_frame()

    records: list[dict[str, Any]] = []
    for _, row in queries.iterrows():
        query = str(row["query"])
        for name, client in clients.items():
            try:
                payload = client.fetch_publications(query)
            except ExtractionError as exc:
                logger.warning("fetch_failed", source=name, query=query, error=str(exc))
                continue
            for record in payload:
                enriched: MutableMapping[str, Any] = dict(record)
                enriched.setdefault("source", name)
                enriched.setdefault("identifier", query)
                enriched.setdefault("title", f"Publication for {query}")
                enriched.setdefault("published_at", pd.NaT)
                enriched.setdefault("doi", None)
                records.append(dict(enriched))

    if not records:
        return empty_publications_frame()

    frame = pd.DataFrame.from_records(records)
    return normalize_publication_frame(frame)


@app.command()
def extract(
    config: Path = typer.Option(..., exists=True, readable=True, help="Path to the YAML configuration file."),
    input: Path = typer.Option(..., exists=True, readable=True, help="CSV file with queries."),
) -> None:
    """Extract publication metadata without writing to disk."""

    _execute_pipeline(config, input, output=None)


@app.command()
def run(
    config: Path = typer.Option(..., exists=True, readable=True, help="Path to the YAML configuration file."),
    input: Path = typer.Option(..., exists=True, readable=True, help="CSV file with queries."),
    output: Path = typer.Option(..., writable=True, help="Destination CSV for the publications."),
) -> None:
    """Run the full ETL pipeline and persist the normalized output."""

    _execute_pipeline(config, input, output=output)


def _execute_pipeline(config_path: Path, input_path: Path, output: Path | None) -> pd.DataFrame:
    load_dotenv()
    config = load_config(config_path)
    configure_logging(config.logging.level)
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
        logger.info("pipeline_completed", output=str(target), records=len(validated_publications))

    return pd.DataFrame(validated_publications)


if __name__ == "__main__":
    app()
