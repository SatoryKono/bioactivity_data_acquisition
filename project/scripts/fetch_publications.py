"""CLI entry point for the publications ETL pipeline."""

from __future__ import annotations

import sys
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

import pandas as pd
import typer
import yaml
from pydantic import BaseModel, Field, ValidationError

# Add the src directory to the Python path
src_dir = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(src_dir))

# Local imports
from bioactivity.clients import BaseApiClient as BasePublicationsClient  # type: ignore
from bioactivity.clients.chembl import ChEMBLClient as ChemblClient  # type: ignore
from bioactivity.clients.crossref import CrossrefClient  # type: ignore
from bioactivity.clients.openalex import OpenAlexClient  # type: ignore
from bioactivity.clients.pubmed import PubMedClient  # type: ignore
from bioactivity.clients.semantic_scholar import SemanticScholarClient  # type: ignore
from bioactivity.config import APIClientConfig as ClientConfig  # type: ignore
from bioactivity.utils.errors import ConfigError, ExtractionError  # type: ignore
from bioactivity.logging import configure_logging, get_logger  # type: ignore

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:
    def load_dotenv() -> None:
        """Placeholder for dotenv functionality."""
        pass


# Define missing classes and functions as placeholders
class PipelineValidationError(Exception):
    """Custom validation error for pipeline operations."""
    pass

def read_queries(input_path: Path) -> pd.DataFrame:
    """Read queries from CSV file."""
    return pd.read_csv(input_path)

def write_publications(publications: pd.DataFrame, output_path: Path) -> None:
    """Write publications to CSV file."""
    publications.to_csv(output_path, index=False)

# Placeholder schemas
class InputSchema:
    @staticmethod
    def validate(df: pd.DataFrame, lazy: bool = True) -> pd.DataFrame:
        return df

class OutputSchema:
    @staticmethod
    def validate(df: pd.DataFrame, lazy: bool = True) -> pd.DataFrame:
        return df

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
        return pd.DataFrame()  # Return empty DataFrame instead of empty_publications_frame()

    records: list[dict[str, Any]] = []
    for _, row in queries.iterrows():
        query = str(row["query"])
        for name, _ in clients.items():
            try:
                # TODO: Implement fetch_publications method for clients
                payload: list[dict[str, Any]] = []  # client.fetch_publications(query)
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
        return pd.DataFrame()  # Return empty DataFrame instead of empty_publications_frame()

    frame = pd.DataFrame.from_records(records)
    return frame  # Return frame directly instead of normalize_publication_frame(frame)


@app.command()  # type: ignore
def extract(
    config: Path,
    input: Path,
) -> None:
    """Extract publication metadata without writing to disk."""
    
    # Validate file existence
    if not config.exists() or not config.is_file():
        raise typer.BadParameter(f"Configuration file not found: {config}")
    if not input.exists() or not input.is_file():
        raise typer.BadParameter(f"Input file not found: {input}")

    _execute_pipeline(config, input, output=None)


@app.command()  # type: ignore
def run(
    config: Path,
    input: Path,
    output: Path,
) -> None:
    """Run the full ETL pipeline and persist the normalized output."""
    
    # Validate file existence
    if not config.exists() or not config.is_file():
        raise typer.BadParameter(f"Configuration file not found: {config}")
    if not input.exists() or not input.is_file():
        raise typer.BadParameter(f"Input file not found: {input}")
    
    # Ensure output directory exists
    output.parent.mkdir(parents=True, exist_ok=True)

    _execute_pipeline(config, input, output=output)


def _execute_pipeline(config_path: Path, input_path: Path, output: Path | None) -> pd.DataFrame:
    load_dotenv()
    config = load_config(config_path)
    configure_logging(config.logging.level)
    logger = get_logger("pipeline")

    queries = read_queries(input_path)
    try:
        validated_queries = InputSchema.validate(queries, lazy=config.etl.strict_validation)
    except Exception as exc:  # noqa: BLE001
        raise PipelineValidationError(f"Input validation failed: {exc}") from exc

    clients = build_clients(config)
    publications = extract_publications(validated_queries, clients)

    try:
        validated_publications = OutputSchema.validate(
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
