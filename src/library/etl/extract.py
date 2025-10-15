"""Extraction stage of the ETL pipeline."""

from __future__ import annotations

import pandas as pd
from structlog.stdlib import BoundLogger

from library.clients import BioactivityClient
from library.config import APIClientConfig
from library.schemas import RawBioactivitySchema


def fetch_bioactivity_data(
    client_config: APIClientConfig,
    logger: BoundLogger | None = None,
) -> pd.DataFrame:
    """Retrieve and validate bioactivity data for a single source."""

    schema = RawBioactivitySchema.to_schema()
    with BioactivityClient(client_config) as client:
        records = client.fetch_records()
    if not records:
        empty_df = pd.DataFrame(columns=list(schema.columns.keys()))
        validated = schema.validate(empty_df, lazy=True)
        if logger is not None:
            logger.info("extract_complete", source=client_config.name, rows=0)
        return validated
    frame = pd.DataFrame.from_records(records)
    frame["retrieved_at"] = pd.to_datetime(frame["retrieved_at"], utc=True)
    validated = schema.validate(frame, lazy=True)
    if logger is not None:
        logger.info("extract_complete", source=client_config.name, rows=len(validated))
    return validated


__all__ = ["fetch_bioactivity_data"]
