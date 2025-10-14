"""Extraction stage for the ETL pipeline."""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from .clients.chembl import ChEMBLClient
from .config import SourceConfig
from .validation.input_schema import INPUT_SCHEMA, validate_input


def extract_source(
    client: ChEMBLClient,
    config: SourceConfig,
    *,
    strict: bool = True,
) -> pd.DataFrame:
    """Extract data for a single source."""
    records = list(
        client.fetch_activities(
            config.activities_endpoint,
            page_size=config.page_size,
        )
    )
    frame = pd.DataFrame.from_records(records)
    if frame.empty:
        return frame
    return validate_input(frame) if strict else frame


def extract_all(sources: Iterable[SourceConfig], *, strict: bool = True) -> pd.DataFrame:
    """Extract and concatenate data from multiple sources."""
    frames: list[pd.DataFrame] = []
    for source in sources:
        client = ChEMBLClient(source.base_url).with_token(source.auth_token)
        try:
            frame = extract_source(client, source, strict=strict)
        finally:
            client.close()
        if not frame.empty:
            frame = frame.assign(source=source.name)
            frames.append(frame)
    if not frames:
        schema_columns = list(INPUT_SCHEMA.columns.keys())
        schema_columns.append("source")
        return pd.DataFrame(columns=schema_columns)
    return pd.concat(frames, ignore_index=True)
