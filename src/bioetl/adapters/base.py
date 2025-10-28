"""Base class for external API adapters."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


@dataclass
class AdapterConfig:
    """Configuration for external adapter."""

    enabled: bool = True
    batch_size: int = 50
    workers: int = 1
    tool: str = "bioactivity_etl"
    email: str = ""
    api_key: str = ""
    mailto: str = ""


class ExternalAdapter(ABC):
    """Base class for external API adapters."""

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize adapter with API client."""
        self.api_config = api_config
        self.adapter_config = adapter_config
        self.api_client = UnifiedAPIClient(api_config)
        self.logger = UnifiedLogger.get(self.__class__.__name__)

    @abstractmethod
    def fetch_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch records by identifiers (DOI, PMID, etc)."""

    @abstractmethod
    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize a single record from the external source."""

    def to_dataframe(self, records: list[dict[str, Any]]) -> pd.DataFrame:
        """Convert list of normalized records to DataFrame."""
        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        return df

    def process(self, ids: list[str]) -> pd.DataFrame:
        """Process list of IDs: fetch, normalize, convert to DataFrame."""
        if not ids:
            self.logger.info("no_ids_provided", adapter=self.__class__.__name__)
            return pd.DataFrame()

        self.logger.info("starting_fetch", adapter=self.__class__.__name__, count=len(ids))

        # Fetch records
        raw_records = self.fetch_by_ids(ids)

        if not raw_records:
            self.logger.warning("no_records_fetched", adapter=self.__class__.__name__)
            return pd.DataFrame()

        # Normalize each record
        normalized_records = []
        for raw_record in raw_records:
            try:
                normalized = self.normalize_record(raw_record)
                if normalized:
                    normalized_records.append(normalized)
            except Exception as e:
                self.logger.error(
                    "normalization_failed",
                    adapter=self.__class__.__name__,
                    error=str(e),
                    record_id=raw_record.get("id", "unknown"),
                )

        self.logger.info(
            "fetch_completed",
            adapter=self.__class__.__name__,
            fetched=len(raw_records),
            normalized=len(normalized_records),
        )

        # Convert to DataFrame
        return self.to_dataframe(normalized_records)

    def close(self) -> None:
        """Close adapter and cleanup resources."""
        self.api_client.close()

