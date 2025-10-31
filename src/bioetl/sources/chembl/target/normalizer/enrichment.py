from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

from bioetl.sources.iuphar.pagination import PageNumberPaginator
from bioetl.sources.iuphar.parser import parse_api_response
from bioetl.sources.iuphar.service import IupharService
from bioetl.sources.uniprot.client.idmapping_client import UniProtIdMappingClient
from bioetl.sources.uniprot.client.orthologs_client import UniProtOrthologsClient
# Import UniProtSearchClient from parent module client.py (not from client package)
# The client.py version has the correct API (client, fields, batch_size, fetch_entries)
try:
    import sys
    import importlib.util
    from pathlib import Path
    
    # Get the path to client.py
    # enrichment.py is in normalizer/ subdirectory, so we need one more parent
    uniprot_dir = Path(__file__).parent.parent.parent.parent / "uniprot"
    client_py_file = uniprot_dir / "client.py"
    
    if client_py_file.exists():
        module_key = str(client_py_file)
        if module_key in sys.modules:
            client_module = sys.modules[module_key]
        else:
            spec = importlib.util.spec_from_file_location("bioetl.sources.uniprot.client", client_py_file)
            if spec and spec.loader:
                client_module = importlib.util.module_from_spec(spec)
                client_module.__name__ = "bioetl.sources.uniprot.client"
                client_module.__package__ = "bioetl.sources.uniprot"
                client_module.__file__ = str(client_py_file)
                sys.modules["bioetl.sources.uniprot.client"] = client_module
                spec.loader.exec_module(client_module)
            else:
                raise ImportError("Could not create module spec")
        UniProtSearchClient = client_module.UniProtSearchClient
    else:
        raise FileNotFoundError(f"client.py not found at {client_py_file}")
except Exception:
    # Fallback to package import
    from bioetl.sources.uniprot.client import UniProtSearchClient
from bioetl.sources.uniprot.normalizer import UniProtNormalizer

__all__ = [
    "MissingMappingRecorder",
    "TargetEnricher",
]


@dataclass(slots=True)
class MissingMappingRecorder:
    """Collect missing mapping records for QC artifacts."""

    records: list[dict[str, Any]] = field(default_factory=list)

    def reset(self) -> None:
        self.records.clear()

    def record(
        self,
        *,
        stage: str,
        target_id: Any | None,
        accession: Any | None,
        resolution: str,
        status: str,
        resolved_accession: Any | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        record: dict[str, Any] = {
            "stage": stage,
            "target_chembl_id": target_id,
            "input_accession": accession,
            "resolved_accession": resolved_accession,
            "resolution": resolution,
            "status": status,
        }
        if details:
            try:
                record["details"] = json.dumps(details, ensure_ascii=False, sort_keys=True)
            except TypeError:
                record["details"] = str(details)
        self.records.append(record)


@dataclass(slots=True)
class TargetEnricher:
    """Coordinate external enrichment services for the target pipeline."""

    uniprot_search_client: UniProtSearchClient | None
    uniprot_id_mapping_client: UniProtIdMappingClient | None
    uniprot_ortholog_client: UniProtOrthologsClient | None
    iuphar_service: IupharService
    iuphar_paginator: PageNumberPaginator | None = None

    def __post_init__(self) -> None:
        self.uniprot_normalizer = UniProtNormalizer(
            search_client=self.uniprot_search_client,
            id_mapping_client=self.uniprot_id_mapping_client,
            ortholog_client=self.uniprot_ortholog_client,
        )

    def enrich_uniprot(
        self,
        df: pd.DataFrame,
        *,
        record_missing_mapping: Callable[..., None],
        record_validation_issue: Callable[[dict[str, Any]], None],
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich the dataframe using UniProt datasets."""

        result = self.uniprot_normalizer.enrich_targets(
            df,
            accession_column="uniprot_accession",
            target_id_column="target_chembl_id",
            gene_symbol_column="gene_symbol",
            organism_column="organism",
            taxonomy_column="taxonomy_id",
        )

        for record in result.missing_mappings:
            record_missing_mapping(**record)

        for issue in result.validation_issues:
            record_validation_issue(issue)

        return result.dataframe, result.silver, result.components, result.metrics

    def enrich_iuphar(
        self,
        df: pd.DataFrame,
        *,
        request_builder,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        """Enrich dataframe using IUPHAR targets and families."""

        if self.iuphar_paginator is None:
            result = self.iuphar_service.enrich_targets(df, targets=[], families=[])
            return (*result, [], [])

        targets = self.fetch_collection(
            "/targets",
            unique_key="targetId",
            params=request_builder.targets(),
        )
        families = self.fetch_collection(
            "/targets/families",
            unique_key="familyId",
            params=request_builder.families(),
        )

        result = self.iuphar_service.enrich_targets(
            df,
            targets=targets,
            families=families,
        )
        return (*result, targets, families)

    def fetch_collection(
        self,
        path: str,
        *,
        unique_key: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if self.iuphar_paginator is None:
            return []

        parser = lambda payload: parse_api_response(payload, unique_key=unique_key)
        return self.iuphar_paginator.fetch_all(
            path,
            unique_key=unique_key,
            params=params,
            parser=parser,
        )

    def build_family_hierarchy(self, families: dict[int, dict[str, Any]]) -> dict[int, dict[str, list[Any]]]:
        return self.iuphar_service.build_family_hierarchy(families)

    def normalize_iuphar_name(self, value: str | None) -> str:
        return self.iuphar_service.normalize_name(value)

    def candidate_names_from_row(self, row: pd.Series) -> list[str]:
        return self.iuphar_service.candidate_names_from_row(row)

    def fallback_classification_record(self, row: pd.Series) -> dict[str, Any]:
        return self.iuphar_service.fallback_classification_record(row)

    def select_best_classification(self, records: list[dict[str, Any]]) -> dict[str, Any] | None:
        result = self.iuphar_service.select_best_classification(records)
        return dict(result) if isinstance(result, dict) else result

