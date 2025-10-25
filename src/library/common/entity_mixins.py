"""Entity-specific mixins for different data types.

This module provides specialized mixins for activity, assay, document,
target, and testitem entities with their specific functionality.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from .mixins import ChEMBLOnlyMixin, MultiSourceMixin

logger = logging.getLogger(__name__)


class ActivityMixin(ChEMBLOnlyMixin):
    """Mixin for activity-specific functionality."""

    def _get_entity_type(self) -> str:
        return "activity"

    def _get_key_column(self) -> str:
        return "activity_chembl_id"

    def _get_chembl_fields(self) -> list[str]:
        return ["activity_type", "activity_value", "activity_unit", "pchembl_value", "data_validity_comment", "activity_comment", "bao_endpoint", "bao_format", "bao_label"]

    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract activity data from ChEMBL."""
        if data.empty:
            return pd.DataFrame()

        client = self.clients.get("chembl")
        if not client:
            logger.warning("ChEMBL client not available")
            return pd.DataFrame()

        # Extract activity IDs
        activity_ids = data["activity_chembl_id"].tolist()

        # Fetch activity data in batches
        activity_records = []
        config = getattr(self, "config", {})
        batch_size = config.get("sources", {}).get("chembl", {}).get("batch_size", 100)

        for i in range(0, len(activity_ids), batch_size):
            batch_ids = activity_ids[i : i + batch_size]
            try:
                batch_data = client.fetch_activities(batch_ids)
                if batch_data and "activities" in batch_data:
                    activity_records.extend(batch_data["activities"])
            except Exception as e:
                logger.error(f"Failed to fetch activity batch {i // batch_size + 1}: {e}")
                continue

        if activity_records:
            return pd.DataFrame(activity_records)
        else:
            logger.warning("No activity data extracted from ChEMBL")
            return pd.DataFrame()

    def _validate_activity_ids(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate and normalize activity_chembl_id format."""
        if "activity_chembl_id" not in data.columns:
            return data

        # Normalize ChEMBL ID format
        data["activity_chembl_id"] = data["activity_chembl_id"].astype(str).str.upper()

        # Validate format
        invalid_mask = ~data["activity_chembl_id"].str.match(r"^CHEMBL_ACT_\d+$", na=False)
        if invalid_mask.any():
            invalid_ids = data.loc[invalid_mask, "activity_chembl_id"].unique()
            logger.warning(f"Found {len(invalid_ids)} invalid activity ChEMBL IDs: {invalid_ids[:5]}")

        return data


class AssayMixin(ChEMBLOnlyMixin):
    """Mixin for assay-specific functionality."""

    def _get_entity_type(self) -> str:
        return "assay"

    def _get_key_column(self) -> str:
        return "assay_chembl_id"

    def _get_chembl_fields(self) -> list[str]:
        return [
            "assay_type",
            "assay_category",
            "assay_classification",
            "assay_organism",
            "assay_strain",
            "assay_tissue",
            "assay_cell_type",
            "assay_subcellular_fraction",
            "assay_description",
            "assay_chembl_id",
        ]

    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract assay data from ChEMBL."""
        if data.empty:
            return pd.DataFrame()

        client = self.clients.get("chembl")
        if not client:
            logger.warning("ChEMBL client not available")
            return pd.DataFrame()

        # Extract assay IDs
        assay_ids = data["assay_chembl_id"].tolist()

        # Fetch assay data in batches
        assay_records = []
        # config = getattr(self, 'config', {})
        # sources = getattr(config, 'sources', {})
        # chembl_config = sources.get("chembl", {})
        # batch_size = getattr(chembl_config, 'batch_size', 100)
        batch_size = 100  # Default batch size

        for i in range(0, len(assay_ids), batch_size):
            batch_ids = assay_ids[i : i + batch_size]
            try:
                batch_data = client.fetch_assays(batch_ids)
                if batch_data and "assays" in batch_data:
                    assay_records.extend(batch_data["assays"])
            except Exception as e:
                logger.error(f"Failed to fetch assay batch {i // batch_size + 1}: {e}")
                continue

        if assay_records:
            return pd.DataFrame(assay_records)
        else:
            logger.warning("No assay data extracted from ChEMBL")
            return pd.DataFrame()

    def _enrich_with_target_data(self, chembl_client, target_ids: list[str]) -> pd.DataFrame:
        """Enrich assay data with target information."""
        target_records = []
        for target_id in target_ids:
            try:
                target_data = chembl_client.fetch_target(target_id)
                if target_data and "error" not in target_data:
                    target_records.append(target_data)
            except Exception as e:
                logger.warning("Failed to fetch target %s: %s", target_id, e)
                continue

        if target_records:
            return pd.DataFrame(target_records)
        else:
            logger.warning("No target data extracted for enrichment")
            return pd.DataFrame()

    def _expand_assay_parameters(self, assay_data: dict[str, Any]) -> dict[str, Any]:
        """Expand first assay_parameter to flat fields with assay_param_ prefix."""
        params = assay_data.get("assay_parameters")
        if params and isinstance(params, list) and len(params) > 0:
            first_param = params[0]
            assay_data["assay_param_type"] = first_param.get("type")
            assay_data["assay_param_relation"] = first_param.get("relation")
            assay_data["assay_param_value"] = first_param.get("value")
            assay_data["assay_param_units"] = first_param.get("units")
            assay_data["assay_param_text_value"] = first_param.get("text_value")
            assay_data["assay_param_standard_type"] = first_param.get("standard_type")
            assay_data["assay_param_standard_value"] = first_param.get("standard_value")
            assay_data["assay_param_standard_units"] = first_param.get("standard_units")
        else:
            # Set NULL for all fields
            for field in ["type", "relation", "value", "units", "text_value", "standard_type", "standard_value", "standard_units"]:
                assay_data[f"assay_param_{field}"] = None
        return assay_data


class DocumentMixin(MultiSourceMixin):
    """Mixin for document-specific functionality."""

    def _get_entity_type(self) -> str:
        return "document"

    def _get_key_column(self) -> str:
        return "document_chembl_id"

    def _get_chembl_fields(self) -> list[str]:
        return ["title", "abstract", "authors", "journal", "year", "volume", "issue", "pages", "doi", "pmid"]

    def _get_pubchem_fields(self) -> list[str]:
        return ["pubchem_compound_id", "pubchem_substance_id"]

    def _setup_document_clients(self) -> None:
        """Setup document-specific clients."""
        from library.clients.crossref import CrossrefClient
        from library.clients.openalex import OpenAlexClient
        from library.clients.pubmed import PubMedClient
        from library.clients.semantic_scholar import SemanticScholarClient

        # Crossref client
        if self._is_source_enabled("crossref"):
            from library.settings import APIClientConfig
            config = APIClientConfig(name="crossref", base_url="https://api.crossref.org")
            self.clients["crossref"] = CrossrefClient(config=config)

        # OpenAlex client
        if self._is_source_enabled("openalex"):
            from library.settings import APIClientConfig
            config = APIClientConfig(name="openalex", base_url="https://api.openalex.org")
            self.clients["openalex"] = OpenAlexClient(config=config)

        # PubMed client
        if self._is_source_enabled("pubmed"):
            from library.settings import APIClientConfig
            config = APIClientConfig(name="pubmed", base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils")
            self.clients["pubmed"] = PubMedClient(config=config)

        # Semantic Scholar client
        if self._is_source_enabled("semantic_scholar"):
            from library.settings import APIClientConfig
            config = APIClientConfig(name="semantic_scholar", base_url="https://api.semanticscholar.org/graph/v1")
            self.clients["semantic_scholar"] = SemanticScholarClient(config=config)

    def _extract_from_source(self, client, source_name: str, data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from a specific source."""
        if source_name == "chembl":
            return self._extract_from_chembl(data)
        elif source_name == "crossref":
            return self._extract_from_crossref(data)
        elif source_name == "openalex":
            return self._extract_from_openalex(data)
        elif source_name == "pubmed":
            return self._extract_from_pubmed(data)
        elif source_name == "semantic_scholar":
            return self._extract_from_semantic_scholar(data)
        else:
            return pd.DataFrame()

    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from ChEMBL."""
        if data.empty:
            return pd.DataFrame()

        client = self.clients.get("chembl")
        if not client:
            logger.warning("ChEMBL client not available")
            return pd.DataFrame()

        # Extract document IDs
        document_ids = data["document_chembl_id"].tolist()

        # Fetch document data in batches
        document_records = []
        # config = getattr(self, 'config', {})
        # sources = getattr(config, 'sources', {})
        # chembl_config = sources.get("chembl", {})
        # batch_size = getattr(chembl_config, 'batch_size', 100)
        batch_size = 100  # Default batch size

        for i in range(0, len(document_ids), batch_size):
            batch_ids = document_ids[i : i + batch_size]
            try:
                batch_data = client.fetch_documents(batch_ids)
                if batch_data and "documents" in batch_data:
                    document_records.extend(batch_data["documents"])
            except Exception as e:
                logger.error(f"Failed to fetch document batch {i // batch_size + 1}: {e}")
                continue

        if document_records:
            return pd.DataFrame(document_records)
        else:
            logger.warning("No document data extracted from ChEMBL")
            return pd.DataFrame()

    def _extract_from_crossref(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from Crossref."""
        # Implementation for Crossref extraction
        return pd.DataFrame()

    def _extract_from_openalex(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from OpenAlex."""
        # Implementation for OpenAlex extraction
        return pd.DataFrame()

    def _extract_from_pubmed(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from PubMed."""
        # Implementation for PubMed extraction
        return pd.DataFrame()

    def _extract_from_semantic_scholar(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract document data from Semantic Scholar."""
        # Implementation for Semantic Scholar extraction
        return pd.DataFrame()

    def _merge_source_data(self, base_data: pd.DataFrame, source_data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Merge document data from a specific source."""
        if source_name == "chembl":
            return self._merge_chembl_data(base_data, source_data)
        else:
            # For other sources, use basic merge
            key_column = self._get_key_column()
            if source_data.empty:
                return base_data

            merged = base_data.merge(source_data, on=key_column, how="left", suffixes=("", f"_{source_name}"))

            # Merge specific fields
            source_fields = self._get_source_fields(source_name)
            for field in source_fields:
                source_field = f"{field}_{source_name}"
                if source_field in merged.columns:
                    before_count = merged[field].notna().sum() if field in merged.columns else 0
                    merged[field] = merged[source_field].fillna(merged[field])
                    after_count = merged[field].notna().sum()
                    merged = merged.drop(columns=[source_field])

                    logger.debug(f"Field {field}: {before_count} -> {after_count} non-null values after merge")

            return merged

    def _get_source_fields(self, source_name: str) -> list[str]:
        """Get fields to merge from a specific source."""
        if source_name == "crossref":
            return ["title", "abstract", "authors", "journal", "year", "doi"]
        elif source_name == "openalex":
            return ["title", "abstract", "authors", "journal", "year", "doi"]
        elif source_name == "pubmed":
            return ["title", "abstract", "authors", "journal", "year", "pmid"]
        elif source_name == "semantic_scholar":
            return ["title", "abstract", "authors", "journal", "year", "doi"]
        else:
            return []


class TargetMixin(ChEMBLOnlyMixin):
    """Mixin for target-specific functionality."""

    def _get_entity_type(self) -> str:
        return "target"

    def _get_key_column(self) -> str:
        return "target_chembl_id"

    def _get_chembl_fields(self) -> list[str]:
        return ["target_type", "target_organism", "target_chembl_id", "pref_name", "target_components", "target_relations"]

    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract target data from ChEMBL."""
        if data.empty:
            return pd.DataFrame()

        client = self.clients.get("chembl")
        if not client:
            logger.warning("ChEMBL client not available")
            return pd.DataFrame()

        # Extract target IDs
        target_ids = data["target_chembl_id"].tolist()

        # Fetch target data in batches
        target_records = []
        # config = getattr(self, 'config', {})
        # sources = getattr(config, 'sources', {})
        # chembl_config = sources.get("chembl", {})
        # batch_size = getattr(chembl_config, 'batch_size', 100)
        batch_size = 100  # Default batch size

        for i in range(0, len(target_ids), batch_size):
            batch_ids = target_ids[i : i + batch_size]
            try:
                batch_data = client.fetch_targets(batch_ids)
                if batch_data and "targets" in batch_data:
                    target_records.extend(batch_data["targets"])
            except Exception as e:
                logger.error(f"Failed to fetch target batch {i // batch_size + 1}: {e}")
                continue

        if target_records:
            return pd.DataFrame(target_records)
        else:
            logger.warning("No target data extracted from ChEMBL")
            return pd.DataFrame()


class TestitemMixin(MultiSourceMixin):
    """Mixin for testitem-specific functionality."""

    def _get_entity_type(self) -> str:
        return "testitem"

    def _get_key_column(self) -> str:
        return "molecule_chembl_id"

    def _get_chembl_fields(self) -> list[str]:
        return ["molecule_chembl_id", "pref_name", "molecule_type", "molecule_properties", "molecule_synonyms"]

    def _get_pubchem_fields(self) -> list[str]:
        return ["pubchem_compound_id", "pubchem_substance_id", "molecular_formula", "molecular_weight"]

    def _extract_from_source(self, client, source_name: str, data: pd.DataFrame) -> pd.DataFrame:
        """Extract testitem data from a specific source."""
        if source_name == "chembl":
            return self._extract_from_chembl(data)
        elif source_name == "pubchem":
            return self._extract_from_pubchem(data)
        else:
            return pd.DataFrame()

    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract testitem data from ChEMBL."""
        if data.empty:
            return pd.DataFrame()

        client = self.clients.get("chembl")
        if not client:
            logger.warning("ChEMBL client not available")
            return pd.DataFrame()

        # Extract molecule IDs
        molecule_ids = data["molecule_chembl_id"].tolist()

        # Fetch molecule data in batches
        molecule_records = []
        # config = getattr(self, 'config', {})
        # sources = getattr(config, 'sources', {})
        # chembl_config = sources.get("chembl", {})
        # batch_size = getattr(chembl_config, 'batch_size', 100)
        batch_size = 100  # Default batch size

        for i in range(0, len(molecule_ids), batch_size):
            batch_ids = molecule_ids[i : i + batch_size]
            try:
                batch_data = client.fetch_molecules(batch_ids)
                if batch_data and "molecules" in batch_data:
                    molecule_records.extend(batch_data["molecules"])
            except Exception as e:
                logger.error(f"Failed to fetch molecule batch {i // batch_size + 1}: {e}")
                continue

        if molecule_records:
            return pd.DataFrame(molecule_records)
        else:
            logger.warning("No molecule data extracted from ChEMBL")
            return pd.DataFrame()

    def _extract_from_pubchem(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract testitem data from PubChem."""
        if data.empty:
            return pd.DataFrame()

        client = self.clients.get("pubchem")
        if not client:
            logger.warning("PubChem client not available")
            return pd.DataFrame()

        # Extract molecule IDs (assuming we have PubChem IDs)
        if "pubchem_compound_id" in data.columns:
            pubchem_ids = data["pubchem_compound_id"].dropna().tolist()
        else:
            # Try to get PubChem IDs from ChEMBL data
            pubchem_ids = []

        if not pubchem_ids:
            logger.warning("No PubChem IDs available for extraction")
            return pd.DataFrame()

        # Fetch PubChem data in batches
        pubchem_records = []
        # config = getattr(self, 'config', {})
        # sources = getattr(config, 'sources', {})
        # pubchem_config = sources.get("pubchem", {})
        # batch_size = getattr(pubchem_config, 'batch_size', 100)
        batch_size = 100  # Default batch size

        for i in range(0, len(pubchem_ids), batch_size):
            batch_ids = pubchem_ids[i : i + batch_size]
            try:
                batch_data = client.fetch_compounds(batch_ids)
                if batch_data and "compounds" in batch_data:
                    pubchem_records.extend(batch_data["compounds"])
            except Exception as e:
                logger.error(f"Failed to fetch PubChem batch {i // batch_size + 1}: {e}")
                continue

        if pubchem_records:
            return pd.DataFrame(pubchem_records)
        else:
            logger.warning("No PubChem data extracted")
            return pd.DataFrame()

    def _merge_source_data(self, base_data: pd.DataFrame, source_data: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Merge testitem data from a specific source."""
        if source_name == "chembl":
            return self._merge_chembl_data(base_data, source_data)
        elif source_name == "pubchem":
            return self._merge_pubchem_data(base_data, source_data)
        else:
            return base_data

    def _is_valid_value(self, value) -> bool:
        """Check if a value is valid (not null, empty, or invalid)."""
        import numpy as np

        # Check for None
        if value is None:
            return False

        # Check for numpy NaN
        if isinstance(value, (np.floating, np.integer)) and np.isnan(value):
            return False

        # Check for pandas NA (but not for numpy arrays)
        try:
            # Skip pd.isna for numpy arrays as they're already handled above
            if not isinstance(value, np.ndarray) and pd.isna(value):
                return False
        except (ValueError, TypeError):
            # If pd.isna fails, continue with other checks
            pass

        # Check for empty strings
        if isinstance(value, str) and value.strip() == "":
            return False

        # Check for empty lists
        if isinstance(value, list) and len(value) == 0:
            return False

        return True
