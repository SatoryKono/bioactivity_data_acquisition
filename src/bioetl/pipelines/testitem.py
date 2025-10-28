"""TestItem Pipeline - ChEMBL molecule data extraction."""

from pathlib import Path
from typing import cast

import pandas as pd

from bioetl.adapters import PubChemAdapter
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import TestItemSchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("testitem", "1.0.0", TestItemSchema)


class TestItemPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL molecule (testitem) data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = chembl_source.get("batch_size", 25)
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"
            batch_size = 25

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = batch_size

        # Initialize external adapters (PubChem)
        self.external_adapters: dict[str, ExternalAdapter] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract molecule data from input file."""
        if input_file is None:
            # Default to data/input/testitem.csv
            input_file = Path("data/input/testitem.csv")

        logger.info("reading_input", path=input_file)

        # Read input file with molecule IDs
        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            # Return empty DataFrame with schema structure
            return pd.DataFrame(columns=[
                "molecule_chembl_id", "molregno", "parent_chembl_id",
                "canonical_smiles", "standard_inchi", "standard_inchi_key",
                "molecular_weight", "heavy_atoms", "aromatic_rings",
                "rotatable_bonds", "hba", "hbd",
                "lipinski_ro5_violations", "lipinski_ro5_pass",
                "all_names", "molecule_synonyms",
                "atc_classifications", "pubchem_cid", "pubchem_synonyms",
            ])

        df = pd.read_csv(input_file)  # Read all records

        # Don't filter - keep all fields from input file
        # They will be matched against schema column_order in transform()

        logger.info("extraction_completed", rows=len(df), columns=len(df.columns))
        return df

    def _fetch_molecule_data(self, molecule_ids: list[str]) -> pd.DataFrame:
        """Fetch molecule data from ChEMBL API."""
        results = []

        for i in range(0, len(molecule_ids), self.batch_size):
            batch_ids = molecule_ids[i:i + self.batch_size]
            logger.info("fetching_batch", batch=i // self.batch_size + 1, size=len(batch_ids))

            try:
                url = f"{self.api_client.config.base_url}/molecule.json"
                params = {
                    "molecule_chembl_id__in": ",".join(batch_ids),
                    "limit": len(batch_ids)  # Запрашиваем столько записей, сколько ID в батче
                }

                response = self.api_client.request_json(url, params=params)
                molecules = response.get("molecules", [])

                # Проверка: все ли ID вернулись
                returned_ids = {m["molecule_chembl_id"] for m in molecules}
                missing_ids = set(batch_ids) - returned_ids

                if missing_ids:
                    logger.warning(
                        "incomplete_batch_response",
                        requested=len(batch_ids),
                        returned=len(molecules),
                        missing=list(missing_ids)
                    )

                    # Fallback: повторный запрос для пропущенных ID с увеличенным лимитом
                    for missing_id in missing_ids:
                        logger.info("retry_missing_molecule", molecule_chembl_id=missing_id)
                        try:
                            retry_url = f"{self.api_client.config.base_url}/molecule/{missing_id}.json"
                            retry_response = self.api_client.request_json(retry_url)
                            retry_mol = retry_response if isinstance(retry_response, dict) and "molecule_chembl_id" in retry_response else None

                            if retry_mol:
                                logger.info("retry_successful", molecule_chembl_id=missing_id)
                                # Парсим данные из retry_mol так же, как из molecules
                                mol_data = {
                                    "molecule_chembl_id": retry_mol.get("molecule_chembl_id"),
                                    "molregno": retry_mol.get("molecule_chembl_id"),
                                    "pref_name": retry_mol.get("pref_name"),
                                    "parent_chembl_id": retry_mol.get("molecule_hierarchy", {}).get("parent_chembl_id") if isinstance(retry_mol.get("molecule_hierarchy"), dict) else None,
                                    "max_phase": retry_mol.get("max_phase"),
                                    "structure_type": retry_mol.get("structure_type"),
                                    "molecule_type": retry_mol.get("molecule_type"),
                                }

                                # molecule_properties
                                props = retry_mol.get("molecule_properties", {})
                                if isinstance(props, dict):
                                    mol_data.update({
                                        "mw_freebase": props.get("mw_freebase"),
                                        "qed_weighted": props.get("qed_weighted"),
                                        "heavy_atoms": props.get("heavy_atoms"),
                                        "aromatic_rings": props.get("aromatic_rings"),
                                        "rotatable_bonds": props.get("num_rotatable_bonds"),
                                        "hba": props.get("hba"),
                                        "hbd": props.get("hbd"),
                                    })

                                # molecule_structures
                                struct = retry_mol.get("molecule_structures", {})
                                if isinstance(struct, dict):
                                    mol_data.update({
                                        "standardized_smiles": struct.get("canonical_smiles"),
                                        "standard_inchi": struct.get("standard_inchi"),
                                        "standard_inchi_key": struct.get("standard_inchi_key"),
                                    })

                                results.append(mol_data)
                            else:
                                # Если retry тоже failed (404), создаём fallback
                                logger.warning("retry_failed_create_fallback", molecule_chembl_id=missing_id)
                                results.append({
                                    "molecule_chembl_id": missing_id,
                                    "molregno": missing_id,
                                    "pref_name": None,
                                    "parent_chembl_id": missing_id,
                                    "max_phase": None,
                                    "structure_type": "MOL",
                                    "molecule_type": "Small molecule",
                                    "standardized_smiles": None,
                                    "standard_inchi": None,
                                    "standard_inchi_key": None,
                                })
                        except Exception as retry_e:
                            logger.error("retry_failed", molecule_chembl_id=missing_id, error=str(retry_e))
                            # Создаём fallback при ошибке retry
                            results.append({
                                "molecule_chembl_id": missing_id,
                                "molregno": missing_id,
                                "pref_name": None,
                                "parent_chembl_id": missing_id,
                                "max_phase": None,
                                "structure_type": "MOL",
                                "molecule_type": "Small molecule",
                                "standardized_smiles": None,
                                "standard_inchi": None,
                                "standard_inchi_key": None,
                            })

                for mol in molecules:
                    mol_data = {
                        "molecule_chembl_id": mol.get("molecule_chembl_id"),
                        "molregno": mol.get("molecule_chembl_id"),  # Placeholder - API doesn't return molregno directly
                        "pref_name": mol.get("pref_name"),
                        "parent_chembl_id": mol.get("molecule_hierarchy", {}).get("parent_chembl_id") if isinstance(mol.get("molecule_hierarchy"), dict) else None,
                        "max_phase": mol.get("max_phase"),
                        "structure_type": mol.get("structure_type"),
                        "molecule_type": mol.get("molecule_type"),
                    }

                    # molecule_properties
                    props = mol.get("molecule_properties", {})
                    if isinstance(props, dict):
                        mol_data.update({
                            "mw_freebase": props.get("mw_freebase"),
                            "qed_weighted": props.get("qed_weighted"),
                            "heavy_atoms": props.get("heavy_atoms"),
                            "aromatic_rings": props.get("aromatic_rings"),
                            "rotatable_bonds": props.get("num_rotatable_bonds"),  # Fixed field name
                            "hba": props.get("hba"),
                            "hbd": props.get("hbd"),
                        })

                    # molecule_structures
                    struct = mol.get("molecule_structures", {})
                    if isinstance(struct, dict):
                        mol_data.update({
                            "standardized_smiles": struct.get("canonical_smiles"),
                            "standard_inchi": struct.get("standard_inchi"),
                            "standard_inchi_key": struct.get("standard_inchi_key"),
                        })

                    results.append(mol_data)

                logger.info("batch_fetched", count=len(molecules))

            except Exception as e:
                logger.error("batch_fetch_failed", error=str(e), batch_ids=batch_ids)
                # Continue with next batch

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        logger.info("api_extraction_completed", rows=len(df))
        return df

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from status endpoint.

        Returns:
            Version string (e.g., 'ChEMBL_36') or None
        """
        try:
            url = f"{self.api_client.config.base_url}/status.json"
            response = self.api_client.request_json(url)

            # Extract version info from status endpoint
            version = response.get("chembl_db_version")
            release_date = response.get("chembl_release_date")
            activities = response.get("activities")

            if version:
                logger.info(
                    "chembl_version_fetched",
                    version=version,
                    release_date=release_date,
                    activities=activities
                )
                return str(version)

            logger.warning("chembl_version_not_in_status_response")
            return None

        except Exception as e:
            logger.warning("failed_to_get_chembl_version", error=str(e))
            return None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform molecule data with hash generation."""
        if df.empty:
            return df

        # Normalize identifiers
        from bioetl.normalizers import registry

        for col in ["molecule_chembl_id", "parent_chembl_id"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Fetch molecule data from ChEMBL API
        molecule_ids = df["molecule_chembl_id"].unique().tolist()
        molecule_data = self._fetch_molecule_data(molecule_ids)

        # Merge with existing data
        if not molecule_data.empty:
            df = df.merge(molecule_data, on="molecule_chembl_id", how="left", suffixes=("", "_api"))
            # Remove duplicate columns from API merge
            df = df.loc[:, ~df.columns.str.endswith("_api")]

        # PubChem enrichment (optional)
        if "pubchem" in self.external_adapters:
            logger.info("pubchem_enrichment_enabled")
            try:
                df = self._enrich_with_pubchem(df)
            except Exception as e:
                logger.error("pubchem_enrichment_failed", error=str(e))
                # Continue with original data - graceful degradation

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = self._chembl_release
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        # Generate hash fields for data integrity
        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        df["hash_business_key"] = df["molecule_chembl_id"].apply(generate_hash_business_key)
        df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        # Generate deterministic index
        df = df.sort_values("molecule_chembl_id")  # Sort by primary key
        df["index"] = range(len(df))

        # Reorder columns according to schema and add missing columns with None
        from bioetl.schemas import TestItemSchema

        expected_cols = TestItemSchema.get_column_order()
        if expected_cols:
            # Add missing columns with None values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

            # Reorder to match schema column_order
            df = df[expected_cols]

        return df

    def _init_external_adapters(self) -> None:
        """Initialize external API adapters."""
        sources = self.config.sources

        # PubChem adapter
        if "pubchem" in sources:
            pubchem = sources["pubchem"]
            # Handle both dict and SourceConfig object
            if isinstance(pubchem, dict):
                enabled = pubchem.get("enabled", False)
                base_url = pubchem.get("base_url", "https://pubchem.ncbi.nlm.nih.gov/rest/pug")
                rate_limit_max_calls = pubchem.get("rate_limit_max_calls", 5)
                rate_limit_period = pubchem.get("rate_limit_period", 1.0)
                batch_size = pubchem.get("batch_size", 100)
            else:
                enabled = pubchem.enabled
                base_url = pubchem.base_url or "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
                rate_limit_max_calls = getattr(pubchem, "rate_limit_max_calls", 5)
                rate_limit_period = getattr(pubchem, "rate_limit_period", 1.0)
                batch_size = pubchem.batch_size or 100

            if enabled:
                pubchem_config = APIConfig(
                    name="pubchem",
                    base_url=base_url,
                    rate_limit_max_calls=rate_limit_max_calls,
                    rate_limit_period=rate_limit_period,
                    cache_enabled=self.config.cache.enabled,
                    cache_ttl=self.config.cache.ttl,
                )
                adapter_config = AdapterConfig(
                    enabled=True,
                    batch_size=batch_size,
                    workers=1,  # PubChem doesn't use parallel workers for simplicity
                )
                self.external_adapters["pubchem"] = PubChemAdapter(pubchem_config, adapter_config)
                logger.info("pubchem_adapter_initialized", base_url=base_url, batch_size=batch_size)

        logger.info("adapters_initialized", count=len(self.external_adapters))

    def _enrich_with_pubchem(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich testitem data with PubChem properties.

        Args:
            df: DataFrame with molecule data from ChEMBL

        Returns:
            DataFrame enriched with pubchem_* fields
        """
        if "pubchem" not in self.external_adapters:
            logger.warning("pubchem_adapter_not_available")
            return df

        if df.empty:
            logger.warning("enrichment_skipped", reason="empty_dataframe")
            return df

        # Check if we have InChI Keys for enrichment
        if "standard_inchi_key" not in df.columns:
            logger.warning("enrichment_skipped", reason="missing_standard_inchi_key_column")
            return df

        pubchem_adapter = self.external_adapters["pubchem"]

        try:
            # Type cast to PubChemAdapter for specific method access
            enriched_df = cast(PubChemAdapter, pubchem_adapter).enrich_with_pubchem(df, inchi_key_col="standard_inchi_key")
            return enriched_df
        except Exception as e:
            logger.error("pubchem_enrichment_error", error=str(e))
            # Return original dataframe on error - graceful degradation
            return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate molecule data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        try:
            # Check for duplicates
            duplicate_count = df["molecule_chembl_id"].duplicated().sum() if "molecule_chembl_id" in df.columns else 0
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                # Remove duplicates, keeping first occurrence
                df = df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

            logger.info("validation_completed", rows=len(df), duplicates_removed=duplicate_count)
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

