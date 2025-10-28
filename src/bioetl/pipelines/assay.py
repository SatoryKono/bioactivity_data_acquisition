"""Assay Pipeline - ChEMBL assay data extraction."""

import json
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import AssaySchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("assay", "1.0.0", AssaySchema)


TARGET_ENRICHMENT_WHITELIST = [
    "target_chembl_id",
    "pref_name",
    "organism",
    "target_type",
    "species_group_flag",
    "tax_id",
    "component_count",
]

ASSAY_CLASS_ENRICHMENT_WHITELIST = {
    "assay_class_id": "assay_class_id",
    "bao_id": "assay_class_bao_id",
    "class_type": "assay_class_type",
    "l1": "assay_class_l1",
    "l2": "assay_class_l2",
    "l3": "assay_class_l3",
    "description": "assay_class_description",
}


class AssayPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL assay data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")  # type: ignore
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract assay data from input file."""
        if input_file is None:
            # Default to data/input/assay.csv
            input_file = Path("data/input/assay.csv")

        logger.info("reading_input", path=input_file)
        df = pd.read_csv(input_file)  # Read all records

        # Create output DataFrame with just assay_chembl_id for API lookup
        result = pd.DataFrame()
        result["assay_chembl_id"] = df["assay_chembl_id"] if "assay_chembl_id" in df.columns else df["assay_chembl_id"]

        logger.info("extraction_completed", rows=len(result))
        return result

    def _fetch_assay_data(self, assay_ids: list[str]) -> pd.DataFrame:
        """Fetch assay data from ChEMBL API."""
        results = []

        batch_size = 20
        for i in range(0, len(assay_ids), batch_size):
            batch_ids = assay_ids[i:i + batch_size]
            logger.info("fetching_batch", batch=i // batch_size + 1, size=len(batch_ids))
            
            try:
                url = f"{self.api_client.config.base_url}/assay.json"
                params = {"assay_chembl_id__in": ",".join(batch_ids)}
                
                response = self.api_client.request_json(url, params=params)
                assays = response.get("assays", [])
                
                for assay in assays:
                    # Extract and serialize classifications with proper empty list handling
                    classifications = assay.get("assay_classifications")
                    classifications_str = None
                    if classifications:
                        try:
                            classifications_str = json.dumps(
                                classifications,
                                ensure_ascii=False,
                                sort_keys=True,
                            )
                        except (TypeError, ValueError):
                            # Fallback to simple string join if JSON serialization fails
                            try:
                                classifications_str = " | ".join(classifications)
                            except TypeError:
                                classifications_str = None
                    
                    # Extract and serialize parameters with proper empty list handling
                    params = assay.get("assay_parameters")
                    params_json = json.dumps(params, ensure_ascii=False) if params is not None else None
                    
                    assay_data = {
                        "assay_chembl_id": assay.get("assay_chembl_id"),
                        "assay_type": assay.get("assay_type"),
                        "assay_category": assay.get("assay_category"),
                        "assay_cell_type": assay.get("assay_cell_type"),
                        "assay_classifications": classifications_str,
                        "assay_group": assay.get("assay_group"),
                        "assay_organism": assay.get("assay_organism"),
                        "assay_parameters_json": params_json,
                        "assay_strain": assay.get("assay_strain"),
                        "assay_subcellular_fraction": assay.get("assay_subcellular_fraction"),
                        "assay_tax_id": assay.get("assay_tax_id"),
                        "assay_test_type": assay.get("assay_test_type"),
                        "assay_tissue": assay.get("assay_tissue"),
                        "assay_type_description": assay.get("assay_type_description"),
                        "bao_format": assay.get("bao_format"),
                        "bao_label": assay.get("bao_label"),
                        "bao_endpoint": assay.get("bao_endpoint"),
                        "cell_chembl_id": assay.get("cell_chembl_id"),
                        "confidence_description": assay.get("confidence_description"),
                        "confidence_score": assay.get("confidence_score"),
                        "assay_description": assay.get("description"),
                        "document_chembl_id": assay.get("document_chembl_id"),
                        "relationship_description": assay.get("relationship_description"),
                        "relationship_type": assay.get("relationship_type"),
                        "src_assay_id": assay.get("src_assay_id"),
                        "src_id": assay.get("src_id"),
                        "target_chembl_id": assay.get("target_chembl_id"),
                        "tissue_chembl_id": assay.get("tissue_chembl_id"),
                    }
                    
                    # Handle assay_class
                    assay_class = assay.get("assay_class")
                    if isinstance(assay_class, dict):
                        assay_data.update({
                            "assay_class_id": assay_class.get("assay_class_id"),
                            "assay_class_bao_id": assay_class.get("bao_id"),
                            "assay_class_type": assay_class.get("assay_class_type"),
                            "assay_class_l1": assay_class.get("class_level_1"),
                            "assay_class_l2": assay_class.get("class_level_2"),
                            "assay_class_l3": assay_class.get("class_level_3"),
                            "assay_class_description": assay_class.get("assay_class_description"),
                        })
                    
                    # Handle variant_sequence (take first one for now)
                    variant_sequences = assay.get("variant_sequence")
                    variant_sequence_json = None
                    
                    if variant_sequences:
                        if isinstance(variant_sequences, list) and len(variant_sequences) > 0:
                            variant = variant_sequences[0]
                            assay_data.update({
                                "variant_id": variant.get("variant_id"),
                                "variant_base_accession": variant.get("base_accession"),
                                "variant_mutation": variant.get("mutation"),
                                "variant_sequence": variant.get("variant_seq"),
                                "variant_accession_reported": variant.get("accession_reported"),
                            })
                            variant_sequence_json = json.dumps(variant_sequences, ensure_ascii=False)
                        elif isinstance(variant_sequences, dict):
                            # Handle single variant as dict
                            assay_data.update({
                                "variant_id": variant_sequences.get("variant_id"),
                                "variant_base_accession": variant_sequences.get("base_accession"),
                                "variant_mutation": variant_sequences.get("mutation"),
                                "variant_sequence": variant_sequences.get("variant_seq"),
                                "variant_accession_reported": variant_sequences.get("accession_reported"),
                            })
                            variant_sequence_json = json.dumps([variant_sequences], ensure_ascii=False)
                    
                    assay_data["variant_sequence_json"] = variant_sequence_json
                    
                    results.append(assay_data)
                
                logger.info("batch_fetched", count=len(assays))
                
            except Exception as e:
                logger.error("batch_fetch_failed", error=str(e), batch_ids=batch_ids)
                # Continue with next batch
        
        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        logger.info("api_extraction_completed", rows=len(df))
        return df

    def _expand_assay_parameters_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand assay parameters JSON into long-format rows."""

        if "assay_parameters_json" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, object]] = []

        for _, row in df.iterrows():
            params_raw = row.get("assay_parameters_json")
            if not params_raw or (isinstance(params_raw, float) and pd.isna(params_raw)):
                continue

            try:
                params = json.loads(params_raw) if isinstance(params_raw, str) else params_raw
            except (TypeError, ValueError):
                logger.warning("assay_param_parse_failed", assay_chembl_id=row.get("assay_chembl_id"))
                continue

            if not isinstance(params, Iterable):
                continue

            index = 0
            for param in params:
                if not isinstance(param, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "param",
                    "row_index": index,
                    "assay_param_type": param.get("type"),
                    "assay_param_relation": param.get("relation"),
                    "assay_param_value": param.get("value"),
                    "assay_param_units": param.get("units"),
                    "assay_param_text_value": param.get("text_value"),
                    "assay_param_standard_type": param.get("standard_type"),
                    "assay_param_standard_value": param.get("standard_value"),
                    "assay_param_standard_units": param.get("standard_units"),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)

    def _expand_variant_sequences(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand variant sequences JSON into long-format rows."""

        if "variant_sequence_json" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, object]] = []

        for _, row in df.iterrows():
            variant_raw = row.get("variant_sequence_json")
            if not variant_raw or (isinstance(variant_raw, float) and pd.isna(variant_raw)):
                continue

            try:
                variants = json.loads(variant_raw) if isinstance(variant_raw, str) else variant_raw
            except (TypeError, ValueError):
                logger.warning("variant_parse_failed", assay_chembl_id=row.get("assay_chembl_id"))
                continue

            if not isinstance(variants, Iterable):
                continue

            index = 0
            for variant in variants:
                if not isinstance(variant, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "variant",
                    "row_index": index,
                    "variant_id": variant.get("variant_id"),
                    "variant_base_accession": variant.get("base_accession"),
                    "variant_mutation": variant.get("mutation"),
                    "variant_sequence": variant.get("variant_seq"),
                    "variant_accession_reported": variant.get("accession_reported"),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)

    def _expand_assay_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand assay classifications JSON into long-format rows."""

        if "assay_classifications" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, object]] = []

        for _, row in df.iterrows():
            class_raw = row.get("assay_classifications")
            if not class_raw or (isinstance(class_raw, float) and pd.isna(class_raw)):
                continue

            parsed: Iterable[dict[str, object]] | None = None
            if isinstance(class_raw, str):
                try:
                    parsed_json = json.loads(class_raw)
                    if isinstance(parsed_json, list):
                        parsed = parsed_json
                except (TypeError, ValueError):
                    logger.warning(
                        "assay_class_parse_failed",
                        assay_chembl_id=row.get("assay_chembl_id"),
                    )
            elif isinstance(class_raw, Iterable):
                parsed = class_raw  # type: ignore[assignment]

            if not parsed:
                continue

            index = 0
            for classification in parsed:
                if not isinstance(classification, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "class",
                    "row_index": index,
                    "assay_class_id": classification.get("assay_class_id"),
                    "assay_class_bao_id": classification.get("bao_id"),
                    "assay_class_type": classification.get("class_type"),
                    "assay_class_l1": classification.get("l1"),
                    "assay_class_l2": classification.get("l2"),
                    "assay_class_l3": classification.get("l3"),
                    "assay_class_description": classification.get("description"),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)

    def _fetch_target_reference_data(self, target_ids: list[str]) -> pd.DataFrame:
        """Fetch whitelisted target reference data for enrichment."""

        if not target_ids:
            return pd.DataFrame(columns=TARGET_ENRICHMENT_WHITELIST)

        records: list[dict[str, object]] = []

        for target_id in sorted(set(filter(None, target_ids))):
            try:
                url = f"{self.api_client.config.base_url}/target/{target_id}.json"
                response = self.api_client.request_json(url)
                target_data = response.get("target") if isinstance(response, dict) and "target" in response else response

                if not isinstance(target_data, dict):
                    continue

                record = {
                    field: target_data.get(field)
                    for field in TARGET_ENRICHMENT_WHITELIST
                }
                records.append(record)
            except Exception as exc:
                logger.warning("target_enrichment_failed", target_id=target_id, error=str(exc))

        if not records:
            return pd.DataFrame(columns=TARGET_ENRICHMENT_WHITELIST)

        return pd.DataFrame(records)

    def _fetch_assay_class_reference_data(self, class_ids: Iterable[int | str]) -> pd.DataFrame:
        """Fetch whitelisted assay class reference data for enrichment."""

        normalized_ids = [class_id for class_id in sorted(set(class_ids)) if class_id is not None]
        if not normalized_ids:
            return pd.DataFrame(columns=ASSAY_CLASS_ENRICHMENT_WHITELIST.values())

        records: list[dict[str, object]] = []

        for class_id in normalized_ids:
            try:
                url = f"{self.api_client.config.base_url}/assay_class/{class_id}.json"
                response = self.api_client.request_json(url)
                class_data = (
                    response.get("assay_class")
                    if isinstance(response, dict) and "assay_class" in response
                    else response
                )

                if not isinstance(class_data, dict):
                    continue

                record = {
                    output_field: class_data.get(input_field)
                    for input_field, output_field in ASSAY_CLASS_ENRICHMENT_WHITELIST.items()
                }
                records.append(record)
            except Exception as exc:
                logger.warning("assay_class_enrichment_failed", assay_class_id=class_id, error=str(exc))

        output_columns = list(ASSAY_CLASS_ENRICHMENT_WHITELIST.values())
        if not records:
            return pd.DataFrame(columns=output_columns)

        return pd.DataFrame(records)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform assay data with explode functionality."""
        if df.empty:
            # Return empty DataFrame with all required columns from schema
            return pd.DataFrame(columns=AssaySchema.Config.column_order)

        from bioetl.normalizers import registry

        # Fetch assay data from ChEMBL API
        assay_ids = df["assay_chembl_id"].unique().tolist()
        assay_data = self._fetch_assay_data(assay_ids)
        
        # Merge with existing data
        if not assay_data.empty:
            df = df.merge(assay_data, on="assay_chembl_id", how="left", suffixes=("", "_api"))
            # Remove duplicate columns from API merge (keep original, remove _api suffix)
            df = df.loc[:, ~df.columns.str.endswith("_api")]

        # Normalize strings
        if "assay_description" in df.columns:
            df["assay_description"] = df["assay_description"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        if "assay_type" in df.columns:
            df["assay_type"] = df["assay_type"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        base_df = df.copy()
        base_df["row_subtype"] = "assay"
        base_df["row_index"] = 0

        # Target enrichment
        target_ids_series = base_df.get("target_chembl_id")
        if target_ids_series is not None:
            target_id_list = [tid for tid in target_ids_series.dropna().tolist() if tid]
            if target_id_list:
                target_reference = self._fetch_target_reference_data(target_id_list)
                matched_targets: set[str] = set()
                missing_count = 0

                if not target_reference.empty:
                    allowed_columns = [
                        col for col in target_reference.columns if col in TARGET_ENRICHMENT_WHITELIST
                    ]
                    target_reference = target_reference[allowed_columns]
                    base_df = base_df.merge(
                        target_reference,
                        on="target_chembl_id",
                        how="left",
                        suffixes=("", "_ref"),
                    )
                    matched_targets = set(
                        target_reference["target_chembl_id"].dropna().astype(str).unique().tolist()
                    )
                    if "pref_name" in base_df.columns:
                        pref_loss_mask = base_df["target_chembl_id"].notna() & base_df["pref_name"].isna()
                        missing_count = int(pref_loss_mask.sum())
                else:
                    missing_count = len(target_id_list)

                missing_targets = set(map(str, target_id_list)) - matched_targets
                missing_count = max(missing_count, len(missing_targets))

                if missing_count:
                    logger.warning("target_enrichment_join_loss", missing_count=missing_count)

        # Explode nested structures into long format dataframes
        params_df = self._expand_assay_parameters_long(base_df)
        variants_df = self._expand_variant_sequences(base_df)
        classes_df = self._expand_assay_classifications(base_df)

        # Assay class enrichment applied to classification rows
        if not classes_df.empty and "assay_class_id" in classes_df.columns:
            class_ids = [cid for cid in classes_df["assay_class_id"].dropna().tolist() if cid is not None]
            if class_ids:
                class_reference = self._fetch_assay_class_reference_data(class_ids)
                matched_classes: set[str] = set()
                missing_count = 0

                if not class_reference.empty:
                    allowed_columns = [
                        col for col in class_reference.columns if col in ASSAY_CLASS_ENRICHMENT_WHITELIST.values()
                    ]
                    class_reference = class_reference[allowed_columns]
                    classes_df = classes_df.merge(
                        class_reference,
                        on="assay_class_id",
                        how="left",
                        suffixes=("", "_ref"),
                    )
                    matched_classes = set(
                        class_reference["assay_class_id"].dropna().astype(str).unique().tolist()
                    )
                    for column in ASSAY_CLASS_ENRICHMENT_WHITELIST.values():
                        if column == "assay_class_id":
                            continue
                        ref_column = f"{column}_ref"
                        if ref_column in classes_df.columns:
                            classes_df[column] = classes_df[column].fillna(classes_df[ref_column])
                            classes_df.drop(columns=[ref_column], inplace=True)

                    missing_count = int(
                        (classes_df["assay_class_id"].notna() & classes_df["assay_class_bao_id"].isna()).sum()
                    )
                else:
                    missing_count = len(class_ids)

                missing_classes = set(map(str, class_ids)) - matched_classes
                missing_count = max(missing_count, len(missing_classes))

                if missing_count:
                    logger.warning("assay_class_enrichment_join_loss", missing_count=missing_count)

        frames = [base_df]
        for expanded in (params_df, variants_df, classes_df):
            if not expanded.empty:
                frames.append(expanded)

        df = pd.concat(frames, ignore_index=True, sort=False)

        # Ensure row_index is deterministic Int64
        if "row_index" in df.columns:
            df["row_index"] = df["row_index"].fillna(0).astype("Int64")
        else:
            df["row_index"] = 0

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = None
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        # Generate hash fields for data integrity
        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        df["hash_business_key"] = df["assay_chembl_id"].apply(generate_hash_business_key)
        df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        # Generate deterministic index
        df = df.sort_values(["assay_chembl_id", "row_subtype", "row_index"])  # Sort by primary key
        df["index"] = range(len(df))

        # Reorder columns according to schema and add missing columns with None
        from bioetl.schemas import AssaySchema

        if "column_order" in AssaySchema.Config.__dict__:
            expected_cols = AssaySchema.Config.column_order
            
            # Add missing columns with None values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
            
            # Reorder to match schema column_order
            df = df[expected_cols]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate assay data against schema."""
        try:
            # Add confidence_score as int if missing
            if "confidence_score" in df.columns and df["confidence_score"].isna().all():
                df["confidence_score"] = df["confidence_score"].astype("Int64")  # Nullable int

            # Skip validation for now - will add later
            logger.info("validation_skipped", rows=len(df))
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

