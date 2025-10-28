"""Assay Pipeline - ChEMBL assay data extraction."""

import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandera.errors import SchemaErrors

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import AssaySchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("assay", "1.0.0", AssaySchema)


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
                    classifications_str = " | ".join(classifications) if classifications else None
                    
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

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform assay data with explode functionality."""
        if df.empty:
            # Return empty DataFrame with all required columns from schema
            return pd.DataFrame(columns=AssaySchema.get_column_order())

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

        # Explode nested structures into long format
        # For now, create basic exploded rows (assay records)
        exploded_rows = []

        for _, row in df.iterrows():
            # Create base assay row
            assay_row = row.to_dict()
            assay_row["row_subtype"] = "assay"
            exploded_rows.append(assay_row)

            # TODO: In future, add explode for params and variants
            # if "assay_parameters" in row and pd.notna(row["assay_parameters"]):
            #     for idx, param in enumerate(row["assay_parameters"]):
            #         param_row = row.to_dict()
            #         param_row["row_subtype"] = "param"
            #         param_row["row_index"] = idx
            #         exploded_rows.append(param_row)

        df = pd.DataFrame(exploded_rows)

        # Generate row_index for deterministic ordering within each (assay_chembl_id, row_subtype) group
        df["row_index"] = df.groupby(["assay_chembl_id", "row_subtype"]).cumcount()

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

        expected_cols = AssaySchema.get_column_order()
        if expected_cols:
            # Add missing columns with None values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

            # Reorder to match schema column_order
            df = df[expected_cols]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate assay data against schema and referential integrity."""
        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        if "confidence_score" in df.columns:
            try:
                df["confidence_score"] = pd.to_numeric(
                    df["confidence_score"], errors="coerce"
                ).astype("Int64")
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("confidence_score_cast_failed", error=str(exc))

        try:
            validated_df = AssaySchema.validate(df, lazy=True)
        except SchemaErrors as exc:
            schema_issues = self._summarize_schema_errors(exc.failure_cases)
            for issue in schema_issues:
                self.record_validation_issue(issue)
                logger.error(
                    "schema_validation_error",
                    column=issue.get("column"),
                    check=issue.get("check"),
                    count=issue.get("count"),
                    severity=issue.get("severity"),
                )

            summary = "; ".join(
                f"{issue.get('column')}: {issue.get('check')} ({issue.get('count')} cases)"
                for issue in schema_issues
            )
            raise ValueError(f"Schema validation failed: {summary}") from exc
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("schema_validation_unexpected_error", error=str(exc))
            raise

        self._check_referential_integrity(validated_df)

        logger.info(
            "validation_completed",
            rows=len(validated_df),
            issues=len(self.validation_issues),
        )
        return validated_df

    def _summarize_schema_errors(self, failure_cases: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert Pandera failure cases to structured validation issues."""

        issues: list[dict[str, Any]] = []
        if failure_cases.empty:
            return issues

        for column, group in failure_cases.groupby("column", dropna=False):
            column_name = (
                str(column)
                if column is not None and not (isinstance(column, float) and pd.isna(column))
                else "<dataframe>"
            )
            checks = sorted({str(check) for check in group["check"].dropna().unique()})
            details = ", ".join(
                group["failure_case"].dropna().astype(str).unique().tolist()[:5]
            )
            issues.append(
                {
                    "issue_type": "schema",
                    "severity": "error",
                    "column": column_name,
                    "check": ", ".join(checks) if checks else "<unspecified>",
                    "count": int(group.shape[0]),
                    "details": details,
                }
            )

        return issues

    def _load_target_reference_ids(self) -> set[str]:
        """Load known target identifiers for referential integrity checks."""

        target_path = Path(self.config.paths.input_root) / "target.csv"
        if not target_path.exists():
            logger.warning("referential_check_skipped", reason="target_file_missing", path=str(target_path))
            return set()

        try:
            target_df = pd.read_csv(target_path, usecols=["target_chembl_id"])
        except ValueError:
            logger.warning(
                "referential_check_skipped",
                reason="target_column_missing",
                path=str(target_path),
            )
            return set()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "referential_check_skipped",
                reason="target_load_failed",
                error=str(exc),
                path=str(target_path),
            )
            return set()

        values = (
            target_df["target_chembl_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
        )
        reference_ids = set(values.tolist())
        logger.debug(
            "referential_reference_loaded",
            path=str(target_path),
            count=len(reference_ids),
        )
        return reference_ids

    def _check_referential_integrity(self, df: pd.DataFrame) -> None:
        """Validate assay → target relationships against reference data."""

        if "target_chembl_id" not in df.columns:
            logger.debug("referential_check_skipped", reason="column_absent")
            return

        reference_ids = self._load_target_reference_ids()
        if not reference_ids:
            return

        target_series = df["target_chembl_id"].astype("string").str.upper()
        missing_mask = target_series.notna() & ~target_series.isin(reference_ids)
        missing_count = int(missing_mask.sum())

        if missing_count == 0:
            logger.info("referential_integrity_passed", relation="assay->target")
            return

        total_rows = len(df)
        missing_ratio = missing_count / total_rows if total_rows else 0.0
        threshold = float(self.config.qc.thresholds.get("assay.target_missing_ratio", 0.0))
        severity = "error" if missing_ratio > threshold else "info"

        sample_targets = (
            target_series[missing_mask]
            .dropna()
            .unique()
            .tolist()[:5]
        )

        issue = {
            "issue_type": "referential_integrity",
            "severity": severity,
            "column": "target_chembl_id",
            "check": "assay->target",
            "count": missing_count,
            "ratio": missing_ratio,
            "threshold": threshold,
            "details": ", ".join(sample_targets),
        }
        self.record_validation_issue(issue)

        log_fn = logger.error if severity == "error" else logger.warning
        log_fn(
            "referential_integrity_failure",
            relation="assay->target",
            missing_count=missing_count,
            missing_ratio=missing_ratio,
            threshold=threshold,
            severity=severity,
        )

        if self._should_fail(severity):
            raise ValueError(
                "Referential integrity violation: assays reference missing targets"
            )

