"""Data validation for document records using Pandera schemas."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema

from library.schemas.document_input_schema import DocumentInputSchema

logger = logging.getLogger(__name__)


class DocumentValidator:
    """Validates document data using Pandera schemas."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize validator with configuration."""
        self.config = config or {}
        self.strict_mode = self.config.get('validation', {}).get('strict', True)

    def get_raw_schema(self) -> DataFrameSchema:
        """Schema for raw document data from input CSV."""
        return DataFrameSchema({
            # Required fields
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r"^CHEMBL\d+$", name="chembl_id_format")
                ],
                nullable=False,
                description="ChEMBL document identifier"
            ),
            "title": Column(
                pa.String,
                nullable=False,
                description="Document title"
            ),
            
            # Optional fields
            "doi": Column(
                pa.String,
                nullable=True,
                checks=[
                    Check.str_matches(r"^10\.\d+/.*", name="doi_format")
                ],
                description="Digital Object Identifier"
            ),
            "document_pubmed_id": Column(
                pa.String,
                nullable=True,
                checks=[
                    Check.str_matches(r"^\d+$", name="pmid_format")
                ],
                description="PubMed identifier"
            ),
            "journal": Column(
                pa.String,
                nullable=True,
                description="Journal name"
            ),
            "year": Column(
                pa.Float64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(1900, name="year_min"),
                    Check.less_than_or_equal_to(2030, name="year_max")
                ],
                description="Publication year"
            ),
            
            # Legacy fields (matching actual CSV columns)
            "abstract": Column(
                pa.String,
                nullable=True,
                description="Document abstract"
            ),
            "pubmed_authors": Column(
                pa.String,
                nullable=True,
                description="Document authors from PubMed"
            ),
            "document_classification": Column(
                pa.Float64,
                nullable=True,
                description="Document classification"
            ),
            "referenses_on_previous_experiments": Column(
                pa.Boolean,
                nullable=True,
                description="Contains external links"
            ),
            "first_page": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="page_positive")
                ],
                description="First page number"
            ),
            "original_experimental_document": Column(
                pa.Boolean,
                nullable=True,
                description="Is experimental document"
            ),
            "issue": Column(
                pa.Float64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="issue_positive")
                ],
                description="Journal issue number"
            ),
            "last_page": Column(
                pa.Float64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="last_page_positive")
                ],
                description="Last page number"
            ),
            "month": Column(
                pa.Int64,
                nullable=True,
                checks=[
                    Check.greater_than_or_equal_to(1, name="month_min"),
                    Check.less_than_or_equal_to(12, name="month_max")
                ],
                description="Publication month"
            ),
            "volume": Column(
                pa.Float64,
                nullable=True,
                checks=[
                    Check.greater_than(0, name="volume_positive")
                ],
                description="Journal volume"
            ),
        })

    def get_normalized_schema(self) -> DataFrameSchema:
        """Schema for normalized document data after enrichment."""
        return DataFrameSchema({
            # Core fields
            "document_chembl_id": Column(
                pa.String,
                checks=[
                    Check.str_matches(r"^CHEMBL\d+$", name="chembl_id_format")
                ],
                nullable=False,
                description="ChEMBL document identifier"
            ),
            "title": Column(
                pa.String,
                nullable=False,
                description="Document title"
            ),
            "doi": Column(
                pa.String,
                nullable=True,
                description="Digital Object Identifier"
            ),
            "document_pubmed_id": Column(
                pa.String,
                nullable=True,
                description="PubMed identifier"
            ),
            
            # Computed fields
            "publication_date": Column(
                pa.String,
                nullable=True,
                checks=[
                    Check.str_matches(r"^\d{4}-\d{2}-\d{2}$", name="date_format")
                ],
                description="Normalized publication date"
            ),
            "document_sortorder": Column(
                pa.String,
                nullable=True,
                description="Document sort order"
            ),
            "citation": Column(
                pa.String,
                nullable=True,
                description="Formatted citation"
            ),
            
            # Source-specific fields (simplified for brevity)
            "crossref_doi": Column(pa.String, nullable=True),
            "crossref_title": Column(pa.String, nullable=True),
            "crossref_error": Column(pa.String, nullable=True),
            "openalex_doi": Column(pa.String, nullable=True),
            "openalex_title": Column(pa.String, nullable=True),
            "openalex_error": Column(pa.String, nullable=True),
            "pubmed_doi": Column(pa.String, nullable=True),
            "pubmed_title": Column(pa.String, nullable=True),
            "pubmed_error": Column(pa.String, nullable=True),
            "semantic_scholar_doi": Column(pa.String, nullable=True),
            "semantic_scholar_title": Column(pa.String, nullable=True),
            "semantic_scholar_error": Column(pa.String, nullable=True),
            "chembl_title": Column(pa.String, nullable=True),
            "chembl_doi": Column(pa.String, nullable=True),
            "chembl_pmid": Column(pa.String, nullable=True),
        })

    def validate_raw(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate raw input data using Pandera schema."""
        logger.info(f"Validating raw document data: {len(df)} records")
        
        try:
            # Use existing schema if available
            validated_df = DocumentInputSchema.validate(df, lazy=True)
            logger.info("Raw data validation passed")
            return validated_df
        except Exception as exc:
            logger.error(f"Raw data validation failed: {exc}")
            raise DocumentValidationError(f"Raw data validation failed: {exc}") from exc

    def validate_normalized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate normalized data using flexible schema."""
        logger.info(f"Validating normalized document data: {len(df)} records")
        
        # Create a flexible schema that only validates existing columns
        existing_columns = set(df.columns)
        
        # Define all possible columns with their types
        all_columns = {
            "document_chembl_id": Column(pa.String, nullable=True, required=False),
            "title": Column(pa.String, nullable=True, required=False),
            "doi": Column(pa.String, nullable=True, required=False),
            "abstract": Column(pa.String, nullable=True, required=False),
            "authors": Column(pa.String, nullable=True, required=False),
            "journal": Column(pa.String, nullable=True, required=False),
            "year": Column(pa.Float, nullable=True, required=False),
            "volume": Column(pa.String, nullable=True, required=False),
            "issue": Column(pa.String, nullable=True, required=False),
            "first_page": Column(pa.String, nullable=True, required=False),
            "last_page": Column(pa.String, nullable=True, required=False),
            "month": Column(pa.String, nullable=True, required=False),
            "pubmed_id": Column(pa.Int, nullable=True, required=False),
            "document_pubmed_id": Column(pa.String, nullable=True, required=False),
            "pubmed_authors": Column(pa.Object, nullable=True, required=False),
            "document_classification": Column(pa.String, nullable=True, required=False),
            "classification": Column(pa.String, nullable=True, required=False),
            "document_contains_external_links": Column(pa.Bool, nullable=True, required=False),
            "referenses_on_previous_experiments": Column(pa.Bool, nullable=True, required=False),
            "original_experimental_document": Column(pa.Bool, nullable=True, required=False),
            "is_experimental_doc": Column(pa.Bool, nullable=True, required=False),
            "chembl_title": Column(pa.String, nullable=True, required=False),
            "chembl_journal": Column(pa.String, nullable=True, required=False),
            "chembl_volume": Column(pa.String, nullable=True, required=False),
            "chembl_issue": Column(pa.String, nullable=True, required=False),
            "chembl_year": Column(pa.Float, nullable=True, required=False),
            "pubmed_pmcid": Column(pa.String, nullable=True, required=False),
            "pubmed_pages": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_error": Column(pa.String, nullable=True, required=False),
            "crossref_doi": Column(pa.String, nullable=True, required=False),
            "chembl_doi": Column(pa.String, nullable=True, required=False),
            "crossref_error": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_venue": Column(pa.String, nullable=True, required=False),
            "publication_date": Column(pa.String, nullable=True, required=False),
            "crossref_doc_type": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_citation_count": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_title": Column(pa.String, nullable=True, required=False),
            "crossref_title": Column(pa.String, nullable=True, required=False),
            "openalex_error": Column(pa.String, nullable=True, required=False),
            "document_sortorder": Column(pa.String, nullable=True, required=False),
            "pubmed_year": Column(pa.String, nullable=True, required=False),
            "crossref_subject": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_year": Column(pa.String, nullable=True, required=False),
            "chembl_doc_type": Column(pa.String, nullable=True, required=False),
            "pubmed_day": Column(pa.String, nullable=True, required=False),
            "openalex_type": Column(pa.String, nullable=True, required=False),
            "citation": Column(pa.String, nullable=True, required=False),
            "pubmed_issn": Column(pa.String, nullable=True, required=False),
            "pubmed_issue": Column(pa.String, nullable=True, required=False),
            "chembl_pmid": Column(pa.String, nullable=True, required=False),
            "pubmed_abstract": Column(pa.String, nullable=True, required=False),
            "pubmed_error": Column(pa.String, nullable=True, required=False),
            "pubmed_month": Column(pa.String, nullable=True, required=False),
            "openalex_doi": Column(pa.String, nullable=True, required=False),
            "pubmed_volume": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_doi": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_abstract": Column(pa.String, nullable=True, required=False),
            "openalex_title": Column(pa.String, nullable=True, required=False),
            "semantic_scholar_authors": Column(pa.String, nullable=True, required=False),
            "pubmed_journal": Column(pa.String, nullable=True, required=False),
            "pubmed_doi": Column(pa.String, nullable=True, required=False),
            "pubmed_title": Column(pa.String, nullable=True, required=False),
            "openalex_concepts": Column(pa.String, nullable=True, required=False),
            "document_citation": Column(pa.String, nullable=True, required=False),
        }
        
        # Only include columns that exist in the dataframe
        schema_columns = {col: all_columns[col] for col in existing_columns if col in all_columns}
        
        # Create flexible schema
        flexible_schema = DataFrameSchema(schema_columns, strict=False)
        
        try:
            validated_df = flexible_schema.validate(df, lazy=True)
            logger.info("Normalized data validation passed")
            return validated_df
        except Exception as exc:
            logger.error(f"Normalized data validation failed: {exc}")
            raise DocumentValidationError(f"Normalized data validation failed: {exc}") from exc

    def validate_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate business rules for document data."""
        logger.info("Validating business rules")
        
        validated_df = df.copy()
        
        # Rule 1: At least one identifier must be present (document_chembl_id is required)
        missing_chembl_id = validated_df['document_chembl_id'].isna()
        if missing_chembl_id.any():
            logger.warning(f"Found {missing_chembl_id.sum()} records with missing document_chembl_id")
        
        # Rule 2: DOI format validation
        if 'doi' in validated_df.columns:
            invalid_doi_mask = validated_df['doi'].notna() & ~validated_df['doi'].str.match(r'^10\.\d+/.*', na=False)
            if invalid_doi_mask.any():
                logger.warning(f"Found {invalid_doi_mask.sum()} records with invalid DOI format")
        
        # Rule 3: Year validation
        if 'year' in validated_df.columns:
            invalid_year_mask = (
                validated_df['year'].notna() & 
                ((validated_df['year'] < 1900) | (validated_df['year'] > 2030))
            )
            if invalid_year_mask.any():
                logger.warning(f"Found {invalid_year_mask.sum()} records with invalid year")
        
        # Rule 4: Month validation
        if 'month' in validated_df.columns:
            invalid_month_mask = (
                validated_df['month'].notna() & 
                ((validated_df['month'] < 1) | (validated_df['month'] > 12))
            )
            if invalid_month_mask.any():
                logger.warning(f"Found {invalid_month_mask.sum()} records with invalid month")
        
        # Rule 5: Page logic validation
        if 'first_page' in validated_df.columns and 'last_page' in validated_df.columns:
            page_logic_errors = (
                validated_df['first_page'].notna() & 
                validated_df['last_page'].notna() & 
                (validated_df['first_page'] > validated_df['last_page'])
            )
            if page_logic_errors.any():
                logger.warning(f"Found {page_logic_errors.sum()} records with page logic errors")
        
        # Rule 6: Publication date consistency
        if 'publication_date' in validated_df.columns and 'year' in validated_df.columns:
            date_year_mismatch = (
                validated_df['publication_date'].notna() & 
                validated_df['year'].notna() & 
                ~validated_df['publication_date'].str.startswith(validated_df['year'].astype(str))
            )
            if date_year_mismatch.any():
                logger.warning(f"Found {date_year_mismatch.sum()} records with publication date/year mismatch")
        
        return validated_df

    def validate_doi_format(self, doi: str | None) -> bool:
        """Validate DOI format."""
        if not doi or pd.isna(doi):
            return True  # Empty DOI is valid
        
        # Basic DOI format: 10.xxxx/xxxx
        doi_pattern = r'^10\.\d+/.*'
        return bool(re.match(doi_pattern, str(doi).strip()))

    def validate_pmid_format(self, pmid: str | None) -> bool:
        """Validate PubMed ID format."""
        if not pmid or pd.isna(pmid):
            return True  # Empty PMID is valid
        
        # PMID should be numeric
        try:
            int(pmid)
            return True
        except (ValueError, TypeError):
            return False

    def validate_chembl_id_format(self, chembl_id: str | None) -> bool:
        """Validate ChEMBL ID format."""
        if not chembl_id or pd.isna(chembl_id):
            return False  # ChEMBL ID is required
        
        # ChEMBL ID format: CHEMBL followed by digits
        chembl_pattern = r'^CHEMBL\d+$'
        return bool(re.match(chembl_pattern, str(chembl_id).strip()))


class DocumentValidationError(Exception):
    """Raised when document validation fails."""
    pass
