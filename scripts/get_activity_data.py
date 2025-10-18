#!/usr/bin/env python3
"""CLI script for extracting activity data from ChEMBL API."""

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from library.activity import (
    ActivityChEMBLClient,
    ActivityConfig,
    ActivityNormalizer,
    ActivityValidator,
    ActivityQualityFilter
)
from library.config import APIClientConfig


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('activity_extraction.log')
        ]
    )


def load_config(config_path: str | None = None) -> ActivityConfig:
    """Load configuration from file or use defaults."""
    if config_path and Path(config_path).exists():
        return ActivityConfig.from_yaml(config_path)
    else:
        return ActivityConfig()


def load_filter_ids(csv_path: str | None = None) -> list[str] | None:
    """Load filter IDs from CSV file."""
    if not csv_path or not Path(csv_path).exists():
        return None
    
    try:
        df = pd.read_csv(csv_path)
        # Assume first column contains the IDs
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        logging.warning(f"Failed to load filter IDs from {csv_path}: {e}")
        return None


def load_activity_ids_from_csv(csv_path: str) -> dict[str, list[str]]:
    """Load activity filter IDs from CSV file with multiple columns."""
    if not Path(csv_path).exists():
        logging.warning(f"Activity CSV file not found: {csv_path}")
        return {}
    
    try:
        df = pd.read_csv(csv_path)
        filters = {}
        
        # Map common column names to filter types
        column_mapping = {
            'assay_chembl_id': 'assay_ids',
            'assay_id': 'assay_ids',
            'molecule_chembl_id': 'molecule_ids', 
            'molecule_id': 'molecule_ids',
            'testitem_chembl_id': 'molecule_ids',
            'target_chembl_id': 'target_ids',
            'target_id': 'target_ids',
            'document_chembl_id': 'document_ids',
            'document_id': 'document_ids'
        }
        
        for col in df.columns:
            if col in column_mapping:
                filter_type = column_mapping[col]
                ids = df[col].dropna().astype(str).tolist()
                if ids:
                    filters[filter_type] = ids
                    logging.info(f"Loaded {len(ids)} {filter_type} from {col}")
        
        return filters
    except Exception as e:
        logging.error(f"Failed to load activity IDs from {csv_path}: {e}")
        return {}


def calculate_file_hash(file_path: Path) -> str:
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def save_results(
    accepted_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    validation_report: dict[str, Any],
    quality_stats: dict[str, Any],
    metadata: dict[str, Any],
    output_dir: Path,
    date_tag: str
) -> None:
    """Save extraction results to files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save main activity data
    if not accepted_df.empty:
        activity_file = output_dir / f"activity__{date_tag}.csv"
        accepted_df.to_csv(activity_file, index=False, encoding='utf-8')
        logging.info(f"Saved {len(accepted_df)} activity records to {activity_file}")
    
    # Save rejected data
    if not rejected_df.empty:
        rejected_file = output_dir / f"rejected__{date_tag}.csv"
        rejected_df.to_csv(rejected_file, index=False, encoding='utf-8')
        logging.info(f"Saved {len(rejected_df)} rejected records to {rejected_file}")
    
    # Save validation report
    validation_file = output_dir / "activity_validation.json"
    with open(validation_file, 'w', encoding='utf-8') as f:
        json.dump(validation_report, f, indent=2, ensure_ascii=False)
    logging.info(f"Saved validation report to {validation_file}")
    
    # Save metadata
    metadata_file = output_dir / "meta.yaml"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        yaml.dump(metadata, f, default_flow_style=False, allow_unicode=True)
    logging.info(f"Saved metadata to {metadata_file}")
    
    # Calculate and save file hashes
    hashes = {}
    for file_path in [activity_file, rejected_file, validation_file, metadata_file]:
        if file_path.exists():
            hashes[file_path.name] = calculate_file_hash(file_path)
    
    # Add hashes to metadata
    metadata['file_hashes'] = hashes
    with open(metadata_file, 'w', encoding='utf-8') as f:
        yaml.dump(metadata, f, default_flow_style=False, allow_unicode=True)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Extract activity data from ChEMBL API")
    
    # API settings
    parser.add_argument("--chembl-base-url", 
                       default="https://www.ebi.ac.uk/chembl/api/data",
                       help="ChEMBL API base URL")
    parser.add_argument("--release", 
                       help="ChEMBL release version")
    parser.add_argument("--limit", 
                       type=int, 
                       default=1000,
                       help="Maximum records per page")
    
    # Filter settings
    parser.add_argument("--activity-csv", 
                       default="data/input/activity.csv",
                       help="CSV file with activity filter IDs")
    parser.add_argument("--assay-ids-csv", 
                       help="CSV file with assay IDs to filter")
    parser.add_argument("--molecule-ids-csv", 
                       help="CSV file with molecule IDs to filter")
    parser.add_argument("--target-ids-csv", 
                       help="CSV file with target IDs to filter")
    
    # Quality settings
    parser.add_argument("--strict-quality", 
                       type=bool, 
                       default=True,
                       help="Use strict quality profile")
    
    # Output settings
    parser.add_argument("--outdir", 
                       default="data/output/activity",
                       help="Output directory")
    parser.add_argument("--cache-dir", 
                       default="data/cache/activity/raw",
                       help="Cache directory")
    
    # Runtime settings
    parser.add_argument("--max-retries", 
                       type=int, 
                       default=5,
                       help="Maximum retry attempts")
    parser.add_argument("--dry-run", 
                       action="store_true",
                       help="Dry run mode (no actual API calls)")
    parser.add_argument("--config", 
                       default="configs/config_activity_full.yaml",
                       help="Configuration file path")
    parser.add_argument("--log-level", 
                       default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Override config with command line arguments
        if args.chembl_base_url != "https://www.ebi.ac.uk/chembl/api/data":
            config.chembl_base_url = args.chembl_base_url
        if args.limit != 1000:
            config.limit = args.limit
        if args.max_retries != 5:
            config.max_retries = args.max_retries
        if args.dry_run:
            config.dry_run = args.dry_run
        if args.outdir != "data/output/activity":
            config.output_dir = args.outdir
        if args.cache_dir != "data/cache/activity/raw":
            config.cache_dir = args.cache_dir
        if args.strict_quality != True:
            config.strict_quality = args.strict_quality
        
        if args.release:
            config.chembl_release = args.release
        
        # Load filter IDs from activity CSV
        activity_filters = load_activity_ids_from_csv(args.activity_csv)
        assay_ids = activity_filters.get('assay_ids') or load_filter_ids(args.assay_ids_csv)
        molecule_ids = activity_filters.get('molecule_ids') or load_filter_ids(args.molecule_ids_csv)
        target_ids = activity_filters.get('target_ids') or load_filter_ids(args.target_ids_csv)
        
        # Generate date tag
        date_tag = datetime.now().strftime("%Y%m%d")
        
        logger.info(f"Starting activity data extraction")
        logger.info(f"ChEMBL URL: {config.chembl_base_url}")
        logger.info(f"Limit: {config.limit}")
        logger.info(f"Dry run: {config.dry_run}")
        
        if config.dry_run:
            logger.info("DRY RUN MODE - No actual API calls will be made")
            return 0
        
        # Initialize components
        api_config = config.to_api_client_config()
        client = ActivityChEMBLClient(api_config, cache_dir=Path(config.cache_dir))
        normalizer = ActivityNormalizer()
        validator = ActivityValidator()
        quality_filter = ActivityQualityFilter()
        
        # Get ChEMBL status
        status = client.get_chembl_status()
        logger.info(f"ChEMBL status: {status}")
        
        # Extract data
        logger.info("Starting data extraction...")
        activities = []
        
        for activity_data in client.fetch_all_activities(
            limit=config.limit,
            assay_ids=assay_ids,
            molecule_ids=molecule_ids,
            target_ids=target_ids,
            use_cache=True
        ):
            parsed_activity = client._parse_activity(activity_data)
            activities.append(parsed_activity)
            
            if len(activities) % 1000 == 0:
                logger.info(f"Extracted {len(activities)} activities...")
        
        logger.info(f"Extraction completed. Total activities: {len(activities)}")
        
        if not activities:
            logger.warning("No activities extracted")
            return 0
        
        # Convert to DataFrame
        raw_df = pd.DataFrame(activities)
        logger.info(f"Created DataFrame with {len(raw_df)} records")
        
        # Normalize data
        logger.info("Normalizing data...")
        normalized_df = normalizer.normalize_activities(raw_df)
        
        # Validate data
        logger.info("Validating data...")
        validated_df = validator.validate_normalized_data(normalized_df)
        validation_report = validator.get_validation_report(validated_df)
        
        # Apply quality filters
        logger.info("Applying quality filters...")
        quality_results = quality_filter.apply_quality_profiles(validated_df)
        quality_stats = quality_filter.get_quality_statistics(validated_df)
        
        # Prepare final datasets
        if config.strict_quality:
            accepted_df = quality_results['strict_quality']['accepted']
        else:
            accepted_df = quality_results['moderate_quality']['accepted']
        
        rejected_df = quality_results['rejected']['data']
        
        # Prepare metadata
        metadata = {
            'chembl_release': status.get('chembl_release', 'unknown'),
            'extraction_url': config.get_extraction_url(),
            'extraction_ts': datetime.utcnow().isoformat() + 'Z',
            'pipeline_version': '1.0.0',
            'rows_total': len(raw_df),
            'rows_filtered_strict': quality_results['strict_quality']['accepted_count'],
            'rows_filtered_moderate': quality_results['moderate_quality']['accepted_count'],
            'rows_rejected': quality_results['rejected']['count'],
            'extraction_parameters': {
                'limit': config.limit,
                'assay_ids_count': len(assay_ids) if assay_ids else 0,
                'molecule_ids_count': len(molecule_ids) if molecule_ids else 0,
                'target_ids_count': len(target_ids) if target_ids else 0,
                'strict_quality': config.strict_quality
            }
        }
        
        # Save results
        output_dir = Path(config.output_dir)
        save_results(
            accepted_df, rejected_df, validation_report, 
            quality_stats, metadata, output_dir, date_tag
        )
        
        # Print summary
        logger.info("=" * 50)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total records extracted: {len(raw_df)}")
        logger.info(f"Records passed strict quality: {quality_results['strict_quality']['accepted_count']}")
        logger.info(f"Records passed moderate quality: {quality_results['moderate_quality']['accepted_count']}")
        logger.info(f"Records rejected: {quality_results['rejected']['count']}")
        logger.info(f"Output directory: {output_dir}")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())