"""Integration tests for target pipeline with HGNC and gene symbol."""

from pathlib import Path

import pandas as pd
import pytest

from src.library.target.config import TargetConfig
from src.library.target.pipeline import run_target_etl


class TestTargetPipelineHGNCIntegration:
    """Integration tests for HGNC and gene symbol in target pipeline."""
    
    @pytest.fixture
    def sample_target_config(self, tmp_path):
        """Create sample configuration for testing."""
        config_data = {
            "http": {
                "global_": {
                    "timeout_sec": 30.0,
                    "retries": {
                        "total": 3,
                        "backoff_multiplier": 1.5
                    }
                },
                "chembl": {
                    "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                    "timeout_sec": 30.0
                }
            },
            "io": {
                "input": {
                    "target_ids_csv": str(tmp_path / "input.csv"),
                    "target_id_column": "target_chembl_id"
                },
                "output": {
                    "dir": str(tmp_path / "output"),
                    "format": "csv",
                    "csv": {
                        "encoding": "utf-8",
                        "float_format": "%.3f",
                        "date_format": "%Y-%m-%dT%H:%M:%SZ",
                        "na_rep": "",
                        "line_terminator": None
                    }
                }
            },
            "runtime": {
                "workers": 1,
                "limit": None,
                "dry_run": False,
                "dev_mode": True,  # Use dev mode for testing
                "allow_incomplete_sources": True
            },
            "sources": {
                "uniprot": {
                    "enabled": True,
                    "base_url": "https://rest.uniprot.org",
                    "timeout_sec": 30.0
                },
                "iuphar": {
                    "enabled": True,
                    "base_url": "https://www.guidetopharmacology.org/services",
                    "timeout_sec": 30.0
                },
                "gtopdb": {
                    "enabled": True,
                    "base_url": "https://www.guidetopharmacology.org/services",
                    "timeout_sec": 30.0
                }
            },
            "postprocess": {
                "correlation": {
                    "enabled": False
                }
            },
            "determinism": {
                "enabled": True,
                "sort_columns": True,
                "sort_rows": True
            }
        }
        
        # Create input CSV
        input_df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL1234", "CHEMBL5678"]
        })
        input_df.to_csv(tmp_path / "input.csv", index=False)
        
        return TargetConfig.from_dict(config_data)
    
    def test_pipeline_extracts_hgnc_fields(self, sample_target_config):
        """Should extract HGNC fields from ChEMBL API."""
        result = run_target_etl(sample_target_config)
        
        assert "hgnc_id" in result.targets.columns
        assert "hgnc_name" in result.targets.columns
        
        # In dev mode, these fields should be present but may be empty
        # The important thing is that the schema validation passes
        assert len(result.targets) == 2
        assert all(col in result.targets.columns for col in ["hgnc_id", "hgnc_name"])
    
    def test_pipeline_extracts_gene_symbol(self, sample_target_config):
        """Should extract gene symbol from UniProt API."""
        result = run_target_etl(sample_target_config)
        
        assert "gene_symbol" in result.targets.columns
        
        # In dev mode, gene_symbol should be present but may be empty
        # The important thing is that the schema validation passes
        assert len(result.targets) == 2
        assert "gene_symbol" in result.targets.columns
    
    def test_pipeline_schema_validation_passes(self, sample_target_config):
        """Should pass schema validation with new fields."""
        result = run_target_etl(sample_target_config)
        
        # Schema validation should pass without errors
        from src.library.schemas.target_schema import TargetNormalizedSchema
        validated_df = TargetNormalizedSchema.validate(result.targets)
        
        # Should have all required fields
        required_fields = [
            "target_chembl_id",
            "hgnc_id", 
            "hgnc_name",
            "gene_symbol",
            "gtop_synonyms",
            "gtop_natural_ligands_n", 
            "gtop_interactions_n",
            "gtop_function_text_short"
        ]
        
        for field in required_fields:
            assert field in validated_df.columns
    
    def test_pipeline_handles_missing_hgnc_data(self, sample_target_config):
        """Should handle targets without HGNC data gracefully."""
        result = run_target_etl(sample_target_config)
        
        # In dev mode, HGNC fields should be present but empty
        # This tests that the pipeline doesn't crash when HGNC data is missing
        assert "hgnc_id" in result.targets.columns
        assert "hgnc_name" in result.targets.columns
        
        # Fields should be present even if empty
        assert result.targets["hgnc_id"].dtype == "object"
        assert result.targets["hgnc_name"].dtype == "object"
    
    def test_pipeline_handles_missing_gene_symbol(self, sample_target_config):
        """Should handle targets without gene symbol data gracefully."""
        result = run_target_etl(sample_target_config)
        
        # In dev mode, gene_symbol field should be present but may be empty
        assert "gene_symbol" in result.targets.columns
        
        # Field should be present even if empty
        assert result.targets["gene_symbol"].dtype == "object"
    
    def test_pipeline_output_structure(self, sample_target_config):
        """Should produce expected output structure."""
        result = run_target_etl(sample_target_config)
        
        # Check that result has expected structure
        assert hasattr(result, "targets")
        assert hasattr(result, "qc")
        assert hasattr(result, "meta")
        
        # Check targets DataFrame
        assert isinstance(result.targets, pd.DataFrame)
        assert len(result.targets) == 2
        
        # Check QC metrics
        assert isinstance(result.qc, pd.DataFrame)
        assert "metric" in result.qc.columns
        assert "value" in result.qc.columns
        
        # Check metadata
        assert isinstance(result.meta, dict)
        assert "pipeline_version" in result.meta
        assert "row_count" in result.meta
        assert result.meta["row_count"] == 2
    
    def test_pipeline_with_real_target_ids(self, tmp_path):
        """Should work with real ChEMBL target IDs in non-dev mode."""
        # This test would require real API calls, so we'll skip it in most test runs
        # but it's useful for manual testing
        pytest.skip("Skipping real API test - requires network access")
        
        config_data = {
            "http": {
                "global_": {
                    "timeout_sec": 30.0,
                    "retries": {"total": 3, "backoff_multiplier": 1.5}
                },
                "chembl": {
                    "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                    "timeout_sec": 30.0
                }
            },
            "io": {
                "input": {
                    "target_ids_csv": str(tmp_path / "input.csv"),
                    "target_id_column": "target_chembl_id"
                },
                "output": {
                    "dir": str(tmp_path / "output"),
                    "format": "csv"
                }
            },
            "runtime": {
                "workers": 1,
                "limit": 1,  # Limit to 1 target for testing
                "dry_run": False,
                "dev_mode": False,  # Use real API
                "allow_incomplete_sources": True
            },
            "sources": {
                "uniprot": {"enabled": True},
                "iuphar": {"enabled": True},
                "gtopdb": {"enabled": True}
            },
            "postprocess": {"correlation": {"enabled": False}},
            "determinism": {"enabled": True, "sort_columns": True, "sort_rows": True}
        }
        
        # Use a real human target ID that should have HGNC data
        input_df = pd.DataFrame({
            "target_chembl_id": ["CHEMBL1234"]  # Replace with real ID
        })
        input_df.to_csv(tmp_path / "input.csv", index=False)
        
        config = TargetConfig.from_dict(config_data)
        result = run_target_etl(config)
        
        # Should have extracted some data
        assert len(result.targets) == 1
        assert "hgnc_id" in result.targets.columns
        assert "hgnc_name" in result.targets.columns
        assert "gene_symbol" in result.targets.columns
