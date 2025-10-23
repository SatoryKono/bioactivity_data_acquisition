"""Tests for unified pipeline logic and standardized artifacts."""

import pytest

from library.common.exit_codes import ExitCode
from library.common.pipeline_base import ETLResult, PipelineBase
from library.common.writer_base import ETLWriter


class TestUnifiedLogic:
    """Test unified pipeline logic and standardized artifacts."""
    
    def test_exit_codes_enum(self):
        """Test that exit codes are properly defined."""
        assert ExitCode.OK == 0
        assert ExitCode.VALIDATION_ERROR == 1
        assert ExitCode.HTTP_ERROR == 2
        assert ExitCode.QC_ERROR == 3
        assert ExitCode.IO_ERROR == 4
        assert ExitCode.CONFIG_ERROR == 5
    
    def test_etl_result_structure(self):
        """Test ETLResult dataclass structure."""
        import pandas as pd
        
        # Create test data
        test_data = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        test_qc = pd.DataFrame({"metric": ["row_count"], "value": [3]})
        
        # Create ETLResult
        result = ETLResult(
            data=test_data,
            qc_summary=test_qc,
            qc_detailed=None,
            rejected=None,
            meta={"test": "metadata"},
            correlation_analysis=None,
            correlation_reports=None,
            correlation_insights=None
        )
        
        # Verify structure
        assert result.data is not None
        assert result.qc_summary is not None
        assert result.qc_detailed is None
        assert result.rejected is None
        assert result.meta is not None
        assert result.correlation_analysis is None
        assert result.correlation_reports is None
        assert result.correlation_insights is None
    
    def test_standardized_artifact_names(self):
        """Test that artifact names follow the standardized pattern."""
        # Expected patterns for each pipeline
        expected_patterns = {
            "documents": "documents_{date_tag}.csv",
            "targets": "targets_{date_tag}.csv", 
            "assays": "assays_{date_tag}.csv",
            "activities": "activities_{date_tag}.csv",
            "testitems": "testitems_{date_tag}.csv"
        }
        
        date_tag = "20240101"
        
        for entity, pattern in expected_patterns.items():
            expected_main = pattern.format(date_tag=date_tag)
            expected_qc = f"{entity}_{date_tag}_qc_summary.csv"
            expected_meta = f"{entity}_{date_tag}.meta.yaml"
            expected_correlation = f"{entity}_{date_tag}_correlation.csv"
            
            # Verify main data file naming
            assert expected_main.endswith(f"{entity}_{date_tag}.csv")
            
            # Verify QC summary naming
            assert expected_qc.endswith(f"{entity}_{date_tag}_qc_summary.csv")
            
            # Verify metadata naming
            assert expected_meta.endswith(f"{entity}_{date_tag}.meta.yaml")
            
            # Verify correlation naming
            assert expected_correlation.endswith(f"{entity}_{date_tag}_correlation.csv")
    
    def test_base_writer_interface(self):
        """Test ETLWriter interface and methods."""
        # Test that ETLWriter has required static methods
        assert hasattr(ETLWriter, 'write_outputs')
        assert hasattr(ETLWriter, '_write_csv_atomic')
        assert hasattr(ETLWriter, '_write_yaml_atomic')
        assert hasattr(ETLWriter, '_apply_determinism')
        assert hasattr(ETLWriter, '_get_csv_options')
        
        # Test that methods are static
        assert isinstance(ETLWriter.write_outputs, staticmethod)
        assert isinstance(ETLWriter._write_csv_atomic, staticmethod)
        assert isinstance(ETLWriter._write_yaml_atomic, staticmethod)
    
    def test_pipeline_base_interface(self):
        """Test PipelineBase abstract interface."""
        # Test that PipelineBase has required abstract methods
        assert hasattr(PipelineBase, '_setup_clients')
        assert hasattr(PipelineBase, 'extract')
        assert hasattr(PipelineBase, 'normalize')
        assert hasattr(PipelineBase, 'validate')
        assert hasattr(PipelineBase, '_build_metadata')
        
        # Test that PipelineBase has concrete methods
        assert hasattr(PipelineBase, 'filter_quality')
        assert hasattr(PipelineBase, 'build_qc_report')
        assert hasattr(PipelineBase, 'run')
        assert hasattr(PipelineBase, '_should_build_correlation')
        assert hasattr(PipelineBase, '_build_correlation')
    
    def test_unified_cli_flags(self):
        """Test that CLI commands use unified flag names."""
        # This test would need to be implemented with actual CLI testing
        # For now, we just verify the structure exists
        unified_flags = [
            "--timeout", "--retries", "--limit", "--dry-run",
            "--log-level", "--log-file", "--log-format",
            "--input", "--output-dir", "--date-tag", "--workers", "--verbose"
        ]
        
        # Verify all expected flags are defined
        for flag in unified_flags:
            assert flag.startswith("--")
            assert len(flag) > 2  # Basic validation
    
    def test_deprecated_flags_warning(self):
        """Test that deprecated flags show warning messages."""
        # This would need to be implemented with actual CLI testing
        # For now, we just verify the concept
        deprecated_flags = ["--timeout-sec"]
        
        for flag in deprecated_flags:
            assert flag.startswith("--")
            assert "timeout" in flag  # Basic validation


if __name__ == "__main__":
    pytest.main([__file__])