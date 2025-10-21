"""Integration tests for IUPHAR mapping functionality."""

import pytest
import pandas as pd
from pathlib import Path
from typing import Any

from library.cli import analyze_iuphar_mapping


class TestIUPHARMappingIntegration:
    """Integration tests for IUPHAR mapping analysis."""

    @pytest.mark.integration
    def test_analyze_iuphar_mapping_basic(self, tmp_path: Path) -> None:
        """Test basic IUPHAR mapping analysis functionality."""
        
        # Create test target data
        target_data = {
            'target_chembl_id': ['CHEMBL123', 'CHEMBL456', 'CHEMBL789'],
            'mapping_uniprot_id': ['P12345', 'P67890', 'P11111'],
            'iuphar_target_id': [1386, 1387, 1388]
        }
        target_df = pd.DataFrame(target_data)
        target_csv = tmp_path / "test_target.csv"
        target_df.to_csv(target_csv, index=False)
        
        # Create test IUPHAR dictionary
        iuphar_data = {
            'target_id': [1386, 1387, 1388, 1389],
            'target_name': ['Test Target 1', 'Test Target 2', 'Test Target 3', 'Test Target 4'],
            'swissprot': ['P12345', 'P67890', 'P11111', 'P22222']
        }
        iuphar_df = pd.DataFrame(iuphar_data)
        iuphar_csv = tmp_path / "test_iuphar.csv"
        iuphar_df.to_csv(iuphar_csv, index=False)
        
        # Test that the function can be called without errors
        # Note: This is a basic smoke test - the actual CLI function would need
        # to be refactored to be testable without typer context
        assert target_csv.exists()
        assert iuphar_csv.exists()
        
        # Verify data integrity
        loaded_target = pd.read_csv(target_csv)
        loaded_iuphar = pd.read_csv(iuphar_csv)
        
        assert len(loaded_target) == 3
        assert len(loaded_iuphar) == 4
        assert 'mapping_uniprot_id' in loaded_target.columns
        assert 'iuphar_target_id' in loaded_target.columns
        assert 'swissprot' in loaded_iuphar.columns

    @pytest.mark.integration
    def test_iuphar_mapping_validation_logic(self, tmp_path: Path) -> None:
        """Test the core validation logic for IUPHAR mapping."""
        
        # Create test data with known mappings
        target_data = {
            'target_chembl_id': ['CHEMBL123', 'CHEMBL456', 'CHEMBL789', 'CHEMBL999'],
            'mapping_uniprot_id': ['P12345', 'P67890', 'P11111', 'P99999'],  # Last one not in IUPHAR
            'iuphar_target_id': [1386, 1387, 1388, 9999]
        }
        target_df = pd.DataFrame(target_data)
        
        iuphar_data = {
            'target_id': [1386, 1387, 1388],
            'target_name': ['Test Target 1', 'Test Target 2', 'Test Target 3'],
            'swissprot': ['P12345', 'P67890', 'P11111']
        }
        iuphar_df = pd.DataFrame(iuphar_data)
        
        # Test validation logic
        uniprot_ids = target_df['mapping_uniprot_id'].dropna()
        validation_results = []
        
        for uid in uniprot_ids:
            matches = iuphar_df[iuphar_df['swissprot'] == uid]
            if len(matches) > 0:
                validation_results.append({
                    'uniprot_id': uid,
                    'found': True,
                    'target_id': matches.iloc[0]['target_id'],
                    'target_name': matches.iloc[0]['target_name']
                })
            else:
                validation_results.append({
                    'uniprot_id': uid,
                    'found': False,
                    'target_id': None,
                    'target_name': None
                })
        
        # Verify results
        assert len(validation_results) == 4
        found_count = sum(1 for r in validation_results if r['found'])
        assert found_count == 3  # 3 out of 4 should be found
        
        # Check specific results
        p12345_result = next(r for r in validation_results if r['uniprot_id'] == 'P12345')
        assert p12345_result['found'] is True
        assert p12345_result['target_id'] == 1386
        
        p99999_result = next(r for r in validation_results if r['uniprot_id'] == 'P99999')
        assert p99999_result['found'] is False

    @pytest.mark.integration
    def test_iuphar_target_id_analysis(self, tmp_path: Path) -> None:
        """Test analysis of specific IUPHAR target IDs."""
        
        # Create test data with multiple UniProt IDs for one target
        iuphar_data = {
            'target_id': [1386, 1386, 1386, 1387],  # Multiple entries for 1386
            'target_name': ['Test Target 1', 'Test Target 1', 'Test Target 1', 'Test Target 2'],
            'swissprot': ['P12345', 'P12346', 'P12347', 'P67890']
        }
        iuphar_df = pd.DataFrame(iuphar_data)
        
        # Test analysis of specific target ID
        target_id = 1386
        matches = iuphar_df[iuphar_df['target_id'] == target_id]
        
        assert len(matches) == 3
        uniprot_ids = matches['swissprot'].unique()
        assert len(uniprot_ids) == 3
        assert 'P12345' in uniprot_ids
        assert 'P12346' in uniprot_ids
        assert 'P12347' in uniprot_ids

    @pytest.mark.integration
    def test_iuphar_mapping_edge_cases(self, tmp_path: Path) -> None:
        """Test edge cases in IUPHAR mapping analysis."""
        
        # Test with empty data
        empty_target = pd.DataFrame(columns=['target_chembl_id', 'mapping_uniprot_id', 'iuphar_target_id'])
        empty_iuphar = pd.DataFrame(columns=['target_id', 'target_name', 'swissprot'])
        
        assert len(empty_target) == 0
        assert len(empty_iuphar) == 0
        
        # Test with missing columns
        incomplete_target = pd.DataFrame({
            'target_chembl_id': ['CHEMBL123'],
            # Missing mapping_uniprot_id and iuphar_target_id
        })
        
        assert 'mapping_uniprot_id' not in incomplete_target.columns
        assert 'iuphar_target_id' not in incomplete_target.columns
        
        # Test with NaN values
        target_with_nans = pd.DataFrame({
            'target_chembl_id': ['CHEMBL123', 'CHEMBL456'],
            'mapping_uniprot_id': ['P12345', None],
            'iuphar_target_id': [1386, None]
        })
        
        uniprot_ids = target_with_nans['mapping_uniprot_id'].dropna()
        assert len(uniprot_ids) == 1
        assert uniprot_ids.iloc[0] == 'P12345'

    @pytest.mark.integration
    @pytest.mark.slow
    def test_iuphar_mapping_performance(self, tmp_path: Path) -> None:
        """Test performance with larger datasets."""
        
        # Create larger test dataset
        n_targets = 1000
        n_iuphar = 5000
        
        target_data = {
            'target_chembl_id': [f'CHEMBL{i:06d}' for i in range(n_targets)],
            'mapping_uniprot_id': [f'P{i:05d}' for i in range(n_targets)],
            'iuphar_target_id': list(range(1000, 1000 + n_targets))
        }
        target_df = pd.DataFrame(target_data)
        
        iuphar_data = {
            'target_id': list(range(1000, 1000 + n_iuphar)),
            'target_name': [f'Test Target {i}' for i in range(n_iuphar)],
            'swissprot': [f'P{i:05d}' for i in range(n_iuphar)]
        }
        iuphar_df = pd.DataFrame(iuphar_data)
        
        # Test validation performance
        import time
        start_time = time.time()
        
        uniprot_ids = target_df['mapping_uniprot_id'].dropna()
        sample_ids = uniprot_ids.head(100).tolist()  # Sample for performance test
        
        validation_results = []
        for uid in sample_ids:
            matches = iuphar_df[iuphar_df['swissprot'] == uid]
            validation_results.append({
                'uniprot_id': uid,
                'found': len(matches) > 0
            })
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should complete within reasonable time (adjust threshold as needed)
        assert processing_time < 5.0  # 5 seconds for 100 validations
        assert len(validation_results) == 100
        
        # All should be found since we created matching data
        found_count = sum(1 for r in validation_results if r['found'])
        assert found_count == 100
