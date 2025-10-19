"""Tests for assay metadata structure."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from library.assay import AssayConfig, run_assay_etl


class TestAssayMetadata:
    """Test cases for assay metadata structure."""

    @patch('library.assay.pipeline._create_api_client')
    def test_assay_metadata_structure(self, mock_create_client):
        """Test that assay metadata has the correct structure aligned with documents."""
        # Setup mock client
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        # Mock ChEMBL status
        mock_client.get_chembl_status.return_value = {"chembl_release": "33"}
        
        # Mock assay data extraction
        mock_assay_data = {
            "assay_chembl_id": "CHEMBL123",
            "assay_type": "B",
            "assay_type_description": "Binding",
            "src_id": 1,
            "src_assay_id": "SRC123",
            "bao_format": "BAO_0000001",
            "bao_label": "Label1",
            "assay_category": ["cat1"],
            "assay_classifications": ["class1"],
            "target_chembl_id": "CHEMBL789",
            "relationship_type": "D",
            "confidence_score": 7,
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "assay_cell_type": "Cell1",
            "assay_tissue": "Tissue1",
            "assay_strain": "Strain1",
            "assay_subcellular_fraction": "Fraction1",
            "assay_parameters": {"param1": "value1"},
            "assay_format": "Format1",
            "description": "Test assay",
            "source_system": "ChEMBL",
            "extracted_at": "2023-01-01T00:00:00",
            "hash_row": "hash1",
            "hash_business_key": "key1"
        }
        mock_client.fetch_by_assay_id.return_value = mock_assay_data
        
        # Mock source enrichment
        mock_client.fetch_source_info.return_value = {
            "src_id": 1,
            "src_assay_id": "SRC123",
            "bao_format": "BAO_0000001",
            "bao_label": "Label1",
            "assay_category": ["cat1"],
            "assay_classifications": ["class1"],
            "target_chembl_id": "CHEMBL789",
            "relationship_type": "D",
            "confidence_score": 7,
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "assay_cell_type": "Cell1",
            "assay_tissue": "Tissue1",
            "assay_strain": "Strain1",
            "assay_subcellular_fraction": "Fraction1",
            "assay_parameters": {"param1": "value1"},
            "assay_format": "Format1",
            "src_name": "Test Source",
            "src_short_name": "TS",
            "src_url": "http://example.com"
        }
        
        # Create config
        config = AssayConfig()
        
        # Run ETL with single assay ID to avoid duplicate data issues
        result = run_assay_etl(
            config=config,
            assay_ids=["CHEMBL123"]
        )
        
        # Проверяем, что метаданные созданы
        assert hasattr(result, 'meta'), "Результат должен содержать метаданные"
        assert isinstance(result.meta, dict), "Метаданные должны быть словарем"
        
        # Проверяем основные поля метаданных (aligned with documents)
        assert 'pipeline_version' in result.meta, "Метаданные должны содержать версию пайплайна"
        assert 'row_count' in result.meta, "Метаданные должны содержать количество строк"
        assert 'enabled_sources' in result.meta, "Метаданные должны содержать включенные источники"
        assert 'extraction_parameters' in result.meta, "Метаданные должны содержать параметры извлечения"
        assert 'chembl_release' in result.meta, "Метаданные должны содержать версию ChEMBL"
        
        # Проверяем значения
        assert result.meta['pipeline_version'] == "1.0.0"
        assert result.meta['row_count'] == 1
        assert result.meta['enabled_sources'] == ['chembl']
        assert result.meta['chembl_release'] == "33"
        
        # Проверяем параметры извлечения
        extraction_params = result.meta['extraction_parameters']
        assert extraction_params['total_assays'] == 1
        assert extraction_params['unique_sources'] == 1
        assert extraction_params['chembl_records'] == 1
        assert extraction_params['correlation_analysis_enabled'] == False
        assert extraction_params['correlation_insights_count'] == 0
        
        # Проверяем, что есть распределения типов ассев
        assert 'assay_types' in extraction_params
        assert 'relationship_types' in extraction_params

    @patch('library.assay.pipeline._create_api_client')
    def test_assay_metadata_with_correlation(self, mock_create_client):
        """Test assay metadata with correlation analysis enabled."""
        # Setup mock client
        mock_client = Mock()
        mock_create_client.return_value = mock_client
        
        # Mock ChEMBL status
        mock_client.get_chembl_status.return_value = {"chembl_release": "33"}
        
        # Mock batch extraction to fail, forcing individual requests
        mock_client.fetch_assays_batch.side_effect = Exception("Batch API Error")
        
        # Mock assay data extraction
        mock_assay_data = {
            "assay_chembl_id": "CHEMBL123",
            "assay_type": "B",
            "assay_type_description": "Binding",
            "src_id": 1,
            "src_assay_id": "SRC123",
            "bao_format": "BAO_0000001",
            "bao_label": "Label1",
            "assay_category": ["cat1"],
            "assay_classifications": ["class1"],
            "target_chembl_id": "CHEMBL789",
            "relationship_type": "D",
            "confidence_score": 7,
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "assay_cell_type": "Cell1",
            "assay_tissue": "Tissue1",
            "assay_strain": "Strain1",
            "assay_subcellular_fraction": "Fraction1",
            "assay_parameters": {"param1": "value1"},
            "assay_format": "Format1",
            "description": "Test assay",
            "source_system": "ChEMBL",
            "extracted_at": "2023-01-01T00:00:00",
            "hash_row": "hash1",
            "hash_business_key": "key1"
        }
        
        # Mock assay data extraction with unique values for each call
        def mock_fetch_by_assay_id(assay_id):
            if assay_id == "CHEMBL123":
                return mock_assay_data
            else:  # CHEMBL456
                return {
                    "assay_chembl_id": "CHEMBL456",
                    "assay_type": "F",
                    "assay_type_description": "Functional",
                    "src_id": 2,
                    "src_assay_id": "SRC456",
                    "bao_format": "BAO_0000002",
                    "bao_label": "Label2",
                    "assay_category": ["cat2"],
                    "assay_classifications": ["class2"],
                    "target_chembl_id": "CHEMBL012",
                    "relationship_type": "D",
                    "confidence_score": 8,
                    "assay_organism": "Mus musculus",
                    "assay_tax_id": 10090,
                    "assay_cell_type": "Cell2",
                    "assay_tissue": "Tissue2",
                    "assay_strain": "Strain2",
                    "assay_subcellular_fraction": "Fraction2",
                    "assay_parameters": {"param2": "value2"},
                    "assay_format": "Format2",
                    "description": "Test assay 2",
                    "source_system": "ChEMBL",
                    "extracted_at": "2023-01-01T00:00:00",
                    "hash_row": "hash2",
                    "hash_business_key": "key2"
                }
        
        mock_client.fetch_by_assay_id.side_effect = mock_fetch_by_assay_id
        
        # Mock source enrichment
        mock_client.fetch_source_info.return_value = {
            "src_id": 1,
            "src_name": "Test Source",
            "src_short_name": "TS",
            "src_url": "http://example.com"
        }
        
        # Create config with correlation enabled
        config = AssayConfig()
        config.postprocess.correlation.enabled = True
        
        # Mock correlation analysis functions
        with patch('library.assay.pipeline.prepare_data_for_correlation_analysis') as mock_prepare, \
             patch('library.assay.pipeline.build_enhanced_correlation_analysis') as mock_analysis, \
             patch('library.assay.pipeline.build_enhanced_correlation_reports') as mock_reports, \
             patch('library.assay.pipeline.build_correlation_insights') as mock_insights:
            
            # Setup correlation mocks - need at least 2 columns for correlation
            mock_prepare.return_value = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
            mock_analysis.return_value = {'correlation_matrix': [[1, 0.5], [0.5, 1]]}
            mock_reports.return_value = {'report1': pd.DataFrame({'metric': ['value1']})}
            mock_insights.return_value = [{'insight1': 'value1'}, {'insight2': 'value2'}]
            
            # Run ETL with two assay IDs to enable correlation analysis
            result = run_assay_etl(
                config=config,
                assay_ids=["CHEMBL123", "CHEMBL456"]
            )
            
            # Проверяем, что корреляционный анализ включен в метаданные
            extraction_params = result.meta['extraction_parameters']
            assert extraction_params['correlation_analysis_enabled'] == True
            assert extraction_params['correlation_insights_count'] == 2

    def test_assay_qc_structure(self):
        """Test that assay QC has the correct structure aligned with documents."""
        from library.assay.pipeline import AssayETLResult
        
        # Create test QC data
        qc_df = pd.DataFrame([
            {"metric": "row_count", "value": 10},
            {"metric": "enabled_sources", "value": 1},
            {"metric": "chembl_records", "value": 10}
        ])
        
        # Create test result
        result = AssayETLResult(
            assays=pd.DataFrame(),
            qc=qc_df,
            meta={}
        )
        
        # Проверяем структуру QC
        assert len(result.qc) == 3
        assert set(result.qc['metric']) == {'row_count', 'enabled_sources', 'chembl_records'}
        assert result.qc['value'].sum() == 21


if __name__ == "__main__":
    pytest.main([__file__])
