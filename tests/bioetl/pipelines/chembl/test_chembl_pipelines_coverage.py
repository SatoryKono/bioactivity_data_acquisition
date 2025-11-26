import pytest
import pandas as pd
import pandera as pa
from unittest.mock import MagicMock, patch
from pathlib import Path

from bioetl.pipelines.chembl.assay.run import ChemblAssayPipeline
from bioetl.pipelines.chembl.document.run import ChemblDocumentPipeline
from bioetl.pipelines.chembl.target.run import ChemblTargetPipeline
from bioetl.pipelines.chembl.testitem.run import TestItemChemblPipeline
from bioetl.core.pipeline.types import (
    StageExecutionOptions,
    WriteArtifacts,
    WriteResult,
)
from bioetl.pipelines.chembl.common import ConfigValidationError

class TestChemblPipelinesCoverage:

    @pytest.fixture
    def options(self):
        return StageExecutionOptions(
            run_tag="test",
            mode="full",
            dry_run=False,
            extended=False
        )

    def test_assay_pipeline_transform_nested(self, options):
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["assay_chembl_id"]}},
            "postprocess": {"nested_serialization": "flatten"}
        }
        pipeline = ChemblAssayPipeline(config)
        
        df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL123"],
            "assay_parameters": [{"param1": "value1", "param2": 2}],
            "target_chembl_id": ["CHEMBL_TGT_1"]
        })
        
        transformed = pipeline.transform(df, options)
        
        assert "assay_param_param1" in transformed.columns
        assert transformed.iloc[0]["assay_param_param1"] == "value1"
        assert "assay_parameters" not in transformed.columns
        assert "assay_class_map" in transformed.columns
        
    def test_assay_pipeline_transform_json(self, options):
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["assay_chembl_id"]}},
            "postprocess": {"nested_serialization": "json"}
        }
        pipeline = ChemblAssayPipeline(config)
        
        df = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL123"],
            "assay_parameters": [{"param1": "value1"}],
             "target_chembl_id": ["CHEMBL_TGT_1"]
        })
        
        transformed = pipeline.transform(df, options)
        
        assert "assay_parameters" in transformed.columns
        assert isinstance(transformed.iloc[0]["assay_parameters"], str)
        assert "value1" in transformed.iloc[0]["assay_parameters"]

    def test_assay_pipeline_validation(self, options):
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["assay_chembl_id"]}}
        }
        pipeline = ChemblAssayPipeline(config)
        
        # Valid data (minimal)
        df_valid = pd.DataFrame({
            "assay_chembl_id": ["CHEMBL1"],
            "assay_type": ["B"],
            "description": ["Test Assay"],
            "target_chembl_id": ["CHEMBL_TGT_1"]
        })
        # Mock validator to avoid full schema validation if needed, 
        # but here we want to test the pipeline's custom validation logic
        
        # The pipeline.validate method calls super().validate which uses the schema.
        # We need to ensure our dataframe matches the schema or mock the schema validation.
        # For this test, let's just check the custom logic for missing IDs.
        
        with patch.object(pipeline, 'validation_service') as mock_service:
            mock_service.validate.return_value = df_valid
            
            # Case: missing ID
            df_missing = df_valid.copy()
            df_missing.loc[0, "assay_chembl_id"] = None
            mock_service.validate.return_value = df_missing
            
            with pytest.raises(pa.errors.SchemaError):
                pipeline.validate(df_missing, options)

    def test_document_pipeline_transform(self, options):
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["document_chembl_id"]}},
            "mode": "all",
            "fallbacks": {"policy": "best_effort"}
        }
        pipeline = ChemblDocumentPipeline(config)
        
        df = pd.DataFrame({
            "document_chembl_id": ["CHEMBL_DOC_1"],
            "title": ["Test Doc"]
        })
        
        transformed = pipeline.transform(df, options)
        
        assert "enrichment_chain" in transformed.columns
        assert "crossref" in transformed.iloc[0]["enrichment_chain"]
        assert transformed.iloc[0]["fallback_policy"] == "best_effort"

    def test_document_pipeline_config_validation(self):
        # Invalid mode
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["document_chembl_id"]}},
            "mode": "invalid_mode"
        }
        with pytest.raises(ConfigValidationError):
            ChemblDocumentPipeline(config)

    def test_target_pipeline_smoke(self, options):
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["target_chembl_id"]}}
        }
        pipeline = ChemblTargetPipeline(config)
        assert pipeline.entity_name == "target"
        
    def test_testitem_pipeline_smoke(self, options):
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["test_item_id"]}}
        }
        pipeline = TestItemChemblPipeline(config)
        assert pipeline.entity_name == "testitem"

    def test_testitem_pipeline_transform(self, options):
        mock_client = MagicMock()
        mock_client.lookup.return_value = {"cid": 123}
        
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["test_item_id"]}},
            "enrichers": {"pubchem_client": mock_client}
        }
        pipeline = TestItemChemblPipeline(config)
        
        df = pd.DataFrame({
            "test_item_id": ["CHEMBL1"],
            "inchi_key": ["  InChiKey=123  "],
            "smiles": [None],
            "molecular_weight": ["100.12345"]
        })
        
        transformed = pipeline.transform(df, options)
        
        # Check canonicalization
        assert transformed.iloc[0]["inchi_key"] == "INCHIKEY=123"
        
        # Check enrichment
        assert transformed.iloc[0]["pubchem_enrichment"] == {"cid": 123}
        
        # Check normalization
        assert transformed.iloc[0]["smiles"] == ""
        assert transformed.iloc[0]["molecular_weight"] == 100.123

    def test_testitem_pipeline_validation(self, options):
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["test_item_id"]}}
        }
        pipeline = TestItemChemblPipeline(config)
        
        # Mock validation service to return DF but allow manual validation checks
        with patch.object(pipeline, 'validation_service') as mock_service:
            df_invalid = pd.DataFrame({
                "test_item_id": [None], 
                "name": ["Test"]
            })
            mock_service.validate.return_value = df_invalid
            
            with pytest.raises(ValueError):
                pipeline.validate(df_invalid, options)

    def test_chembl_common_validate_config(self):
        # Test common config validation
        config = {
            "sources": {"chembl": {"batch_size": -1, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        with pytest.raises(ConfigValidationError, match="batch_size"):
            ChemblAssayPipeline(config)

        config["sources"]["chembl"]["batch_size"] = 10
        config["determinism"]["sort"]["by"] = ["wrong_field"]

        with pytest.raises(
            ConfigValidationError, match="missing required fields"
        ):
            ChemblAssayPipeline(config)

    def test_save_results_uses_writer(self, options, tmp_path):
        # Test save_results logic in ChemblAssayPipeline (custom writer logic)
        mock_writer = MagicMock()
        mock_writer.write_dataset_atomic.return_value = WriteResult(
            rows=1, artifacts=WriteArtifacts()
        )

        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["assay_chembl_id"]}},
            "io": {"writer": mock_writer},
        }
        pipeline = ChemblAssayPipeline(config)

        df = pd.DataFrame({"col": [1]})
        artifacts = WriteArtifacts(data_path=tmp_path / "test.csv")

        result = pipeline.save_results(df, artifacts, options)

        assert mock_writer.write_dataset_atomic.called
        assert result.rows == 1

