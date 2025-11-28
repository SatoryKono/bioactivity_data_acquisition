"""Tests for ChemblCommonPipeline and ChemblWriteService."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from bioetl.core.pipeline.types import StageExecutionOptions, WriteArtifacts, WriteResult
from bioetl.core.pipeline.unified import ChemblExtractionServiceDescriptor, ChemblPipelineBase
from bioetl.pipelines.chembl.common.base import (
    ChemblCommonPipeline,
    ChemblWriteService,
    ConfigValidationError,
)
from bioetl.pipelines.chembl.common.chembl_extraction_service import (
    ChemblExtractionService,
)
from bioetl.pipelines.chembl.common.strategies import ExtractionStrategyFactory


class TestChemblWriteService:
    """Test suite for ChemblWriteService."""

    def test_init(self) -> None:
        """Test ChemblWriteService initialization."""
        mock_pipeline = MagicMock()
        mock_pipeline.entity_name = "test"
        mock_pipeline.run_id = "run-123"
        mock_pipeline.pipeline_name = "test_pipeline"

        service = ChemblWriteService(mock_pipeline)
        assert service.pipeline == mock_pipeline

    @patch("bioetl.pipelines.chembl.common.base.Path")
    def test_save_dry_run(self, mock_path: MagicMock) -> None:
        """Test save method in dry run mode."""
        mock_pipeline = MagicMock()
        mock_pipeline.entity_name = "activity"
        mock_pipeline.run_id = "run-123"
        mock_pipeline.pipeline_name = "activity_pipeline"
        mock_pipeline._write_quality_report = MagicMock()

        service = ChemblWriteService(mock_pipeline)
        df = pd.DataFrame([{"id": 1, "value": "test"}])
        
        artifacts = MagicMock()
        artifacts.data_path = Path("test.csv")
        artifacts.quality_report_path = Path("quality.csv")
        artifacts.meta_path = Path("meta.yaml")
        artifacts.manifest_path = Path("manifest.json")
        artifacts.extra = {}

        options = MagicMock()
        options.dry_run = True
        options.extended = False

        context = MagicMock()
        context.output_dir = Path("/output")
        runtime = MagicMock()

        result = service.save(df, artifacts, options, context=context, runtime=runtime)

        assert isinstance(result, WriteResult)
        assert result.rows == 1
        # In dry run, files should not be written
        mock_pipeline._write_quality_report.assert_not_called()

    @patch("bioetl.pipelines.chembl.common.base.Path")
    def test_save_wet_run(self, mock_path: MagicMock) -> None:
        """Test save method in wet run mode."""
        mock_pipeline = MagicMock()
        mock_pipeline.entity_name = "activity"
        mock_pipeline.run_id = "run-123"
        mock_pipeline.pipeline_name = "activity_pipeline"
        mock_pipeline._write_quality_report = MagicMock()

        service = ChemblWriteService(mock_pipeline)
        df = pd.DataFrame([{"id": 1, "value": "test"}])
        
        artifacts = MagicMock()
        artifacts.data_path = Path("test.csv")
        artifacts.quality_report_path = Path("quality.csv")
        artifacts.meta_path = Path("meta.yaml")
        artifacts.manifest_path = Path("manifest.json")
        artifacts.extra = {}

        options = MagicMock()
        options.dry_run = False
        options.extended = False

        context = MagicMock()
        context.output_dir = Path("/output")
        runtime = MagicMock()

        # Mock file operations
        mock_file = MagicMock()
        mock_path.return_value = mock_file
        mock_file.mkdir = MagicMock()
        mock_file.write_text = MagicMock()
        df.to_csv = MagicMock()

        result = service.save(df, artifacts, options, context=context, runtime=runtime)

        assert isinstance(result, WriteResult)
        assert result.rows == 1
        mock_pipeline._write_quality_report.assert_called_once()

    def test_write_metadata_compatibility(self) -> None:
        """Test write_metadata method for compatibility."""
        mock_pipeline = MagicMock()
        service = ChemblWriteService(mock_pipeline)
        
        # Should not raise and should return None
        result = service.write_metadata(Path("/out"), MagicMock(), None, dry_run=True)
        assert result is None


class TestChemblCommonPipeline:
    """Test suite for ChemblCommonPipeline."""

    def test_init_minimal(self) -> None:
        """Test minimal initialization."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test-123")
        
        assert pipeline.entity_name == "chembl"
        assert pipeline.run_id == "test-123"
        assert pipeline.write_service is not None

    def test_init_with_custom_extraction_service(self) -> None:
        """Test initialization with custom extraction service."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        extraction_service = MagicMock(spec=ChemblExtractionService)
        pipeline = ChemblCommonPipeline(
            config, 
            run_id="test-123",
            extraction_service=extraction_service
        )
        
        assert pipeline.extraction_service == extraction_service

    def test_validate_config_invalid_batch_size(self) -> None:
        """Test config validation with invalid batch size."""
        config = {
            "sources": {"chembl": {"batch_size": -1}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        with pytest.raises(ConfigValidationError, match="batch_size must be integer"):
            ChemblCommonPipeline(config)

    def test_validate_config_invalid_max_url_length(self) -> None:
        """Test config validation with invalid max URL length."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": -1}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        with pytest.raises(ConfigValidationError, match="max_url_length must be integer"):
            ChemblCommonPipeline(config)

    def test_validate_config_invalid_namespace(self) -> None:
        """Test config validation with invalid cache namespace."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": ""},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        with pytest.raises(ConfigValidationError, match="namespace must be non-empty"):
            ChemblCommonPipeline(config)

    def test_validate_config_invalid_sort_by(self) -> None:
        """Test config validation with invalid sort by configuration."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": "invalid"}},
        }
        
        with pytest.raises(ConfigValidationError, match="sort.by must be a list"):
            ChemblCommonPipeline(config)

    def test_validate_config_missing_required_sort_fields(self) -> None:
        """Test config validation with missing required sort fields."""
        class TestPipeline(ChemblCommonPipeline):
            entity_name = "test"
            required_sort_fields = ["required_field"]
        
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["other_field"]}},
        }
        
        with pytest.raises(ConfigValidationError, match="missing required fields"):
            TestPipeline(config)

    def test_get_config_value_success(self) -> None:
        """Test successful config value retrieval."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
            "level1": {"level2": {"value": "test"}}
        }
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        result = pipeline._get_config_value("level1.level2.value")
        assert result == "test"

    def test_get_config_value_missing_key(self) -> None:
        """Test config value retrieval with missing key."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
            "level1": {"level2": {}}
        }
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        with pytest.raises(ConfigValidationError, match="Missing configuration key"):
            pipeline._get_config_value("level1.level2.missing")

    def test_get_config_value_invalid_type(self) -> None:
        """Test config value retrieval with invalid intermediate type."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
            "level1": "not_a_mapping"
        }
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        with pytest.raises(ConfigValidationError, match="Missing configuration key"):
            pipeline._get_config_value("level1.level2.value")

    def test_extract_dry_run_with_validation(self) -> None:
        """Test extract method in dry run mode with validation service."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        pipeline.validation_service = MagicMock()
        pipeline.validation_service.empty_frame.return_value = pd.DataFrame([{"id": 1}])
        
        options = MagicMock()
        options.dry_run = True
        
        result = pipeline.extract(None, options)

        assert not result.empty
        pipeline.validation_service.empty_frame.assert_called_once()

    def test_extract_uses_injected_strategy_factory(self) -> None:
        """Ensure extract delegates to provided strategy factory."""

        class DummyStrategy:
            def __init__(self) -> None:
                self.called = False

            def supports(self, descriptor_type: str) -> bool:
                return True

            def run(self, pipeline, descriptor, options):
                self.called = True
                assert pipeline._descriptor_type == "service"
                assert descriptor is None
                assert options is options_mock
                return pd.DataFrame([{"id": 1}])

        dummy_strategy = DummyStrategy()
        factory = MagicMock(spec=ExtractionStrategyFactory)
        factory.get.return_value = dummy_strategy

        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }

        options_mock = MagicMock()
        options_mock.dry_run = False

        pipeline = ChemblCommonPipeline(
            config,
            run_id="test",
            extraction_strategy_factory=factory,
        )

        result = pipeline.extract(None, options_mock)

        factory.get.assert_called_once_with("service")
        assert dummy_strategy.called
        assert not result.empty

    def test_transform(self) -> None:
        """Test transform method."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        pipeline.pre_transform = MagicMock()
        pipeline.domain_enrich = MagicMock()
        
        df = pd.DataFrame([{"id": 1}])
        options = MagicMock()
        
        result = pipeline.transform(df, options)
        
        pipeline.pre_transform.assert_called_once_with(df)
        pipeline.domain_enrich.assert_called_once()

    def test_validate_with_service(self) -> None:
        """Test validate method with validation service."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        pipeline.validation_service = MagicMock()
        pipeline.validation_service.validate.return_value = pd.DataFrame([{"id": 1}])
        
        df = pd.DataFrame([{"id": 1}])
        options = MagicMock()
        
        result = pipeline.validate(df, options)
        
        pipeline.validation_service.validate.assert_called_once_with(
            df, pipeline=pipeline, options=options
        )

    def test_validate_without_service(self) -> None:
        """Test validate method without validation service."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        pipeline.validation_service = None
        
        df = pd.DataFrame([{"id": 1}])
        options = MagicMock()
        
        result = pipeline.validate(df, options)
        
        # Should return the same DataFrame when no validation service
        assert result.equals(df)

    def test_save_results_delegation(self) -> None:
        """Test save_results method delegates to parent."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        with patch.object(pipeline.__class__.__bases__[0], 'save_results') as mock_save:
            df = pd.DataFrame([{"id": 1}])
            artifacts = MagicMock()
            options = MagicMock()
            
            pipeline.save_results(df, artifacts, options)
            mock_save.assert_called_once_with(df, artifacts, options)

    def test_write_quality_report(self) -> None:
        """Test quality report writing."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        df = pd.DataFrame([{"id": 1, "value": None}])
        output_path = Path("/tmp/quality_report.csv")
        
        with patch.object(pd.DataFrame, 'to_csv') as mock_to_csv:
            pipeline._write_quality_report(df, output_path)
            mock_to_csv.assert_called_once_with(output_path, index=False)

    def test_fallback_rows(self) -> None:
        """Test fallback row generation."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        ids = ["CHEMBL1", "CHEMBL2"]
        exc = Exception("test error")
        
        result = pipeline._fallback_rows(ids, exc)
        
        assert len(result) == 2
        assert all(row["chembl_id"] in ids for row in result)
        assert all(row["error_code"] == "extract_failed" for row in result)
        assert all(row["error_message"] == "test error" for row in result)

    def test_build_generic_descriptor(self) -> None:
        """Test generic descriptor building."""
        config = {
            "sources": {
                "chembl": {
                    "batch_size": 10,
                    "max_url_length": 1000,
                    "client": "mock_client",
                    "activity_fetcher": MagicMock()
                }
            },
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        descriptor = pipeline._build_generic_descriptor()
        
        assert isinstance(descriptor, ChemblExtractionServiceDescriptor)
        assert callable(descriptor.build_context)
        assert callable(descriptor.fetcher_factory)
        assert callable(descriptor.finalizer_factory)

    def test_build_descriptor_not_implemented(self) -> None:
        """Test build_descriptor raises NotImplementedError."""
        config = {
            "sources": {"chembl": {"batch_size": 10, "max_url_length": 1000}},
            "cache": {"namespace": "test"},
            "determinism": {"sort": {"by": ["id"]}},
        }
        
        pipeline = ChemblCommonPipeline(config, run_id="test")
        
        with pytest.raises(NotImplementedError):
            pipeline.build_descriptor()
