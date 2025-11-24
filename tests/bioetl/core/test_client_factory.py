"""Unit tests for APIClientFactory."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bioetl.config.models.base import PipelineMetadata
from bioetl.config.models.http import HTTPClientConfig, HTTPConfig, RetryConfig
from bioetl.config.models.models import (
    PipelineConfig,
    PipelineDomainConfig,
    PipelineInfrastructureConfig,
)
from bioetl.config.models.source import SourceConfig, SourceParameters
from bioetl.core import APIClientFactory, UnifiedAPIClient


@pytest.fixture
def pipeline_config() -> PipelineConfig:
    """Sample PipelineConfig for testing."""
    infrastructure = PipelineInfrastructureConfig(
        http=HTTPConfig(
            default=HTTPClientConfig(
                timeout_sec=30.0,
                connect_timeout_sec=10.0,
                read_timeout_sec=30.0,
                retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
            ),
            profiles={
                "fast": HTTPClientConfig(
                    timeout_sec=10.0,
                    connect_timeout_sec=5.0,
                    read_timeout_sec=10.0,
                ),
            },
        ),
    )
    domain = PipelineDomainConfig(
        sources={
            "chembl": SourceConfig(
                enabled=True,
                parameters=SourceParameters.from_mapping(
                    {"base_url": "https://www.ebi.ac.uk/chembl/api/data"}
                ),
            ),
            "test_source": SourceConfig(
                enabled=True,
                http_profile="fast",
                parameters=SourceParameters.from_mapping({"base_url": "https://example.com/api"}),
            ),
        },
    )
    return PipelineConfig(
        version=1,
        pipeline=PipelineMetadata(
            name="test_pipeline",
            version="1.0.0",
            description="Test pipeline",
        ),
        domain=domain,
        infrastructure=infrastructure,
    )


@pytest.mark.unit
class TestAPIClientFactory:
    """Test suite for APIClientFactory."""

    def test_init(self, pipeline_config: PipelineConfig) -> None:
        """Test APIClientFactory initialization."""
        factory = APIClientFactory(pipeline_config)
        assert factory._config == pipeline_config

    def test_build_with_default_config(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with default configuration."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.build(base_url="https://example.com/api")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.args[0] == pipeline_config.http.default
            assert call_args.kwargs["base_url"] == "https://example.com/api"
            assert call_args.kwargs["name"] == "default"

    def test_build_with_source_name(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with source name."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.build(base_url="https://example.com/api", source="chembl")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.kwargs["name"] == "chembl"

    def test_build_with_profile(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with HTTP profile."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.build(base_url="https://example.com/api", profile="fast")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            # Should merge default and profile configs
            assert call_args.args[0].timeout_sec == 10.0  # From profile

    def test_build_with_overrides(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with config overrides."""
        factory = APIClientFactory(pipeline_config)
        overrides = HTTPClientConfig(timeout_sec=60.0)

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.build(base_url="https://example.com/api", overrides=overrides)

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.args[0].timeout_sec == 60.0  # From overrides

    def test_build_with_custom_name(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with custom name."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.build(base_url="https://example.com/api", name="custom_client")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.kwargs["name"] == "custom_client"

    def test_build_with_unknown_profile(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with unknown profile raises error."""
        factory = APIClientFactory(pipeline_config)

        with pytest.raises(KeyError, match="Unknown HTTP profile"):
            factory.build(base_url="https://example.com/api", profile="unknown_profile")

    def test_for_source(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client for a specific source."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.for_source("test_source", base_url="https://example.com/api")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.kwargs["name"] == "test_source"
            # Should use profile from source config
            assert call_args.args[0].timeout_sec == 10.0  # From fast profile

    def test_for_source_with_max_url_length(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client for source with max_url_length parameter."""
        factory = APIClientFactory(pipeline_config)
        # Add max_url_length to source parameters
        source_config = pipeline_config.sources["chembl"]
        updated_parameters = source_config.parameters.model_copy(update={"max_url_length": 2000})
        pipeline_config.sources["chembl"] = source_config.model_copy(
            update={"parameters": updated_parameters}
        )

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.for_source("chembl", base_url="https://example.com/api")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            http_config = call_args.args[0]
            assert http_config.max_url_length == 2000

    def test_for_source_with_existing_http_overrides(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client for source with existing HTTP overrides."""
        factory = APIClientFactory(pipeline_config)
        # Add HTTP overrides to source config
        source_config = pipeline_config.sources["chembl"]
        updated_parameters = source_config.parameters.model_copy(update={"max_url_length": 2000})
        pipeline_config.sources["chembl"] = source_config.model_copy(
            update={
                "http": HTTPClientConfig(timeout_sec=45.0),
                "parameters": updated_parameters,
            }
        )

        with patch("bioetl.core.http.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            _client = factory.for_source("chembl", base_url="https://example.com/api")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            http_config = call_args.args[0]
            assert http_config.timeout_sec == 45.0  # From source http override
            assert http_config.max_url_length == 2000  # From parameters

    def test_for_source_unknown_source(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client for unknown source raises error."""
        factory = APIClientFactory(pipeline_config)

        with pytest.raises(KeyError, match="Unknown source"):
            factory.for_source("unknown_source", base_url="https://example.com/api")
