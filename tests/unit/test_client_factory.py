"""Unit tests for APIClientFactory."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bioetl.config import PipelineConfig
from bioetl.config.models import (
    HTTPClientConfig,
    HTTPConfig,
    PipelineMetadata,
    RetryConfig,
    SourceConfig,
)
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory


@pytest.fixture
def pipeline_config() -> PipelineConfig:
    """Sample PipelineConfig for testing."""
    return PipelineConfig(  # type: ignore[call-arg]
        version=1,
        pipeline=PipelineMetadata(  # type: ignore[call-arg]
            name="test_pipeline",
            version="1.0.0",
            description="Test pipeline",
        ),
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
        sources={
            "chembl": SourceConfig(  # type: ignore[call-arg,dict-item]
                enabled=True,
                parameters={"base_url": "https://www.ebi.ac.uk/chembl/api/data"},
            ),
            "test_source": SourceConfig(  # type: ignore[call-arg,dict-item]
                enabled=True,
                http_profile="fast",
                parameters={"base_url": "https://example.com/api"},
            ),
        },
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

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.build(base_url="https://example.com/api")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.args[0] == pipeline_config.http.default
            assert call_args.kwargs["base_url"] == "https://example.com/api"
            assert call_args.kwargs["name"] == "default"

    def test_build_with_source_name(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with source name."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.build(base_url="https://example.com/api", source="chembl")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.kwargs["name"] == "chembl"

    def test_build_with_profile(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with HTTP profile."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.build(base_url="https://example.com/api", profile="fast")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            # Should merge default and profile configs
            assert call_args.args[0].timeout_sec == 10.0  # From profile

    def test_build_with_overrides(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with config overrides."""
        factory = APIClientFactory(pipeline_config)
        overrides = HTTPClientConfig(timeout_sec=60.0)

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.build(base_url="https://example.com/api", overrides=overrides)

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            assert call_args.args[0].timeout_sec == 60.0  # From overrides

    def test_build_with_custom_name(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client with custom name."""
        factory = APIClientFactory(pipeline_config)

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.build(base_url="https://example.com/api", name="custom_client")

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

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.for_source("test_source", base_url="https://example.com/api")

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
        source_config.parameters = source_config.parameters or {}
        source_config.parameters["max_url_length"] = 2000

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.for_source("chembl", base_url="https://example.com/api")

            assert mock_client_class.called
            call_args = mock_client_class.call_args
            http_config = call_args.args[0]
            assert http_config.max_url_length == 2000

    def test_for_source_with_existing_http_overrides(self, pipeline_config: PipelineConfig) -> None:
        """Test building a client for source with existing HTTP overrides."""
        factory = APIClientFactory(pipeline_config)
        # Add HTTP overrides to source config
        source_config = pipeline_config.sources["chembl"]
        source_config.http = HTTPClientConfig(timeout_sec=45.0)
        source_config.parameters = source_config.parameters or {}
        source_config.parameters["max_url_length"] = 2000

        with patch("bioetl.core.client_factory.UnifiedAPIClient") as mock_client_class:
            mock_client = MagicMock(spec=UnifiedAPIClient)
            mock_client_class.return_value = mock_client

            client = factory.for_source("chembl", base_url="https://example.com/api")

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

