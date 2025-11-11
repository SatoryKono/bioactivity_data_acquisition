"""Integration tests for CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest  # type: ignore[reportMissingImports]
from click.testing import CliRunner  # type: ignore[reportMissingImports]
from typer.main import get_command  # type: ignore[reportMissingImports]

from bioetl.cli.app import app  # type: ignore[reportUnknownVariableType]

CLI_APP = get_command(app)  # type: ignore[reportUnknownVariableType]


@pytest.mark.integration  # type: ignore[reportUntypedClassDecorator,reportUnknownMemberType]
class TestCLIIntegration:
    """Test suite for CLI integration."""

    def test_activity_command_integration(self, tmp_path: Path) -> None:
        """Test full activity command integration."""
        runner: CliRunner = CliRunner()  # type: ignore[reportUnknownVariableType]

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
version: 1
pipeline:
  name: activity_chembl
  version: "1.0.0"
http:
  default:
    timeout_sec: 30.0
    connect_timeout_sec: 10.0
    read_timeout_sec: 30.0
sources:
  chembl:
    enabled: true
    parameters:
      base_url: "https://www.ebi.ac.uk/chembl/api/data"
validation:
  schema_out: "bioetl.schemas.activity.activity_chembl:ActivitySchema"
determinism:
  sort:
    by: ["activity_id"]
    ascending: [True]
  hashing:
    business_key_fields: ["activity_id"]
"""
        )

        output_dir = tmp_path / "output"

        # Mock load_config to avoid profile resolution issues in tests
        with (
            patch("bioetl.config.load_config") as mock_load_config,
            patch("bioetl.core.client_factory.APIClientFactory.for_source") as mock_factory,
        ):
            from pathlib import Path as PathType

            from bioetl.config.loader import load_config as real_load_config

            # Load config without default profiles for test
            # Use absolute path to avoid resolution issues
            abs_config_path = PathType(config_file).resolve()
            try:
                real_config = real_load_config(
                    config_path=abs_config_path,
                    include_default_profiles=False,
                )
            except Exception:
                # If config loading fails, create a minimal config manually
                from bioetl.config.models import (
                    CLIConfig,
                    DeterminismConfig,
                    DeterminismHashingConfig,
                    DeterminismSortingConfig,
                    HTTPClientConfig,
                    HTTPConfig,
                    MaterializationConfig,
                    PipelineConfig,
                    PipelineMetadata,
                    RetryConfig,
                    SourceConfig,
                    ValidationConfig,
                )

                real_config = PipelineConfig(
                    version=1,
                    pipeline=PipelineMetadata(
                        name="activity_chembl",
                        version="1.0.0",
                        description="Test activity pipeline",
                    ),
                    http=HTTPConfig(
                        default=HTTPClientConfig(
                            timeout_sec=30.0,
                            connect_timeout_sec=10.0,
                            read_timeout_sec=30.0,
                            retries=RetryConfig(total=3, backoff_multiplier=2.0, backoff_max=10.0),
                        ),
                    ),
                    materialization=MaterializationConfig(root=str(output_dir)),
                    determinism=DeterminismConfig(
                        sort=DeterminismSortingConfig(
                            by=["activity_id"],
                            ascending=[True],
                        ),
                        hashing=DeterminismHashingConfig(
                            business_key_fields=("activity_id",),
                        ),
                    ),
                    validation=ValidationConfig(
                        schema_out="bioetl.schemas.activity.activity_chembl:ActivitySchema",
                        strict=True,
                        coerce=True,
                    ),
                    sources={
                        "chembl": SourceConfig(
                            enabled=True,
                            parameters={"base_url": "https://www.ebi.ac.uk/chembl/api/data"},
                        ),
                    },
                    cli=CLIConfig(),
                )
            mock_load_config.return_value = real_config
            mock_client = MagicMock()
            mock_status_response = MagicMock()
            mock_status_response.json.return_value = {"chembl_release": "33"}
            mock_status_response.status_code = 200
            mock_status_response.headers = {}

            mock_activity_response = MagicMock()
            mock_activity_response.json.return_value = {
                "page_meta": {"offset": 0, "limit": 25, "count": 0, "next": None},
                "activities": [],
            }
            mock_activity_response.status_code = 200
            mock_activity_response.headers = {}

            mock_client.get.side_effect = [mock_status_response, mock_activity_response]
            mock_factory.return_value = mock_client

            result: Any = runner.invoke(
                CLI_APP,  # type: ignore[reportUnknownArgumentType]
                [
                    "activity_chembl",
                    "--config",
                    str(config_file),
                    "--output-dir",
                    str(output_dir),
                ],
            )

            assert result.exit_code == 0, (  # type: ignore[reportUnknownMemberType]
                f"Expected exit code 0, got {result.exit_code}. Stdout: {result.stdout}, Stderr: {result.stderr}"  # type: ignore[reportUnknownMemberType]
            )
            assert "completed successfully" in result.stdout  # type: ignore[reportUnknownMemberType]

    def test_activity_command_exit_code_on_error(self, tmp_path: Path) -> None:
        """Test that CLI returns correct exit code on error."""
        runner: CliRunner = CliRunner()  # type: ignore[reportUnknownVariableType]

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
version: 1
pipeline:
  name: activity_chembl
  version: "1.0.0"
http:
  default:
    timeout_sec: 30.0
    connect_timeout_sec: 10.0
    read_timeout_sec: 30.0
"""
        )

        output_dir = tmp_path / "output"

        # Mock load_config to avoid profile resolution issues in tests
        with (
            patch("bioetl.config.load_config") as mock_load_config,
            patch("bioetl.pipelines.chembl.activity.run.ChemblActivityPipeline.run") as mock_run,
        ):
            from bioetl.config.loader import load_config as real_load_config

            # Load config without default profiles for test
            real_config = real_load_config(
                config_path=config_file,
                include_default_profiles=False,
            )
            mock_load_config.return_value = real_config

            # Mock pipeline.run() to raise an error
            mock_run.side_effect = ValueError("Pipeline error")

            result: Any = runner.invoke(
                CLI_APP,  # type: ignore[reportUnknownArgumentType]
                [
                    "activity_chembl",
                    "--config",
                    str(config_file),
                    "--output-dir",
                    str(output_dir),
                ],
            )

            # Exit code 1 for pipeline errors (ValueError is not an API error)
            assert result.exit_code == 1  # type: ignore[reportUnknownMemberType]
            # Check for error message in either stdout or stderr
            error_output = (result.stdout + result.stderr).lower()  # type: ignore[reportUnknownMemberType]
            assert "failed" in error_output or "error" in error_output  # type: ignore[reportUnknownMemberType]
