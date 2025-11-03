"""End-to-end smoke tests for CLI pipeline entrypoints."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import typer
import pytest
from typer.testing import CliRunner

from scripts import PIPELINE_REGISTRY
from bioetl.cli.command import create_pipeline_command

RUNNER = CliRunner()


class _DryRunStubPipeline:
    """Minimal pipeline used to exercise CLI wiring during dry-run."""

    def __init__(self, config: object, run_id: str) -> None:  # noqa: D401 - simple stub
        self.config = config
        self.run_id = run_id
        self.runtime_options: dict[str, object] = {}


_ENV_OVERRIDES = {
    "IUPHAR_API_KEY": "test-key",
    "PUBMED_API_KEY": "test-key",
    "SEMANTIC_SCHOLAR_API_KEY": "test-key",
}


@pytest.mark.parametrize("pipeline_key", sorted(PIPELINE_REGISTRY))
def test_cli_command_supports_dry_run(
    pipeline_key: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every registered pipeline CLI should support a dry-run execution."""

    for env_key, value in _ENV_OVERRIDES.items():
        monkeypatch.setenv(env_key, value)

    config = PIPELINE_REGISTRY[pipeline_key]
    stubbed_config = replace(
        config,
        pipeline_factory=lambda: _DryRunStubPipeline,
        default_input=None,
    )
    command = create_pipeline_command(stubbed_config)
    app = typer.Typer()
    app.command(name=pipeline_key)(command)

    args = [
        "--config",
        str(config.default_config),
        "--output-dir",
        str(tmp_path / pipeline_key),
        "--dry-run",
        "--no-validate-columns",
    ]

    if config.mode_choices:
        if config.default_mode in config.mode_choices:
            mode_value = config.default_mode
        else:
            mode_value = config.mode_choices[0]
        args.extend(["--mode", mode_value])

    result = RUNNER.invoke(app, args)

    assert result.exit_code == 0, result.stdout
    assert "[DRY-RUN] Configuration loaded successfully." in result.stdout
