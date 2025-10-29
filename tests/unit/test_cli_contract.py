from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
import sys

import pytest
from typer.testing import CliRunner

PROJECT_SRC = Path(__file__).resolve().parents[2] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))


@dataclass(frozen=True)
class EntryPoint:
    """Description of a pipeline CLI entry point."""

    name: str
    module: str
    pipeline_attr: str
    config_path: Path
    mode: str


ENTRYPOINTS = (
    EntryPoint(
        name="assay",
        module="scripts.run_assay",
        pipeline_attr="AssayPipeline",
        config_path=Path("configs/pipelines/assay.yaml"),
        mode="smoke",
    ),
    EntryPoint(
        name="activity",
        module="scripts.run_activity",
        pipeline_attr="ActivityPipeline",
        config_path=Path("configs/pipelines/activity.yaml"),
        mode="smoke",
    ),
    EntryPoint(
        name="testitem",
        module="scripts.run_testitem",
        pipeline_attr="TestItemPipeline",
        config_path=Path("configs/pipelines/testitem.yaml"),
        mode="smoke",
    ),
    EntryPoint(
        name="target",
        module="scripts.run_target",
        pipeline_attr="TargetPipeline",
        config_path=Path("configs/pipelines/target.yaml"),
        mode="smoke",
    ),
    EntryPoint(
        name="document",
        module="scripts.run_document",
        pipeline_attr="DocumentPipeline",
        config_path=Path("configs/pipelines/document.yaml"),
        mode="all",
    ),
)


@pytest.mark.unit
@pytest.mark.parametrize("entry", ENTRYPOINTS, ids=lambda entry: entry.name)
def test_cli_help_exposes_contract(entry: EntryPoint) -> None:
    """Each CLI advertises the shared contract in help output."""

    module = importlib.import_module(entry.module)
    runner = CliRunner()
    result = runner.invoke(module.app, ["--help"])

    assert result.exit_code == 0, result.output

    expected_flags = (
        "--config",
        "--golden",
        "--sample",
        "--fail-on-schema-drift",
        "--extended",
        "--mode",
        "--dry-run",
        "--verbose",
        "--set",
    )
    if entry.name == "target":
        expected_flags += ("--limit",)

    for flag in expected_flags:
        assert flag in result.stdout, f"{flag} missing from help for {entry.name}"


@pytest.mark.unit
@pytest.mark.parametrize("entry", ENTRYPOINTS, ids=lambda entry: entry.name)
def test_cli_overrides_propagate_to_pipeline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entry: EntryPoint) -> None:
    """CLI overrides and environment variables reach the pipeline configuration."""

    module = importlib.import_module(entry.module)

    captured: dict[str, object] = {}

    class RecordingPipeline:
        def __init__(self, config, run_id):  # noqa: D401, ANN001 - signature dictated by Typer wiring
            captured["config"] = config
            captured["run_id"] = run_id
            self.runtime_options: dict[str, object] = {}
            captured["runtime_options"] = self.runtime_options

    monkeypatch.setattr(module, entry.pipeline_attr, RecordingPipeline)

    golden_path = tmp_path / "golden.csv"
    golden_path.write_text("id\n1\n", encoding="utf-8")

    monkeypatch.setenv("BIOETL_CACHE__TTL", "321")

    runner = CliRunner()
    args = [
        "--config",
        str(entry.config_path),
        "--dry-run",
        "--verbose",
        "--extended",
        "--allow-schema-drift",
        "--sample",
        "5",
        "--golden",
        str(golden_path),
        "--mode",
        entry.mode,
        "--set",
        "cache.ttl=123",
        "--set",
        "qc.thresholds.null_fraction=0.05",
        "--set",
        "cli.custom_flag='contract'",
    ]

    if entry.name == "target":
        args.extend(["--limit", "7"])

    result = runner.invoke(module.app, args)

    assert result.exit_code == 0, result.output

    config = captured.get("config")
    assert config is not None, "Pipeline did not receive configuration"

    cache_ttl = getattr(config, "cache").ttl
    assert cache_ttl == 321  # environment overrides CLI --set value

    qc_thresholds = getattr(config, "qc").thresholds
    assert qc_thresholds["null_fraction"] == pytest.approx(0.05)

    cli_section = getattr(config, "cli")
    assert cli_section["custom_flag"] == "contract"
    assert cli_section["extended"] is True
    assert cli_section["mode"] == entry.mode
    assert cli_section["fail_on_schema_drift"] is False
    assert cli_section["dry_run"] is True
    assert cli_section["verbose"] is True
    assert cli_section["sample"] == 5
    assert cli_section["golden"] == str(golden_path)
    if entry.name == "target":
        assert cli_section["limit"] == 7

    run_id = captured.get("run_id")
    assert isinstance(run_id, str) and run_id.startswith(entry.name)

    runtime_options = captured.get("runtime_options")
    assert isinstance(runtime_options, dict)
    if entry.name == "target":
        assert runtime_options.get("sample") == 5
        assert runtime_options.get("limit") == 7
    else:
        assert runtime_options.get("sample") == 5
        assert runtime_options.get("limit") == 5

