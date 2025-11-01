from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from typer.testing import CliRunner

PROJECT_SRC = Path(__file__).resolve().parents[2] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from bioetl.cli.main import app as main_cli_app  # noqa: E402
from bioetl.core.output_writer import OutputArtifacts  # noqa: E402
from scripts import PIPELINE_COMMAND_REGISTRY, PIPELINE_REGISTRY  # noqa: E402


@dataclass(frozen=True)
class EntryPoint:
    """Description of a pipeline CLI entry point."""

    name: str
    legacy_name: str
    module: str
    pipeline_attr: str
    config_path: Path
    mode: str


_PIPELINE_ENTRYPOINTS: dict[str, tuple[str, str]] = {
    "chembl_assay": ("assay", "scripts.run_assay"),
    "chembl_activity": ("activity", "scripts.run_activity"),
    "chembl_testitem": ("testitem", "scripts.run_testitem"),
    "chembl_target": ("target", "scripts.run_target"),
    "chembl_document": ("document", "scripts.run_document"),
    "iuphar_target": ("gtp_iuphar", "scripts.run_gtp_iuphar"),
    "uniprot_protein": ("uniprot", "scripts.run_uniprot"),
}


def _build_registry_entrypoints() -> list[EntryPoint]:
    entries: list[EntryPoint] = []
    for name in (
        "chembl_assay",
        "chembl_activity",
        "chembl_testitem",
        "chembl_target",
        "chembl_document",
        "iuphar_target",
        "uniprot_protein",
    ):
        legacy_name, module_path = _PIPELINE_ENTRYPOINTS[name]
        config = PIPELINE_REGISTRY[name]
        pipeline_cls = config.pipeline_factory()
        default_mode = config.default_mode
        mode = "smoke" if default_mode == "default" else default_mode
        entries.append(
            EntryPoint(
                name=name,
                legacy_name=legacy_name,
                module=module_path,
                pipeline_attr=pipeline_cls.__name__,
                config_path=config.default_config,
                mode=mode,
            )
        )

    return entries


ENTRYPOINTS = tuple(_build_registry_entrypoints())


@pytest.mark.unit
def test_cli_list_command_reflects_registry() -> None:
    """The aggregated CLI lists every registered pipeline with descriptions."""

    runner = CliRunner()
    result = runner.invoke(main_cli_app, ["list"])

    assert result.exit_code == 0, result.output

    for key, config in sorted(PIPELINE_REGISTRY.items()):
        description = config.description or config.pipeline_name
        expected_line = f"  - {key} ({description})"
        assert expected_line in result.stdout


def _import_entry_module(entry: EntryPoint) -> ModuleType:
    sys.modules.pop(entry.module, None)
    return importlib.import_module(entry.module)


def _load_entry_module(
    entry: EntryPoint,
    monkeypatch: pytest.MonkeyPatch | None = None,
    pipeline_override: type | None = None,
) -> ModuleType:
    if (
        monkeypatch is not None
        and pipeline_override is not None
        and entry.legacy_name in PIPELINE_COMMAND_REGISTRY
    ):
        original = PIPELINE_COMMAND_REGISTRY[entry.legacy_name]
        monkeypatch.setitem(
            PIPELINE_COMMAND_REGISTRY,
            entry.legacy_name,
            replace(original, pipeline_factory=lambda: pipeline_override),
        )

    return _import_entry_module(entry)


@pytest.mark.unit
@pytest.mark.parametrize("entry", ENTRYPOINTS, ids=lambda entry: entry.name)
def test_cli_help_exposes_contract(entry: EntryPoint) -> None:
    """Each CLI advertises the shared contract in help output."""

    module = _load_entry_module(entry)
    runner = CliRunner()
    result = runner.invoke(module.app, ["--help"])

    assert result.exit_code == 0, result.output

    expected_flags = (
        "--config",
        "--golden",
        "--sample",
        "--limit",
        "--fail-on-schema-drift",
        "--extended",
        "--mode",
        "--dry-run",
        "--verbose",
        "--set",
    )

    for flag in expected_flags:
        assert flag in result.stdout, f"{flag} missing from help for {entry.name}"

    if entry.legacy_name != "target":
        assert "Legacy alias for --sample" in result.stdout
        assert "Process only the first N records for smoke testing (preferred)" in result.stdout


@pytest.mark.unit
@pytest.mark.parametrize("entry", ENTRYPOINTS, ids=lambda entry: entry.name)
def test_cli_overrides_propagate_to_pipeline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entry: EntryPoint) -> None:
    """CLI overrides and environment variables reach the pipeline configuration."""

    captured: dict[str, object] = {}

    class RecordingPipeline:
        def __init__(self, config, run_id):  # noqa: D401, ANN001 - signature dictated by Typer wiring
            captured["config"] = config
            captured["run_id"] = run_id
            self.runtime_options: dict[str, object] = {}
            captured["runtime_options"] = self.runtime_options

    module = _load_entry_module(entry, monkeypatch, pipeline_override=RecordingPipeline)

    monkeypatch.setattr(module, entry.pipeline_attr, RecordingPipeline, raising=False)

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

    if entry.legacy_name == "target":
        args.extend(["--limit", "7"])
    else:
        args.extend(["--limit", "5"])

    if entry.legacy_name == "document":
        monkeypatch.setenv("PUBMED_API_KEY", "contract")
        monkeypatch.setenv("SEMANTIC_SCHOLAR_API_KEY", "contract")
    if entry.legacy_name == "gtp_iuphar":
        monkeypatch.setenv("IUPHAR_API_KEY", "contract")

    result = runner.invoke(module.app, args)

    assert result.exit_code == 0, result.output

    config = captured.get("config")
    assert config is not None, "Pipeline did not receive configuration"

    cache_ttl = config.cache.ttl
    assert cache_ttl == 321  # environment overrides CLI --set value

    qc_thresholds = config.qc.thresholds
    assert qc_thresholds["null_fraction"] == pytest.approx(0.05)

    cli_section = config.cli
    assert cli_section["custom_flag"] == "contract"
    assert cli_section["extended"] is True
    assert cli_section["mode"] == entry.mode
    assert cli_section["fail_on_schema_drift"] is False
    assert cli_section["dry_run"] is True
    assert cli_section["verbose"] is True
    assert cli_section["sample"] == 5
    assert cli_section["golden"] == str(golden_path)
    if entry.legacy_name == "target":
        assert cli_section["limit"] == 7
    else:
        assert cli_section["limit"] == 5

    run_id = captured.get("run_id")
    assert isinstance(run_id, str) and run_id.startswith(entry.legacy_name)

    runtime_options = captured.get("runtime_options")
    assert isinstance(runtime_options, dict)
    assert runtime_options.get("sample") == 5
    assert runtime_options.get("limit") == 5


@pytest.mark.unit
def test_cli_sample_limit_applies_and_does_not_leak_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Limiting one invocation should not affect subsequent runs."""

    entry = _get_entry_by_name("assay")

    pipelines: list[object] = []

    class _LimitAwarePipeline:
        def __init__(self, config, run_id):  # noqa: D401, ANN001 - Typer wiring
            self.config = config
            self.run_id = run_id
            self.runtime_options: dict[str, object] = {}
            self.source_rows: int | None = None
            self.limited_rows: int | None = None
            pipelines.append(self)

        def extract(self, *args, **kwargs):  # noqa: ANN001 - interface defined by PipelineBase
            df = pd.DataFrame({"value": range(10)})
            self.source_rows = len(df)
            return df

        def run(
            self,
            output_path: Path,
            *,
            extended: bool = False,
            input_file: Path | None = None,
            **kwargs,
        ) -> OutputArtifacts:  # noqa: ANN003 - signature mirrors pipeline contract
            df = self.extract()
            self.limited_rows = len(df)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                "value\n" + "\n".join(str(value) for value in df["value"]),
                encoding="utf-8",
            )
            quality_path = output_path.parent / "quality_report.csv"
            quality_path.write_text("metric,value\nrows,1\n", encoding="utf-8")
            return OutputArtifacts(
                dataset=output_path,
                quality_report=quality_path,
                run_directory=output_path.parent,
            )

    module = _load_entry_module(entry, monkeypatch, pipeline_override=_LimitAwarePipeline)
    monkeypatch.setattr(module, entry.pipeline_attr, _LimitAwarePipeline, raising=False)

    runner = CliRunner()
    limited_output = tmp_path / "limited"
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--output-dir",
            str(limited_output),
            "--sample",
            "3",
            "--no-validate-columns",
        ],
    )

    assert result.exit_code == 0, result.output
    first_pipeline = pipelines.pop(0)
    assert isinstance(first_pipeline, _LimitAwarePipeline)
    assert first_pipeline.runtime_options.get("limit") == 3
    assert first_pipeline.runtime_options.get("sample") == 3
    assert first_pipeline.source_rows == 10
    assert first_pipeline.limited_rows == 3

    second_output = tmp_path / "default"
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--output-dir",
            str(second_output),
            "--no-validate-columns",
        ],
    )

    assert result.exit_code == 0, result.output
    second_pipeline = pipelines.pop(0)
    assert isinstance(second_pipeline, _LimitAwarePipeline)
    assert second_pipeline.runtime_options.get("limit") is None
    assert second_pipeline.runtime_options.get("sample") is None
    assert second_pipeline.source_rows == 10
    assert second_pipeline.limited_rows == 10


@pytest.mark.unit
def test_cli_rejects_mismatched_limit_and_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Providing conflicting limit/sample values should surface an error."""

    entry = _get_entry_by_name("assay")
    module = _load_entry_module(entry, monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--dry-run",
            "--sample",
            "3",
            "--limit",
            "4",
        ],
    )

    assert result.exit_code != 0
    assert "--sample and --limit must match" in result.stderr


@pytest.mark.unit
def test_cli_limit_alias_emits_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Using the legacy --limit flag should guide the user towards --sample."""

    entry = _get_entry_by_name("assay")
    module = _load_entry_module(entry, monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--dry-run",
            "--limit",
            "5",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Hint: --limit is deprecated; prefer --sample for new workflows." in result.stderr

@pytest.mark.unit
def test_cli_main_registers_pipeline_commands() -> None:
    """The consolidated CLI registers each pipeline command alongside list."""

    registered_command_names = {command.name for command in main_cli_app.registered_commands}
    expected_names = set(PIPELINE_COMMAND_REGISTRY) | set(PIPELINE_REGISTRY) | {"list"}

    missing = expected_names - registered_command_names
    assert not missing, f"Missing commands from main CLI: {sorted(missing)}"


@pytest.mark.unit
def test_cli_main_help_lists_pipeline_commands() -> None:
    """Help output from the consolidated CLI exposes the pipeline commands."""

    runner = CliRunner()
    result = runner.invoke(main_cli_app, ["--help"])

    assert result.exit_code == 0, result.output

    all_commands = set(PIPELINE_COMMAND_REGISTRY) | set(PIPELINE_REGISTRY)
    for command_name in sorted(all_commands):
        assert command_name in result.stdout

    assert "list" in result.stdout


@pytest.mark.unit
@pytest.mark.parametrize("entry", ENTRYPOINTS, ids=lambda entry: entry.name)
def test_cli_default_behaviour(monkeypatch: pytest.MonkeyPatch, entry: EntryPoint) -> None:
    """Default CLI invocation applies expected pipeline-specific flags."""

    captured: dict[str, object] = {}

    class RecordingPipeline:
        def __init__(self, config, run_id):  # noqa: D401, ANN001 - Typer contract
            captured["config"] = config
            captured["run_id"] = run_id
            self.runtime_options: dict[str, object] = {}
            captured["runtime_options"] = self.runtime_options

    module = _load_entry_module(entry, monkeypatch, pipeline_override=RecordingPipeline)

    monkeypatch.setattr(module, entry.pipeline_attr, RecordingPipeline, raising=False)

    if entry.legacy_name == "target":
        monkeypatch.setenv("IUPHAR_API_KEY", "contract")
    if entry.legacy_name == "document":
        monkeypatch.setenv("PUBMED_API_KEY", "contract")
        monkeypatch.setenv("SEMANTIC_SCHOLAR_API_KEY", "contract")

    runner = CliRunner()
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output

    config = captured.get("config")
    assert config is not None, "Pipeline did not receive configuration"

    if entry.legacy_name == "document":
        assert config.cli.get("mode") == "all"

    if entry.legacy_name == "target":
        stages = config.cli.get("stages", {})
        assert stages.get("uniprot") is True
        assert stages.get("iuphar") is True

        runtime_options = captured.get("runtime_options")
        assert isinstance(runtime_options, dict)
        assert runtime_options.get("with_uniprot") is True
        assert runtime_options.get("with_iuphar") is True

@pytest.mark.unit
def test_target_cli_auto_disables_iuphar_when_api_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Target CLI should disable IUPHAR stage when API key is absent."""

    entry = next(ep for ep in ENTRYPOINTS if ep.name == "chembl_target")

    captured: dict[str, object] = {}

    class RecordingPipeline:
        def __init__(self, config, run_id):  # noqa: D401, ANN001 - Typer contract
            captured["config"] = config
            captured["run_id"] = run_id
            self.runtime_options: dict[str, object] = {}
            captured["runtime_options"] = self.runtime_options

    monkeypatch.delenv("IUPHAR_API_KEY", raising=False)

    module = _load_entry_module(entry, monkeypatch, pipeline_override=RecordingPipeline)
    monkeypatch.setattr(module, entry.pipeline_attr, RecordingPipeline, raising=False)

    runner = CliRunner()
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output

    config = captured.get("config")
    assert config is not None

    stages = config.cli.get("stages", {})
    assert stages.get("iuphar") is False

    source_cfg = config.sources.get("iuphar")
    assert source_cfg is not None and source_cfg.enabled is False
    assert source_cfg.api_key is None
    assert source_cfg.headers.get("x-api-key") == ""

    runtime_options = captured.get("runtime_options")
    assert isinstance(runtime_options, dict)
    assert runtime_options.get("with_iuphar") is False


@pytest.mark.unit
def test_target_cli_requires_api_key_when_stage_forced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicitly enabling IUPHAR without API key should error out."""

    entry = next(ep for ep in ENTRYPOINTS if ep.name == "chembl_target")

    monkeypatch.delenv("IUPHAR_API_KEY", raising=False)

    module = _load_entry_module(entry, monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--dry-run",
            "--with-iuphar",
        ],
    )

    assert result.exit_code != 0
    assert "IUPHAR_API_KEY" in result.stderr


def _get_entry_by_name(name: str) -> EntryPoint:
    for entry in ENTRYPOINTS:
        if entry.name == name or entry.legacy_name == name:
            return entry
    raise AssertionError(f"Entry point '{name}' not found")


class _FakeArtifactsPipeline:
    """Pipeline stub returning deterministic output artefacts for testing."""

    def __init__(self, config, run_id):  # noqa: D401, ANN001 - Typer wiring contract
        self.config = config
        self.run_id = run_id
        self.runtime_options: dict[str, object] = {}

    def run(self, output_path: Path, extended: bool = False, *args, **kwargs) -> OutputArtifacts:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dataset_path = output_path
        dataset_path.write_text("chembl_id\nCHEMBL1\n", encoding="utf-8")
        quality_path = output_path.parent / "quality_report.csv"
        quality_path.write_text("metric,value\nrows,1\n", encoding="utf-8")
        return OutputArtifacts(
            dataset=dataset_path,
            quality_report=quality_path,
            run_directory=output_path.parent,
        )


@pytest.mark.unit
def test_cli_validate_columns_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """CLI should succeed when column validation reports no mismatches."""

    entry = _get_entry_by_name("assay")
    module = _load_entry_module(entry, monkeypatch, pipeline_override=_FakeArtifactsPipeline)
    monkeypatch.setattr(module, entry.pipeline_attr, _FakeArtifactsPipeline, raising=False)

    import bioetl.utils.column_validator as column_validator_module

    calls: dict[str, object] = {}

    class DummyResult:
        def __init__(self) -> None:
            self.overall_match = True
            self.missing_columns: list[str] = []
            self.extra_columns: list[str] = []
            self.empty_columns: list[str] = []

    class DummyValidator:
        def __init__(self) -> None:
            calls["validator_created"] = True

        def compare_columns(self, *, entity: str, actual_df, schema_version: str = "latest", **kwargs) -> DummyResult:  # type: ignore[override]
            calls["entity"] = entity
            calls["schema_version"] = schema_version
            calls["columns"] = list(actual_df.columns)
            return DummyResult()

        def generate_report(self, results, output_dir: Path) -> Path:  # type: ignore[override]
            report_path = output_dir / "report.md"
            report_path.write_text("validation report", encoding="utf-8")
            calls["report_path"] = report_path
            calls["results_len"] = len(results)
            return report_path

    monkeypatch.setattr(column_validator_module, "ColumnValidator", DummyValidator)

    runner = CliRunner()
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--output-dir",
            str(tmp_path),
            "--validate-columns",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls.get("validator_created") is True
    assert calls.get("entity") == entry.legacy_name
    assert calls.get("schema_version") == "latest"
    assert calls.get("columns") == ["chembl_id"]
    report_path = calls.get("report_path")
    assert isinstance(report_path, Path)
    assert report_path.exists()
    assert calls.get("results_len") == 1


@pytest.mark.unit
def test_cli_validate_columns_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """CLI should exit with code 1 when column validation reports missing columns."""

    entry = _get_entry_by_name("assay")
    module = _load_entry_module(entry, monkeypatch, pipeline_override=_FakeArtifactsPipeline)
    monkeypatch.setattr(module, entry.pipeline_attr, _FakeArtifactsPipeline, raising=False)

    import bioetl.utils.column_validator as column_validator_module

    class DummyResult:
        def __init__(self) -> None:
            self.overall_match = False
            self.missing_columns = ["expected_col"]
            self.extra_columns: list[str] = []
            self.empty_columns: list[str] = []

    class DummyValidator:
        def compare_columns(self, *, entity: str, actual_df, schema_version: str = "latest", **kwargs) -> DummyResult:  # type: ignore[override]
            return DummyResult()

        def generate_report(self, results, output_dir: Path) -> Path:  # type: ignore[override]
            report_path = output_dir / "report.md"
            report_path.write_text("validation report", encoding="utf-8")
            return report_path

    monkeypatch.setattr(column_validator_module, "ColumnValidator", DummyValidator)

    runner = CliRunner()
    result = runner.invoke(
        module.app,
        [
            "--config",
            str(entry.config_path),
            "--output-dir",
            str(tmp_path),
            "--validate-columns",
        ],
    )

    assert result.exit_code == 1
    assert "Отсутствуют" in result.stdout
