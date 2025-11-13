from __future__ import annotations

import inspect
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest
import typer
from typer.models import OptionInfo

from bioetl.cli.cli_command import (
    _parse_set_overrides,
    _validate_config_path,
    _validate_output_dir,
    create_pipeline_command,
)
from bioetl.pipelines.errors import PipelineError, PipelineHTTPError


class DummyLogger:
    def __init__(self) -> None:
        self.logged: list[tuple[Any, dict[str, Any]]] = []

    def info(self, event: Any, **context: Any) -> None:
        self.logged.append((event, context))

    def error(self, event: Any, **context: Any) -> None:
        self.logged.append((event, context))


class DummyPipeline:
    def __init__(self, config: Any, run_id: str, *, side_effect: object) -> None:
        self.config = config
        self.run_id = run_id
        self._side_effect = side_effect

    def run(self, output_dir: Path, **kwargs: Any) -> Any:
        side_effect = self._side_effect
        if isinstance(side_effect, Exception):
            raise side_effect
        if callable(side_effect):
            return side_effect(output_dir=output_dir, **kwargs)
        dataset = side_effect if isinstance(side_effect, str) else "dataset.csv"
        return SimpleNamespace(
            write_result=SimpleNamespace(dataset=dataset),
            stage_durations_ms={"extract": 5},
        )


def _make_pipeline_config(tmp_path: Path) -> SimpleNamespace:
    cli_ns = SimpleNamespace(
        dry_run=False,
        verbose=False,
        limit=None,
        sample=None,
        extended=False,
        golden=None,
        input_file=None,
        fail_on_schema_drift=True,
        validate_columns=True,
        date_tag=None,
    )
    return SimpleNamespace(
        cli=cli_ns,
        validation=SimpleNamespace(strict=True, schema_out=True),
        determinism=SimpleNamespace(environment=SimpleNamespace(timezone="UTC")),
        materialization=SimpleNamespace(root=str(tmp_path / "materialized")),
        postprocess=SimpleNamespace(correlation=SimpleNamespace(enabled=False)),
        pipeline=SimpleNamespace(name="test"),
    )


def test_create_pipeline_command_signature_contract() -> None:
    command_config = SimpleNamespace(
        name="demo",
        description="Demo pipeline",
        pipeline_class=object,
    )
    command = create_pipeline_command(pipeline_class=object, command_config=command_config)

    signature = inspect.signature(command)
    expected_parameters = [
        "config",
        "output_dir",
        "dry_run",
        "verbose",
        "set_overrides",
        "sample",
        "limit",
        "extended",
        "fail_on_schema_drift",
        "validate_columns",
        "golden",
        "input_file",
    ]
    assert list(signature.parameters) == expected_parameters
    for parameter in signature.parameters.values():
        assert isinstance(parameter.default, OptionInfo)


def test_parse_set_overrides_invalid() -> None:
    with pytest.raises(typer.BadParameter):
        _parse_set_overrides(["invalid-format"])


def test_validate_config_path_missing(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    with pytest.raises(typer.Exit) as exit_info:
        _validate_config_path(missing)
    assert exit_info.value.code == 2


def test_validate_output_dir_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "foo"

    def failing_mkdir(self: Path, *args: Any, **kwargs: Any) -> None:
        raise OSError("no permission")

    monkeypatch.setattr(Path, "mkdir", failing_mkdir, raising=False)

    with pytest.raises(typer.Exit) as exit_info:
        _validate_output_dir(target)
    assert exit_info.value.code == 2


def _prepare_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    run_side_effect: object,
) -> tuple[Callable[..., None], DummyLogger, Path, Path, dict[str, Any]]:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "out"

    env_settings = SimpleNamespace()
    monkeypatch.setattr("bioetl.cli.pipeline_command_runner.load_environment_settings", lambda: env_settings)
    applied: dict[str, Any] = {}
    monkeypatch.setattr(
        "bioetl.cli.pipeline_command_runner.apply_runtime_overrides",
        lambda settings: applied.setdefault("settings", settings),
    )

    def fake_load_config(**_: Any) -> SimpleNamespace:
        return _make_pipeline_config(tmp_path)

    monkeypatch.setattr("bioetl.cli.pipeline_command_runner.load_config", fake_load_config)

    logger = DummyLogger()
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.get", lambda _: logger)
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.configure", lambda _: None)
    monkeypatch.setattr(
        "bioetl.cli.pipeline_command_runner.uuid4",
        lambda: uuid.UUID("00000000-0000-0000-0000-000000000000"),
    )

    def constructor(config: Any, run_id: str) -> DummyPipeline:
        return DummyPipeline(config, run_id, side_effect=run_side_effect)

    monkeypatch.setattr(
        "bioetl.cli.cli_command._resolve_pipeline_class",
        lambda **kwargs: constructor,
    )

    command_config = SimpleNamespace(
        name="test-command",
        description="Test command",
        pipeline_class=object,
    )
    command = create_pipeline_command(pipeline_class=object, command_config=command_config)

    return command, logger, config_path, output_dir, applied


def test_command_dry_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    call_counter = {"resolved": 0}

    def constructor(config: Any, run_id: str) -> DummyPipeline:
        call_counter["resolved"] += 1
        return DummyPipeline(config, run_id, side_effect="dataset.csv")

    monkeypatch.setattr(
        "bioetl.cli.cli_command._resolve_pipeline_class",
        lambda **kwargs: constructor,
    )
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.configure", lambda _: None)
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.get", lambda _: DummyLogger())
    monkeypatch.setattr("bioetl.cli.pipeline_command_runner.load_environment_settings", lambda: SimpleNamespace())
    monkeypatch.setattr("bioetl.cli.pipeline_command_runner.apply_runtime_overrides", lambda _: None)
    monkeypatch.setattr(
        "bioetl.cli.pipeline_command_runner.load_config",
        lambda **_: _make_pipeline_config(tmp_path),
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "out"

    command_config = SimpleNamespace(
        name="dry",
        description="dry run",
        pipeline_class=object,
    )
    command = create_pipeline_command(pipeline_class=object, command_config=command_config)
    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir, dry_run=True)
    assert exit_info.value.code == 0
    # Pipeline is never resolved for dry-run.
    assert call_counter["resolved"] == 0


def test_command_successful_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    command, logger, config_path, output_dir, applied = _prepare_command(
        monkeypatch,
        tmp_path,
        run_side_effect="dataset.csv",
    )

    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir, verbose=True, set_overrides=["foo=bar"])
    assert exit_info.value.code == 0
    assert applied["settings"] is not None

    captured = capsys.readouterr()
    assert "Pipeline completed successfully" in captured.out
    assert any(event for event, _ in logger.logged if "CLI_RUN_START" in str(event))


def test_command_pipeline_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    command, logger, config_path, output_dir, _ = _prepare_command(
        monkeypatch,
        tmp_path,
        run_side_effect=PipelineError("pipeline failed"),
    )

    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir)
    assert exit_info.value.code == 1
    error_events = [event for event, _ in logger.logged if "CLI_RUN_ERROR" in str(event)]
    assert error_events


def test_command_external_api_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import requests

    command, _, config_path, output_dir, _ = _prepare_command(
        monkeypatch,
        tmp_path,
        run_side_effect=requests.exceptions.Timeout("timeout"),
    )

    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir)
    assert exit_info.value.code == 3


def test_command_pipeline_http_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    command, _, config_path, output_dir, _ = _prepare_command(
        monkeypatch,
        tmp_path,
        run_side_effect=PipelineHTTPError("http error"),
    )

    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir)
    assert exit_info.value.code == 3


def test_command_limit_sample_conflict(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    command, _, config_path, output_dir, _ = _prepare_command(
        monkeypatch,
        tmp_path,
        run_side_effect="dataset.csv",
    )

    with pytest.raises(typer.BadParameter):
        command(config=config_path, output_dir=output_dir, limit=10, sample=5)


def test_command_environment_settings_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "bioetl.cli.pipeline_command_runner.load_environment_settings",
        lambda: (_ for _ in ()).throw(ValueError("bad env")),
    )
    monkeypatch.setattr("bioetl.cli.pipeline_command_runner.apply_runtime_overrides", lambda _: None)
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.configure", lambda _: None)
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.get", lambda _: DummyLogger())

    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "out"

    command_config = SimpleNamespace(
        name="env",
        description="env",
        pipeline_class=object,
    )
    command = create_pipeline_command(pipeline_class=object, command_config=command_config)
    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir)
    assert exit_info.value.code == 2


def test_command_load_config_failures(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "out"

    monkeypatch.setattr("bioetl.cli.pipeline_command_runner.load_environment_settings", lambda: SimpleNamespace())
    monkeypatch.setattr("bioetl.cli.pipeline_command_runner.apply_runtime_overrides", lambda _: None)
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.configure", lambda _: None)
    monkeypatch.setattr("bioetl.cli.cli_command.UnifiedLogger.get", lambda _: DummyLogger())

    command_config = SimpleNamespace(
        name="config",
        description="config",
        pipeline_class=object,
    )

    def make_command() -> Callable[..., None]:
        return create_pipeline_command(pipeline_class=object, command_config=command_config)

    monkeypatch.setattr(
        "bioetl.cli.pipeline_command_runner.load_config",
        lambda **_: (_ for _ in ()).throw(FileNotFoundError("missing profile")),
    )
    command = make_command()
    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir)
    assert exit_info.value.code == 2

    monkeypatch.setattr(
        "bioetl.cli.pipeline_command_runner.load_config",
        lambda **_: (_ for _ in ()).throw(ValueError("validation fail")),
    )
    command = make_command()
    with pytest.raises(typer.Exit) as exit_info:
        command(config=config_path, output_dir=output_dir)
    assert exit_info.value.code == 2

