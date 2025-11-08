"""Дополнительные тесты командного фабричного метода CLI."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import typer
from requests import RequestException

import bioetl.cli.command as cli_command


def _register_dummy_pipeline(monkeypatch: pytest.MonkeyPatch) -> type:
    """Создаёт модуль с фиктивным pipeline-классом и регистрирует его в sys.modules."""

    module_name = "tests.fake_cli_pipeline"
    module = ModuleType(module_name)

    class DummyPipeline:
        def __init__(self, config: Any, run_id: str) -> None:
            self.config = config
            self.run_id = run_id
            self.runs: list[dict[str, Any]] = []

        def run(self, output_dir: Path, **kwargs: Any) -> Any:
            self.runs.append({"output_dir": output_dir, "kwargs": kwargs})
            dataset_path = output_dir / "dataset.parquet"

            return SimpleNamespace(
                write_result=SimpleNamespace(dataset=dataset_path),
                stage_durations_ms={"total": 1.0},
            )

    DummyPipeline.__module__ = module_name
    module.DummyPipeline = DummyPipeline
    monkeypatch.setitem(sys.modules, module_name, module)
    return DummyPipeline


def _build_stub_config(tmp_path: Path) -> SimpleNamespace:
    """Конструирует упрощённый конфиг пайплайна для CLI-тестов."""

    return SimpleNamespace(
        cli=SimpleNamespace(
            dry_run=False,
            limit=None,
            sample=None,
            extended=False,
            golden=None,
            input_file=None,
            verbose=False,
            fail_on_schema_drift=True,
            validate_columns=True,
            date_tag=None,
        ),
        validation=SimpleNamespace(strict=True),
        materialization=SimpleNamespace(root=str(tmp_path / "materialized")),
        determinism=SimpleNamespace(environment=SimpleNamespace(timezone="UTC")),
        postprocess=SimpleNamespace(correlation=SimpleNamespace(enabled=False)),
    )


@pytest.mark.unit
def test_validate_output_dir_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """OSError при создании каталога приводит к Exit с кодом 2."""

    path = tmp_path / "locked"

    def raise_os_error(self: Path, *args: Any, **kwargs: Any) -> None:  # noqa: ARG001
        raise OSError("permission denied")

    monkeypatch.setattr(Path, "mkdir", raise_os_error, raising=False)

    with pytest.raises(typer.Exit) as exit_info:
        cli_command._validate_output_dir(path)  # noqa: SLF001

    assert exit_info.value.exit_code == 2


@pytest.mark.unit
def test_create_pipeline_command_mutually_exclusive_limit_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Одновременное указание --limit и --sample вызывает BadParameter."""

    pipeline_class = _register_dummy_pipeline(monkeypatch)
    command_config = SimpleNamespace(name="dummy", description="Dummy pipeline")
    command = cli_command.create_pipeline_command(pipeline_class, command_config)

    with pytest.raises(typer.BadParameter):
        command(
            config=Path("config.yaml"),
            output_dir=Path("out"),
            limit=10,
            sample=5,
        )


@pytest.mark.unit
def test_create_pipeline_command_environment_failure(
    monkeypatch: pytest.MonkeyPatch,
    patch_unified_logger,
) -> None:
    """Ошибка валидации окружения приводит к коду выхода 2 и сообщению об ошибке."""

    pipeline_class = _register_dummy_pipeline(monkeypatch)
    command_config = SimpleNamespace(name="dummy", description="Dummy pipeline")
    command = cli_command.create_pipeline_command(pipeline_class, command_config)

    def raise_env_error() -> None:
        raise ValueError("invalid env")

    monkeypatch.setattr(cli_command, "load_environment_settings", raise_env_error)
    monkeypatch.setattr(cli_command, "apply_runtime_overrides", lambda _: None)
    patch_unified_logger(cli_command)

    config_path = Path("config.yaml")
    output_dir = Path("out")

    with pytest.raises(typer.Exit) as exit_info:
        command(
            config=config_path,
            output_dir=output_dir,
            limit=None,
            sample=None,
            set_overrides=[],
            dry_run=False,
            verbose=False,
            extended=False,
            fail_on_schema_drift=True,
            validate_columns=True,
            golden=None,
            input_file=None,
        )

    assert exit_info.value.exit_code == 2


@pytest.mark.unit
def test_create_pipeline_command_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_unified_logger,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Успешный запуск команды переходит по happy-path до выхода с кодом 0."""

    pipeline_class = _register_dummy_pipeline(monkeypatch)
    command_config = SimpleNamespace(name="dummy", description="Dummy pipeline")
    command = cli_command.create_pipeline_command(pipeline_class, command_config)

    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "output"

    stub_config = _build_stub_config(tmp_path)

    monkeypatch.setattr(cli_command, "load_environment_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(cli_command, "apply_runtime_overrides", lambda _: None)
    monkeypatch.setattr(cli_command, "load_config", lambda **_: stub_config)
    patch_unified_logger(cli_command)

    with pytest.raises(typer.Exit) as exit_info:
        command(
            config=config_path,
            output_dir=output_dir,
            limit=None,
            sample=None,
            set_overrides=[],
            dry_run=False,
            verbose=False,
            extended=False,
            fail_on_schema_drift=True,
            validate_columns=True,
            golden=None,
            input_file=None,
        )

    assert exit_info.value.exit_code == 0

    captured = capsys.readouterr()
    assert "Pipeline completed successfully" in captured.out


@pytest.mark.unit
def test_create_pipeline_command_api_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_unified_logger,
) -> None:
    """Исключения RequestException приводят к коду выхода 3 и сообщению об API ошибке."""

    pipeline_class = _register_dummy_pipeline(monkeypatch)
    command_config = SimpleNamespace(name="dummy", description="Dummy pipeline")
    command = cli_command.create_pipeline_command(pipeline_class, command_config)

    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "output"

    stub_config = _build_stub_config(tmp_path)

    def failing_run(self: Any, *_: Any, **__: Any) -> None:  # noqa: ANN001
        raise RequestException("boom")

    monkeypatch.setattr(cli_command, "load_environment_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(cli_command, "apply_runtime_overrides", lambda _: None)
    monkeypatch.setattr(cli_command, "load_config", lambda **_: stub_config)
    patch_unified_logger(cli_command)
    monkeypatch.setattr(sys.modules[pipeline_class.__module__].DummyPipeline, "run", failing_run)

    with pytest.raises(typer.Exit) as exit_info:
        command(
            config=config_path,
            output_dir=output_dir,
            limit=None,
            sample=None,
            set_overrides=[],
            dry_run=False,
            verbose=False,
            extended=False,
            fail_on_schema_drift=True,
            validate_columns=True,
            golden=None,
            input_file=None,
        )

    assert exit_info.value.exit_code == 3


@pytest.mark.unit
def test_create_pipeline_command_missing_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_unified_logger,
) -> None:
    """Отсутствующие профили/конфиги переводятся в код выхода 2."""

    pipeline_class = _register_dummy_pipeline(monkeypatch)
    command_config = SimpleNamespace(name="dummy", description="Dummy pipeline")
    command = cli_command.create_pipeline_command(pipeline_class, command_config)

    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    output_dir = tmp_path / "output"

    def raise_missing_config(**_: Any) -> None:
        raise FileNotFoundError("profile.yaml")

    monkeypatch.setattr(cli_command, "load_environment_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(cli_command, "apply_runtime_overrides", lambda _: None)
    monkeypatch.setattr(cli_command, "load_config", raise_missing_config)
    patch_unified_logger(cli_command)

    with pytest.raises(typer.Exit) as exit_info:
        command(
            config=config_path,
            output_dir=output_dir,
            limit=None,
            sample=None,
            set_overrides=[],
            dry_run=False,
            verbose=False,
            extended=False,
            fail_on_schema_drift=True,
            validate_columns=True,
            golden=None,
            input_file=None,
        )

    assert exit_info.value.exit_code == 2


@pytest.mark.unit
def test_update_pipeline_config_from_cli(tmp_path: Path) -> None:
    """Функция-хелпер корректно переносит флаги CLI в конфигурацию."""

    config = SimpleNamespace(
        cli=SimpleNamespace(
            dry_run=False,
            limit=None,
            sample=None,
            extended=False,
            golden=None,
            input_file=None,
            verbose=False,
            fail_on_schema_drift=True,
            validate_columns=True,
            date_tag=None,
        ),
        validation=SimpleNamespace(strict=True),
        materialization=SimpleNamespace(root="data/output"),
    )

    cli_command._update_pipeline_config_from_cli(
        config,
        dry_run=True,
        limit=5,
        sample=3,
        extended=True,
        golden=tmp_path / "golden.parquet",
        input_file=None,
        verbose=True,
        fail_on_schema_drift=False,
        validate_columns=False,
        output_dir=tmp_path / "out",
    )

    assert config.cli.dry_run is True
    assert config.cli.limit == 5
    assert config.cli.sample == 3
    assert config.cli.extended is True
    assert config.cli.golden == str(tmp_path / "golden.parquet")
    assert config.cli.fail_on_schema_drift is False
    assert config.cli.validate_columns is False
    assert config.validation.strict is False
    assert config.materialization.root == str(tmp_path / "out")

