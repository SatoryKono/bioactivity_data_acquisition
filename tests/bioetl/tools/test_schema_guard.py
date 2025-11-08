"""Тесты для `bioetl.tools.schema_guard`."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from pathlib import Path
from typing import Any

import pytest

import bioetl.tools.schema_guard as schema_guard


@pytest.mark.unit
def test_check_required_fields_detects_issues() -> None:
    """Проверяем, что обязательные поля валидируются и ошибки формируются детерминированно."""

    config = SimpleNamespace(
        pipeline=SimpleNamespace(name="mismatch"),
        sources=SimpleNamespace(chembl=SimpleNamespace()),
        determinism=SimpleNamespace(sort=SimpleNamespace(by=())),
    )

    errors = schema_guard._check_required_fields(config, "expected")  # noqa: SLF001

    assert errors == [
        "pipeline.name mismatch: expected expected, got mismatch",
        "Missing required field: sources.chembl.batch_size",
        "Missing required field: determinism.sort.by",
    ]


@pytest.mark.unit
def test_validate_schema_registry_reports_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ошибки в реестре схем отражаются в результирующем списке."""

    module_name = "tests.fake_schema.module"
    fake_module = ModuleType(module_name)
    fake_module.SCHEMA_VERSION = "2.0"
    monkeypatch.setitem(sys.modules, module_name, fake_module)

    entry = SimpleNamespace(
        schema=SimpleNamespace(columns={"id": object}),
        column_order=("id", "id"),
        version="1.0",
    )

    class DummyRegistry:
        @staticmethod
        def as_mapping() -> dict[str, Any]:
            return {f"{module_name}.ActivitySchema": entry}

    monkeypatch.setattr(schema_guard, "SCHEMA_REGISTRY", DummyRegistry())

    errors = schema_guard._validate_schema_registry()  # noqa: SLF001

    assert any("column_order contains duplicates" in error for error in errors)
    assert any("missing required column 'hash_row'" in error for error in errors)
    assert any("version mismatch" in error for error in errors)


@pytest.mark.unit
def test_run_schema_guard_generates_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_unified_logger,
    track_path_replace,
) -> None:
    """Создается отчёт с результатами и фиксируются ошибки конфигов."""

    config_root = tmp_path / "configs" / "pipelines" / "chembl"
    config_root.mkdir(parents=True)
    activity_config_path = config_root / "activity.yaml"
    activity_config_path.write_text("dummy: value", encoding="utf-8")

    def fake_load_config(path: Path) -> Any:
        if path.name == "activity.yaml":
            return SimpleNamespace(
                pipeline=SimpleNamespace(name="activity"),
                sources=SimpleNamespace(chembl=SimpleNamespace(batch_size=10)),
                determinism=SimpleNamespace(sort=SimpleNamespace(by=["hash_row"])),
            )
        raise RuntimeError("unexpected config access")

    class DummyRegistry:
        @staticmethod
        def as_mapping() -> dict[str, Any]:
            return {}

    monkeypatch.setattr(schema_guard, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(schema_guard, "CONFIGS", config_root)
    monkeypatch.setattr(schema_guard, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(schema_guard, "SCHEMA_REGISTRY", DummyRegistry())
    monkeypatch.setattr("bioetl.config.loader.load_config", fake_load_config)
    patch_unified_logger(schema_guard)

    results, registry_errors, report_path = schema_guard.run_schema_guard()

    assert registry_errors == []
    assert "activity" in results and "assay" in results
    assert results["activity"]["valid"] is True
    assert results["assay"]["valid"] is False
    assert report_path.exists()
    assert track_path_replace, "ожидается атомарная запись отчёта"

