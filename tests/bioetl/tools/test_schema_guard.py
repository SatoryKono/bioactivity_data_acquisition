from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace, ModuleType
from typing import Any

import pytest

from bioetl.tools import schema_guard


class DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        pass

    def warning(self, *args: Any, **kwargs: Any) -> None:
        pass


class DummyUnifiedLogger:
    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        return DummyLogger()


class DummyStage:
    def __init__(self, *_: Any, **__: Any) -> None:
        pass

    def __enter__(self) -> DummyStage:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


def test_validate_config_success_and_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text("{}", encoding="utf-8")

    class Config:
        def __init__(self) -> None:
            self.pipeline = SimpleNamespace(name="activity")

    monkeypatch.setattr(schema_guard, "UnifiedLogger", DummyUnifiedLogger)

    def fake_load_config(path: Path) -> Config:
        assert path == config_path
        return Config()

    monkeypatch.setattr("bioetl.config.loader.load_config", fake_load_config)
    ok, payload = schema_guard._validate_config(config_path)
    assert ok
    assert payload["pipeline_name"] == "activity"

    def failing_load(_: Path) -> None:
        raise ValueError("boom")

    monkeypatch.setattr("bioetl.config.loader.load_config", failing_load)
    ok, payload = schema_guard._validate_config(config_path)
    assert not ok
    assert payload["validation_errors"] == ["boom"]
    assert payload["exception_type"] == "ValueError"


def test_check_required_fields_detects_issues() -> None:
    class ChemblSource:
        def __init__(self, batch_size: int) -> None:
            self.batch_size = batch_size

    class DeterminismSort:
        def __init__(self, by: list[str] | None) -> None:
            self.by = by

    class Determinism:
        def __init__(self, sort: DeterminismSort) -> None:
            self.sort = sort

    class Config:
        def __init__(self) -> None:
            self.pipeline = SimpleNamespace(name="unexpected")
            self.sources = SimpleNamespace(chembl=ChemblSource(batch_size=42))
            self.determinism = Determinism(sort=DeterminismSort(by=None))

    errors = schema_guard._check_required_fields(Config(), "activity")
    assert "pipeline.name mismatch" in errors[0]
    assert "Invalid batch_size" in " ".join(errors)
    assert "determinism.sort.by" in " ".join(errors)


def test_validate_schema_registry_reports_mismatches(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_module = ModuleType("dummy.schema_module")
    dummy_module.SCHEMA_VERSION = "1.0"
    monkeypatch.setitem(sys.modules, "dummy.schema_module", dummy_module)

    schema = SimpleNamespace(columns={"hash_row": object, "hash_business_key": object, "value": object})
    entry_valid = SimpleNamespace(schema=schema, column_order=("hash_row", "hash_business_key", "value"), version="1.0")
    entry_dup = SimpleNamespace(schema=schema, column_order=("hash_row", "hash_row"), version="1.0")
    schema_missing_hash = SimpleNamespace(columns={"hash_row": object, "value": object})
    entry_missing = SimpleNamespace(schema=schema_missing_hash, column_order=("value",), version="2.0")

    registry_mapping = {
        "dummy.schema_module.Schema": entry_valid,
        "dummy.schema_module.SchemaDup": entry_dup,
        "dummy.schema_module.SchemaMissing": entry_missing,
    }

    class DummyRegistry:
        @staticmethod
        def as_mapping() -> dict[str, Any]:
            return registry_mapping

    monkeypatch.setattr(schema_guard, "SCHEMA_REGISTRY", DummyRegistry())
    errors = schema_guard._validate_schema_registry()
    assert any("duplicates" in err for err in errors)
    assert any("missing required column" in err for err in errors)
    assert any("version mismatch" in err for err in errors)


def test_write_report_and_run_schema_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifacts = tmp_path / "artifacts"
    project_root = tmp_path
    configs_dir = project_root / "configs" / "pipelines" / "chembl"
    configs_dir.mkdir(parents=True)

    activity_config = configs_dir / "activity.yaml"
    activity_config.write_text("{}", encoding="utf-8")

    class Config:
        def __init__(self) -> None:
            self.pipeline = SimpleNamespace(name="wrong-name")
            self.sources = SimpleNamespace(chembl=SimpleNamespace(batch_size=99))
            self.determinism = SimpleNamespace(sort=SimpleNamespace(by=None))

    monkeypatch.setattr(schema_guard, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(schema_guard, "CONFIGS", configs_dir)
    monkeypatch.setattr(schema_guard, "ARTIFACTS_DIR", artifacts)
    monkeypatch.setattr(schema_guard, "UnifiedLogger", DummyUnifiedLogger)

    def fake_load_config(_: Path) -> Config:
        return Config()

    monkeypatch.setattr("bioetl.config.loader.load_config", fake_load_config)

    schema = SimpleNamespace(columns={"hash_row": object, "hash_business_key": object})
    entry = SimpleNamespace(schema=schema, column_order=("hash_row", "hash_business_key"), version="1.0")

    module = ModuleType("dummy.schema.Module")
    module.SCHEMA_VERSION = "1.0"
    package = ModuleType("dummy")
    subpackage = ModuleType("dummy.schema")
    setattr(subpackage, "Module", module)
    monkeypatch.setitem(sys.modules, "dummy", package)
    monkeypatch.setitem(sys.modules, "dummy.schema", subpackage)
    monkeypatch.setitem(sys.modules, "dummy.schema.Module", module)

    class Registry:
        @staticmethod
        def as_mapping() -> dict[str, Any]:
            return {"dummy.schema.Module": entry}

    monkeypatch.setattr(schema_guard, "SCHEMA_REGISTRY", Registry())

    results, registry_errors, report_path = schema_guard.run_schema_guard()
    assert "activity" in results
    assert results["activity"]["valid"] is False
    assert not registry_errors
    assert report_path.exists()
    content = report_path.read_text(encoding="utf-8")
    assert "Schema Registry" in content

