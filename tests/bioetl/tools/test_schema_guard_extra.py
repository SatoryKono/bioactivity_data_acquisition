"""Тесты для schema_guard."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from bioetl.tools import schema_guard as module


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def warning(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def bind(self, **_: Any) -> "_DummyLogger":
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


@pytest.fixture(autouse=True)
def patch_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())


def test_check_required_fields_detects_missing() -> None:
    config = SimpleNamespace()
    errors = module._check_required_fields(config, "activity")
    assert "pipeline.name" in "; ".join(errors)


def test_validate_schema_registry_reports_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Entry:
        schema = SimpleNamespace(columns={"hash_row": object(), "value": object()})
        column_order = ("hash_row",)
        version = "1"

    class _Registry:
        def as_mapping(self) -> dict[str, _Entry]:
            return {"bioetl.schemas.fake.Schema": _Entry()}

    fake_module = SimpleNamespace(SCHEMA_VERSION="2")
    monkeypatch.setitem(sys.modules, "bioetl.schemas.fake", fake_module)
    monkeypatch.setattr(module, "SCHEMA_REGISTRY", _Registry())

    errors = module._validate_schema_registry()
    assert any("missing required column" in error for error in errors)


def test_run_schema_guard_generates_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module.PROJECT_ROOT = tmp_path
    module.ARTIFACTS_DIR = tmp_path / "artifacts"
    module.CONFIGS = tmp_path / "configs"
    module.CONFIGS.mkdir(parents=True, exist_ok=True)

    activity_config = module.CONFIGS / "activity.yaml"
    activity_config.write_text("pipeline: activity", encoding="utf-8")
    assay_config = module.CONFIGS / "assay.yaml"
    assay_config.write_text("pipeline: assay", encoding="utf-8")

    valid_config = SimpleNamespace(
        pipeline=SimpleNamespace(name="activity"),
        sources=SimpleNamespace(chembl=SimpleNamespace(batch_size=10)),
        determinism=SimpleNamespace(sort=SimpleNamespace(by=["id"])),
    )

    invalid_config = SimpleNamespace(
        pipeline=SimpleNamespace(name="wrong"),
        sources=SimpleNamespace(chembl=SimpleNamespace(batch_size=50)),
        determinism=SimpleNamespace(sort=SimpleNamespace(by=[])),
    )

    def fake_validate_config(path: Path) -> tuple[bool, dict[str, Any]]:
        if path.name == "activity.yaml":
            return True, {
                "config": valid_config,
                "pipeline_name": "activity",
                "validation_errors": [],
            }
        return True, {
            "config": invalid_config,
            "pipeline_name": "assay",
            "validation_errors": [],
        }

    class _Registry:
        def as_mapping(self) -> dict[str, Any]:
            entry = SimpleNamespace(
                schema=SimpleNamespace(columns={"hash_row": object(), "hash_business_key": object(), "value": object()}),
                column_order=("hash_row", "hash_business_key", "value"),
                version="1",
            )
            return {"bioetl.schemas.fake.Schema": entry}

    fake_module = SimpleNamespace(SCHEMA_VERSION="1")
    monkeypatch.setitem(sys.modules, "bioetl.schemas.fake", fake_module)
    monkeypatch.setattr(module, "SCHEMA_REGISTRY", _Registry())
    monkeypatch.setattr(module, "_validate_config", fake_validate_config)

    results, registry_errors, report_path = module.run_schema_guard()

    assert "activity" in results
    assert not results["activity"]["errors"]
    assert results["assay"]["errors"]
    assert registry_errors == []
    assert report_path.exists()


def test_validate_config_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_obj = SimpleNamespace(
        pipeline=SimpleNamespace(name="activity"),
        sources=SimpleNamespace(chembl=SimpleNamespace(batch_size=10)),
        determinism=SimpleNamespace(sort=SimpleNamespace(by=["id"])),
    )

    fake_module = SimpleNamespace(read_pipeline_config=lambda path: config_obj)
    monkeypatch.setitem(sys.modules, "bioetl.config.loader", fake_module)

    valid, payload = module._validate_config(tmp_path / "activity.yaml")

    assert valid
    assert payload["pipeline_name"] == "activity"


def test_validate_config_handles_exception(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def _raise(_: Path) -> None:
        raise RuntimeError("boom")

    fake_module = SimpleNamespace(read_pipeline_config=_raise)
    monkeypatch.setitem(sys.modules, "bioetl.config.loader", fake_module)

    valid, payload = module._validate_config(tmp_path / "broken.yaml")

    assert not valid
    assert "boom" in payload["validation_errors"][0]

