"""Regression coverage for the validation chain orchestration."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pandas as pd
import pandera.errors
import pytest
from pandera import DataFrameSchema

from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.base import PipelineBase


class _ValidationProbePipeline(PipelineBase):
    """Minimal pipeline exposing the shared validation routine."""

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused in tests
        return pd.DataFrame()

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover - unused
        return pd.DataFrame({"id": list(ids)})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
        return df


def _build_pipeline(config: PipelineConfig, run_id: str) -> _ValidationProbePipeline:
    return _ValidationProbePipeline(config=config, run_id=run_id)


def _configure_schema(config: PipelineConfig) -> None:
    config.validation.schema_out = "tests.support.simple_schema.SimpleSchema"
    config.validation.strict = True
    config.validation.coerce = True


def test_validation_chain_strict_mode_raises(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    config = pipeline_config_fixture.model_copy(deep=True)
    _configure_schema(config)
    pipeline = _build_pipeline(config, run_id)

    df = pd.DataFrame({"id": [1], "value": ["invalid"]})

    with pytest.raises(pandera.errors.SchemaErrors):
        pipeline.validate(df)


def test_validation_chain_fail_open_records_summary(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    config = pipeline_config_fixture.model_copy(deep=True)
    _configure_schema(config)
    config.cli.fail_on_schema_drift = False  # type: ignore[attr-defined]
    pipeline = _build_pipeline(config, run_id)

    df = pd.DataFrame({"id": [1], "value": ["invalid"]})

    validated = pipeline.validate(df)

    assert "hash_row" in validated.columns
    assert pipeline._validation_schema is not None
    summary = pipeline._validation_summary
    assert summary is not None
    assert summary["schema_valid"] is False
    assert summary["failure_count"] >= 1
    assert summary["schema_identifier"] == "tests.support.simple_schema.SimpleSchema"
    assert summary["error"]


def test_validation_chain_retries_without_coerce(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = pipeline_config_fixture.model_copy(deep=True)
    _configure_schema(config)
    pipeline = _build_pipeline(config, run_id)

    # Патчим get_backend, чтобы перехватить backend и подменить его метод validate
    call_sequence: list[bool] = []
    original_get_backend = DataFrameSchema.get_backend
    # Словарь для хранения оригинальных методов validate для каждого backend
    backend_originals: dict[int, Any] = {}

    def patched_get_backend(
        self: DataFrameSchema,
        check_obj: pd.DataFrame,
    ) -> Any:
        backend = cast(Any, original_get_backend)(self, check_obj)
        # Применяем патч только к SimpleSchema
        # Патчим каждый раз, так как для retry создается новая схема с другим backend
        schema_name = getattr(self, "name", None)
        if schema_name == "SimpleSchema":
            # Используем id объекта backend как ключ для хранения оригинального метода
            backend_id = id(backend)
            if backend_id not in backend_originals:
                backend_originals[backend_id] = backend.validate

            original_backend_validate = backend_originals[backend_id]

            def patched_backend_validate(
                check_obj: pd.DataFrame,
                schema: DataFrameSchema,
                *args: object,
                **kwargs: object,
            ) -> pd.DataFrame:
                call_sequence.append(bool(schema.coerce))
                if bool(schema.coerce):
                    # Вызываем оригинальную валидацию, которая вызовет ошибку coercion
                    # Исключение будет перехвачено в SchemaValidationStep
                    return original_backend_validate(check_obj, schema, *args, **kwargs)
                # При coerce=False валидация должна проходить успешно для теста
                # В реальности при coerce=False pandera все равно проверяет типы и выбросит ошибку,
                # но в тесте мы хотим проверить, что retry логика работает правильно.
                # Поэтому пропускаем проверку типов, возвращая данные как есть.
                # Это симулирует успешную валидацию при coerce=False для случая,
                # когда ошибки были только из-за coercion (coerce_only=True в CoerceRetryStep).
                result = check_obj.copy()
                # Имитируем успешную валидацию, добавляя pandera атрибуты если нужно
                try:
                    # Пытаемся добавить схему к результату для совместимости с pandera
                    if hasattr(result, "pandera"):
                        result.pandera.add_schema(schema)
                except Exception:  # noqa: BLE001
                    # Игнорируем ошибки при добавлении pandera атрибутов
                    pass
                return result

            backend.validate = patched_backend_validate
        return backend

    monkeypatch.setattr(DataFrameSchema, "get_backend", patched_get_backend)

    df = pd.DataFrame({"id": [1], "value": ["invalid"]})

    validated = pipeline.validate(df)

    assert call_sequence == [True, False]
    assert validated.loc[0, "value"] == "invalid"
    assert "hash_row" in validated.columns
    summary = pipeline._validation_summary
    assert summary is not None
    assert summary["schema_valid"] is True
