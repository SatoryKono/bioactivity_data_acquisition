"""Unit tests for validity comments extraction and validation."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.pipelines.activity.activity import ChemblActivityPipeline
from bioetl.schemas.activity import ActivitySchema


@pytest.mark.unit
class TestValidityCommentsSchema:
    """Test suite for validity comments schema validation."""

    def test_schema_shape(self) -> None:
        """Test that schema contains three comment columns."""
        schema_columns = set(ActivitySchema.columns.keys())
        required_columns = {"activity_comment", "data_validity_comment", "data_validity_description"}

        assert required_columns.issubset(schema_columns), f"Missing columns: {required_columns - schema_columns}"

    def test_schema_column_types(self) -> None:
        """Test that comment columns have correct types (nullable string)."""
        schema = ActivitySchema

        assert schema.columns["activity_comment"].nullable is True
        assert schema.columns["data_validity_comment"].nullable is True
        assert schema.columns["data_validity_description"].nullable is True


@pytest.mark.unit
class TestValidityCommentsExtraction:
    """Test suite for validity comments extraction."""

    def test_extract_guarantees_columns(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that extract guarantees presence of comment fields even with empty values."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # DataFrame без полей комментариев
        df = pd.DataFrame({"activity_id": [1, 2, 3]})

        result = pipeline._ensure_comment_fields(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert "activity_comment" in result.columns
        assert "data_validity_comment" in result.columns
        assert "data_validity_description" in result.columns

        # Проверка что поля добавлены с pd.NA
        assert result["activity_comment"].isna().all()
        assert result["data_validity_comment"].isna().all()
        assert result["data_validity_description"].isna().all()

    def test_extract_guarantees_columns_with_existing_fields(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that extract doesn't override existing fields."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2],
                "activity_comment": ["Comment 1", None],
                "data_validity_comment": ["Manually validated", None],
                "data_validity_description": ["Description 1", None],
            }
        )

        result = pipeline._ensure_comment_fields(df, MagicMock())  # type: ignore[reportPrivateUsage]

        assert result["activity_comment"].iloc[0] == "Comment 1"
        assert pd.isna(result["activity_comment"].iloc[1])
        assert result["data_validity_comment"].iloc[0] == "Manually validated"
        assert result["data_validity_description"].iloc[0] == "Description 1"


@pytest.mark.unit
class TestValidityCommentsMetrics:
    """Test suite for validity comments metrics logging."""

    def test_metrics_logging_empty_dataframe(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that metrics logging doesn't fail on empty DataFrame."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
        df = pd.DataFrame()

        log = MagicMock()
        pipeline._log_validity_comments_metrics(df, log)  # type: ignore[reportPrivateUsage]

        # Не должно быть ошибок, но и не должно быть вызовов логирования
        log.info.assert_not_called()

    def test_metrics_logging_na_rates(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that metrics compute NA rates correctly."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3, 4, 5],
                "activity_comment": ["Comment 1", None, None, "Comment 4", None],
                "data_validity_comment": ["Valid", None, "Invalid", None, None],
                "data_validity_description": [None, None, "Desc", None, None],
            }
        )

        log = MagicMock()
        pipeline._log_validity_comments_metrics(df, log)  # type: ignore[reportPrivateUsage]

        # Проверка что логирование вызвано
        log.info.assert_called_once()

        # Проверка аргументов вызова
        call_args = log.info.call_args
        assert call_args[0][0] == "validity_comments_metrics"

        metrics = call_args[1]
        assert "activity_comment_na_rate" in metrics
        assert metrics["activity_comment_na_rate"] == 0.6  # 3 из 5
        assert metrics["data_validity_comment_na_rate"] == 0.6  # 3 из 5
        assert metrics["data_validity_description_na_rate"] == 0.8  # 4 из 5

    def test_metrics_logging_top_10_values(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that metrics compute top 10 data_validity_comment values."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3, 4, 5],
                "data_validity_comment": [
                    "Manually validated",
                    "Outside typical range",
                    "Manually validated",
                    "Potential transcription error",
                    None,
                ],
            }
        )

        log = MagicMock()
        pipeline._log_validity_comments_metrics(df, log)  # type: ignore[reportPrivateUsage]

        call_args = log.info.call_args
        metrics = call_args[1]

        assert "top_10_data_validity_comments" in metrics
        top_10 = metrics["top_10_data_validity_comments"]
        assert top_10["Manually validated"] == 2
        assert top_10["Outside typical range"] == 1
        assert top_10["Potential transcription error"] == 1

    def test_metrics_logging_unknown_values(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that metrics detect unknown data_validity_comment values."""
        config = pipeline_config_fixture

        with patch(
            "bioetl.pipelines.activity.activity.required_vocab_ids",
            return_value={"Manually validated", "Outside typical range"},
        ):
            pipeline = ChemblActivityPipeline(config=config, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3],
                "data_validity_comment": [
                    "Manually validated",  # В whitelist
                    "Unknown value",  # Не в whitelist
                    "Another unknown",  # Не в whitelist
                ],
            }
        )

        log = MagicMock()
        pipeline._log_validity_comments_metrics(df, log)  # type: ignore[reportPrivateUsage]

        # Проверка warning о неизвестных значениях
        log.warning.assert_called_once()
        warning_call = log.warning.call_args
        assert warning_call[0][0] == "unknown_data_validity_comments_detected"
        warning_metrics = warning_call[1]
        assert warning_metrics["unknown_count"] == 2


@pytest.mark.unit
class TestValidityCommentsSoftEnum:
    """Test suite for soft enum validation of data_validity_comment."""

    def test_soft_enum_validation_with_whitelist(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that soft enum validation logs warnings but doesn't fail."""
        config = pipeline_config_fixture

        with patch(
            "bioetl.pipelines.activity.activity.required_vocab_ids",
            return_value={"Manually validated", "Outside typical range"},
        ):
            pipeline = ChemblActivityPipeline(config=config, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3],
                "data_validity_comment": [
                    "Manually validated",  # В whitelist
                    "Unknown value",  # Не в whitelist
                    "Another unknown",  # Не в whitelist
                ],
            }
        )

        log = MagicMock()
        # Soft enum не должен падать, только логировать
        pipeline._validate_data_validity_comment_soft_enum(df, log)  # type: ignore[reportPrivateUsage]

        # Проверка warning
        log.warning.assert_called_once()
        warning_call = log.warning.call_args
        assert warning_call[0][0] == "soft_enum_unknown_data_validity_comment"
        warning_metrics = warning_call[1]
        assert warning_metrics["unknown_count"] == 2

    def test_soft_enum_validation_without_whitelist(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that soft enum validation skips when whitelist is not configured."""
        config = pipeline_config_fixture

        with patch(
            "bioetl.pipelines.activity.activity.required_vocab_ids",
            side_effect=RuntimeError("dictionary missing"),
        ):
            pipeline = ChemblActivityPipeline(config=config, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2],
                "data_validity_comment": ["Any value", "Another value"],
            }
        )

        log = MagicMock()
        pipeline._validate_data_validity_comment_soft_enum(df, log)  # type: ignore[reportPrivateUsage]

        # Не должно быть предупреждений если whitelist не настроен
        log.warning.assert_not_called()

    def test_soft_enum_validation_all_valid(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that soft enum validation doesn't log when all values are valid."""
        config = pipeline_config_fixture

        with patch(
            "bioetl.pipelines.activity.activity.required_vocab_ids",
            return_value={"Manually validated", "Outside typical range"},
        ):
            pipeline = ChemblActivityPipeline(config=config, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2],
                "data_validity_comment": ["Manually validated", "Outside typical range"],
            }
        )

        log = MagicMock()
        pipeline._validate_data_validity_comment_soft_enum(df, log)  # type: ignore[reportPrivateUsage]

        # Не должно быть предупреждений если все значения валидны
        log.warning.assert_not_called()


@pytest.mark.unit
class TestValidityCommentsInvariant:
    """Test suite for validity comments invariants."""

    def test_invariant_data_validity_description_without_comment(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that invariant warning is logged when data_validity_description is filled without data_validity_comment."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3],
                "data_validity_comment": ["Valid", None, None],
                "data_validity_description": ["Desc 1", "Desc 2", None],  # Desc 2 без comment
            }
        )

        log = MagicMock()
        pipeline._normalize_string_fields(df, log)  # type: ignore[reportPrivateUsage]

        # Проверка warning о нарушении инварианта
        log.warning.assert_called()
        warning_calls = [call[0][0] for call in log.warning.call_args_list]
        assert "invariant_data_validity_description_without_comment" in warning_calls

    def test_invariant_no_warning_when_both_filled(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that no warning when both fields are filled."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        df = pd.DataFrame(
            {
                "activity_id": [1, 2],
                "data_validity_comment": ["Valid", "Invalid"],
                "data_validity_description": ["Desc 1", "Desc 2"],
            }
        )

        log = MagicMock()
        pipeline._normalize_string_fields(df, log)  # type: ignore[reportPrivateUsage]

        # Не должно быть предупреждений о нарушении инварианта
        warning_calls = [call[0][0] for call in log.warning.call_args_list]
        assert "invariant_data_validity_description_without_comment" not in warning_calls


@pytest.mark.unit
class TestValidityCommentsOnlyFields:
    """Test suite for only fields extraction."""

    @patch("bioetl.pipelines.activity.activity.UnifiedAPIClient")
    def test_only_fields_extraction(
        self, mock_client_class: MagicMock, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that extract uses only= parameter to request specific fields."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Mock API response
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "page_meta": {"next": None},
            "activities": [
                {
                    "activity_id": 1,
                    "activity_comment": "Test comment",
                    "data_validity_comment": "Manually validated",
                    "data_validity_description": "Test description",
                }
            ],
        }
        mock_client.get.return_value = mock_response

        # Mock client factory using patch
        mock_factory = MagicMock()
        mock_factory.for_source.return_value = mock_client

        # Mock handshake
        with (
            patch.object(pipeline, "_fetch_chembl_release", return_value="v33"),
            patch.object(pipeline, "_client_factory", mock_factory),
        ):
            pipeline.extract_all()

        # Проверка что запрос использовал only= параметр
        get_calls = mock_client.get.call_args_list
        assert len(get_calls) > 0

        # Проверка что в параметрах есть only
        first_call = get_calls[0]
        call_kwargs: dict[str, Any] = first_call.kwargs if hasattr(first_call, "kwargs") else {}  # type: ignore[assignment]
        call_args = first_call.args if hasattr(first_call, "args") else ()
        params_raw: Any = call_kwargs.get("params") or (call_args[1] if len(call_args) > 1 else {})
        params: dict[str, Any] = params_raw if isinstance(params_raw, dict) else {}  # type: ignore[assignment]

        assert "only" in params
        only_fields_str = str(params["only"])  # type: ignore[arg-type]
        only_fields = only_fields_str.split(",")
        assert "activity_comment" in only_fields
        assert "data_validity_comment" in only_fields
        # data_validity_description НЕ должен быть в only=, т.к. не существует в /activity endpoint
        assert "data_validity_description" not in only_fields

    def test_extract_data_validity_description(
        self, pipeline_config_fixture: PipelineConfig, run_id: str
    ) -> None:
        """Test that data_validity_description is extracted via fetch_data_validity_lookup in extract."""
        pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)

        # Создать DataFrame с data_validity_comment
        df = pd.DataFrame(
            {
                "activity_id": [1, 2, 3],
                "data_validity_comment": [
                    "Manually validated",
                    "Outside typical range",
                    None,
                ],
            }
        )

        # Mock ChemblClient и fetch_data_validity_lookup
        from bioetl.clients.chembl import ChemblClient

        mock_api_client = MagicMock()
        mock_chembl_client = ChemblClient(mock_api_client)
        mock_chembl_client.fetch_data_validity_lookup = MagicMock(  # type: ignore[method-assign]
            return_value={
                "Manually validated": {
                    "data_validity_comment": "Manually validated",
                    "description": "This record has been manually validated",
                },
                "Outside typical range": {
                    "data_validity_comment": "Outside typical range",
                    "description": "Value is outside the typical range",
                },
            }
        )

        log = MagicMock()
        result = pipeline._extract_data_validity_descriptions(df, mock_chembl_client, log)  # type: ignore[reportPrivateUsage]

        # Проверка что fetch_data_validity_lookup был вызван
        mock_chembl_client.fetch_data_validity_lookup.assert_called_once()

        # Проверка что data_validity_description добавлен
        assert "data_validity_description" in result.columns

        # Проверка что значения корректно заполнены
        assert result["data_validity_description"].iloc[0] == "This record has been manually validated"
        assert result["data_validity_description"].iloc[1] == "Value is outside the typical range"
        assert pd.isna(result["data_validity_description"].iloc[2])  # None для пустого comment

        # Проверка что вызов был с правильными параметрами
        call_args = mock_chembl_client.fetch_data_validity_lookup.call_args
        assert set(call_args.kwargs["comments"]) == {"Manually validated", "Outside typical range"}
        assert call_args.kwargs["fields"] == ["data_validity_comment", "description"]

