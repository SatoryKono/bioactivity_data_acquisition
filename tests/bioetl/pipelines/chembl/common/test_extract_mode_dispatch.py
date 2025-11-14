"""Tests for the shared ChEMBL extract mode dispatcher."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from structlog.stdlib import BoundLogger

from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline


@pytest.mark.unit
def test_dispatch_extract_mode_prefers_cli_ids(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """Ensure CLI-provided identifiers trigger batch extraction."""

    config = pipeline_config_fixture.model_copy(deep=True)
    config.cli.input_file = "ids.csv"
    config.cli.limit = 50
    config.cli.sample = 10

    pipeline = ChemblActivityPipeline(config=config, run_id=run_id)

    batch_result = pd.DataFrame({"activity_id": [1, 2, 3]})
    batch_callback = MagicMock(return_value=batch_result)
    full_callback = MagicMock(return_value=pd.DataFrame())
    log = MagicMock(spec=BoundLogger)

    with patch.object(pipeline, "_read_input_ids", return_value=["42", "84"]) as read_mock:
        result = pipeline._dispatch_extract_mode(
            log,
            event_name="chembl_activity.extract_mode",
            batch_callback=batch_callback,
            full_callback=full_callback,
            id_column_name="activity_id",
        )

    read_mock.assert_called_once_with(
        id_column_name="activity_id",
        limit=50,
        sample=10,
    )
    batch_callback.assert_called_once_with(["42", "84"])
    full_callback.assert_not_called()
    log.info.assert_called_once_with(
        "chembl_activity.extract_mode",
        mode="batch",
        source="cli_input",
        ids_count=2,
    )
    assert result is batch_result


@pytest.mark.unit
def test_dispatch_extract_mode_falls_back_to_full(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """When no identifiers are supplied, the full extraction path is used."""

    config = pipeline_config_fixture.model_copy(deep=True)
    config.cli.input_file = None

    pipeline = ChemblActivityPipeline(config=config, run_id=run_id)

    batch_callback = MagicMock(return_value=pd.DataFrame())
    full_result = pd.DataFrame({"activity_id": []})
    full_callback = MagicMock(return_value=full_result)
    log = MagicMock(spec=BoundLogger)

    with patch.object(pipeline, "_read_input_ids") as read_mock:
        result = pipeline._dispatch_extract_mode(
            log,
            event_name="chembl_activity.extract_mode",
            batch_callback=batch_callback,
            full_callback=full_callback,
            id_column_name="activity_id",
        )

    read_mock.assert_not_called()
    batch_callback.assert_not_called()
    full_callback.assert_called_once_with()
    log.info.assert_called_once_with("chembl_activity.extract_mode", mode="full")
    assert result is full_result
