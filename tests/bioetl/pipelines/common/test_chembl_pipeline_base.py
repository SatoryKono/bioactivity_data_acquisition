"""Tests for `ChemblPipelineBase` handshake behaviour."""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar
from unittest.mock import MagicMock

import pandas as pd
import pytest
from structlog.stdlib import BoundLogger

from bioetl.config import PipelineConfig
from bioetl.config.pipeline_source import BaseSourceParameters, ChemblPipelineSourceConfig
from bioetl.pipelines.chembl_base import ChemblPipelineBase


class _DummyChemblPipeline(ChemblPipelineBase):
    """Minimal concrete implementation for testing base helpers."""

    ACTOR: ClassVar[str] = "dummy_chembl"

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover - unused
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


@pytest.mark.unit
def test_fetch_chembl_release_respects_custom_endpoint(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    pipeline = _DummyChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

    source_config = ChemblPipelineSourceConfig[BaseSourceParameters](
        enabled=True,
        description=None,
        http_profile=None,
        http=None,
        page_size=25,
        max_url_length=2000,
        handshake_endpoint="/status-lite.json",
        handshake_enabled=True,
        parameters=BaseSourceParameters(),
    )

    response = MagicMock()
    response.json.return_value = {"chembl_release": "CHEMBL_37"}
    response.status_code = 200

    client = MagicMock()
    client.handshake = None
    client.get.return_value = response

    log = MagicMock(spec=BoundLogger)
    log.bind.return_value = log

    release = pipeline.fetch_chembl_release(
        client,
        log,
        source_config=source_config,
    )

    client.get.assert_called_once_with("/status-lite.json")
    assert release == "CHEMBL_37"
    assert pipeline.chembl_release == "CHEMBL_37"


@pytest.mark.unit
def test_fetch_chembl_release_skips_when_disabled(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    pipeline = _DummyChemblPipeline(config=pipeline_config_fixture, run_id=run_id)

    source_config = ChemblPipelineSourceConfig[BaseSourceParameters](
        enabled=True,
        description=None,
        http_profile=None,
        http=None,
        page_size=25,
        max_url_length=2000,
        handshake_endpoint="/status-lite.json",
        handshake_enabled=False,
        parameters=BaseSourceParameters(),
    )

    client = MagicMock()
    client.handshake = None

    log = MagicMock(spec=BoundLogger)
    log.bind.return_value = log

    release = pipeline.fetch_chembl_release(
        client,
        log,
        source_config=source_config,
    )

    client.get.assert_not_called()
    assert release is None
    assert pipeline.chembl_release is None

