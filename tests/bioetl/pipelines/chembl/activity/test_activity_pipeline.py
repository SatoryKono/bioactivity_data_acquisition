from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
from bioetl.chembl.common.enrich import ChemblEnrichmentScenario


def _noop_transform(
    pipeline: ChemblActivityPipeline,
    df: pd.DataFrame,
    client: Any,
    cfg: dict[str, Any],
    log: Any,
) -> pd.DataFrame:
    """Return the input DataFrame unchanged (utility for tests)."""

    return df


@pytest.mark.unit
def test_enrichment_scenario_default_paths() -> None:
    """Scenario helpers should derive enabled path and config payload automatically."""

    scenario = ChemblEnrichmentScenario(
        name="assay",
        entity_name="assay",
        config_path=("activity", "enrich", "assay"),
        client_name="test_client",
        transform=_noop_transform,
    )

    enabled_config = {"activity": {"enrich": {"assay": {"enabled": True, "page_limit": 10}}}}
    disabled_config = {"activity": {"enrich": {"assay": {"enabled": False}}}}

    assert scenario.is_enabled(enabled_config)
    assert not scenario.is_enabled(disabled_config)

    extracted = scenario.extract_config(enabled_config, log=MagicMock())
    assert extracted == {"enabled": True, "page_limit": 10}


@pytest.mark.unit
def test_execute_enrichment_stages_runs_registered_scenario(
    monkeypatch: pytest.MonkeyPatch,
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Registered scenarios should be executed when enabled in the config."""

    pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
    config_with_enrich = pipeline.config.model_copy(
        update={
            "domain": {
                "chembl": {
                    "activity": {
                        "enrich": {"dummy": {"enabled": True, "custom": "value"}}
                    }
                }
            }
        }
    )
    pipeline.config = config_with_enrich

    calls: list[dict[str, Any]] = []

    def transform(
        _pipeline: ChemblActivityPipeline,
        df: pd.DataFrame,
        _client: Any,
        cfg: dict[str, Any],
        _log: Any,
    ) -> pd.DataFrame:
        calls.append(cfg)
        enriched = df.copy()
        enriched["dummy_marker"] = "ok"
        return enriched

    scenario = ChemblEnrichmentScenario(
        name="dummy",
        entity_name="assay",
        config_path=("activity", "enrich", "dummy"),
        client_name="dummy_client",
        transform=transform,
    )

    monkeypatch.setattr(
        ChemblActivityPipeline,
        "_ENRICHMENT_SCENARIOS",
        {"dummy": scenario},
        raising=False,
    )
    monkeypatch.setattr(
        ChemblActivityPipeline,
        "_DEFAULT_ENRICHMENT_ORDER",
        ("dummy",),
        raising=False,
    )

    class DummyBundle:
        def __init__(self) -> None:
            self.chembl_client = MagicMock()

    def fake_bundle(
        self: ChemblActivityPipeline, entity_name: str, *, client_name: str
    ) -> DummyBundle:
        assert entity_name == "assay"
        assert client_name == "dummy_client"
        return DummyBundle()

    monkeypatch.setattr(
        ChemblActivityPipeline,
        "_build_activity_enrichment_bundle",
        fake_bundle,
    )

    df = pd.DataFrame({"activity_id": [1]})
    result = pipeline.execute_enrichment_stages(df)

    assert "dummy_marker" in result.columns
    assert result.loc[0, "dummy_marker"] == "ok"
    assert calls and calls[0]["custom"] == "value"


@pytest.mark.unit
def test_execute_enrichment_stages_skips_disabled_scenario(
    monkeypatch: pytest.MonkeyPatch,
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Disabled scenarios must not execute their transformation."""

    pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=run_id)
    config_with_enrich = pipeline.config.model_copy(
        update={
            "domain": {
                "chembl": {
                    "activity": {
                        "enrich": {"dummy": {"enabled": False, "custom": "value"}}
                    }
                }
            }
        }
    )
    pipeline.config = config_with_enrich

    called: bool = False

    def transform(
        _pipeline: ChemblActivityPipeline,
        df: pd.DataFrame,
        _client: Any,
        _cfg: dict[str, Any],
        _log: Any,
    ) -> pd.DataFrame:
        nonlocal called
        called = True
        return df.assign(dummy_marker="should-not-appear")

    scenario = ChemblEnrichmentScenario(
        name="dummy",
        entity_name="assay",
        config_path=("activity", "enrich", "dummy"),
        client_name="dummy_client",
        transform=transform,
    )

    monkeypatch.setattr(
        ChemblActivityPipeline,
        "_ENRICHMENT_SCENARIOS",
        {"dummy": scenario},
        raising=False,
    )
    monkeypatch.setattr(
        ChemblActivityPipeline,
        "_DEFAULT_ENRICHMENT_ORDER",
        ("dummy",),
        raising=False,
    )

    def fail_if_called(
        self: ChemblActivityPipeline, entity_name: str, *, client_name: str
    ) -> None:
        raise AssertionError(
            "Enrichment bundle should not be requested when scenario is disabled"
        )

    monkeypatch.setattr(
        ChemblActivityPipeline,
        "_build_activity_enrichment_bundle",
        fail_if_called,
    )

    df = pd.DataFrame({"activity_id": [1]})
    result = pipeline.execute_enrichment_stages(df)

    assert not called
    assert list(result.columns) == ["activity_id"]
    assert result.equals(df)

