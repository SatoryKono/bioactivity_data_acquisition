from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pandas as pd

from infrastructure.chembl.enrich import ChemblEnrichmentScenario
from application.pipelines.specs.mixins import EnrichmentScenarioEngine


class _LoggerProbe:
    def __init__(self) -> None:
        self.debug_calls: list[dict[str, Any]] = []

    def debug(self, event: str, **kwargs: Any) -> None:  # noqa: D401
        """Capture debug events for assertions."""

        self.debug_calls.append({"event": event, **kwargs})

    def bind(self, **_: Any) -> "_LoggerProbe":
        return self


class _PipelineProbe:
    def __init__(self, chembl_config: dict[str, Any]) -> None:
        self.config = SimpleNamespace(chembl=chembl_config)
        self.logger = _LoggerProbe()

    def logger_for(self, **_: Any) -> _LoggerProbe:  # noqa: D401
        """Return the stub logger."""

        return self.logger

    def build_chembl_entity_bundle(self, *_: Any, **__: Any) -> Any:
        return SimpleNamespace(chembl_client="chembl_client")


def _make_scenario(name: str, *, value: str, enabled: bool = True) -> ChemblEnrichmentScenario:
    def transform(pipeline: Any, df: pd.DataFrame, chembl_client: Any, cfg: dict[str, Any], log: Any) -> pd.DataFrame:  # noqa: ANN401
        log.debug("transform_called", scenario=name, cfg=cfg, chembl_client=chembl_client)
        df[name] = value
        df[f"cfg_{name}"] = [cfg] * len(df)
        return df

    return ChemblEnrichmentScenario(
        name=name,
        entity_name=f"entity_{name}",
        config_path=("activity", "enrich", name),
        client_name=f"client_{name}",
        transform=transform,
        enabled_path=("activity", "enrich", name, "enabled") if enabled else None,
    )


def test_execute_runs_registered_scenarios_in_order() -> None:
    engine = EnrichmentScenarioEngine()
    engine.register(_make_scenario("first", value="A"))
    engine.register(_make_scenario("second", value="B"))

    chembl_config = {
        "activity": {
            "enrich": {
                "first": {"enabled": True, "value": "A"},
                "second": {"enabled": True, "value": "B"},
            }
        }
    }
    pipeline = _PipelineProbe(chembl_config)

    df = pd.DataFrame({"activity_id": [1]})
    enriched = engine.execute(pipeline, df)

    assert list(enriched.columns) == ["activity_id", "first", "cfg_first", "second", "cfg_second"]
    assert enriched.loc[0, "first"] == "A"
    assert enriched.loc[0, "second"] == "B"
    assert isinstance(enriched.loc[0, "cfg_first"], dict)
    assert isinstance(enriched.loc[0, "cfg_second"], dict)


def test_execute_skips_disabled_scenario() -> None:
    engine = EnrichmentScenarioEngine()
    engine.register(_make_scenario("first", value="A"))
    engine.register(_make_scenario("second", value="B"))

    chembl_config = {
        "activity": {
            "enrich": {
                "first": {"enabled": True},
                "second": {"enabled": False},
            }
        }
    }
    pipeline = _PipelineProbe(chembl_config)

    df = pd.DataFrame({"activity_id": [1]})
    enriched = engine.execute(pipeline, df)

    assert "second" not in enriched.columns
    debug_events = {entry["event"] for entry in pipeline.logger.debug_calls}
    assert "enrich_disabled" in debug_events


def test_execute_respects_selected_order() -> None:
    engine = EnrichmentScenarioEngine()
    engine.register(_make_scenario("first", value="A"))
    engine.register(_make_scenario("second", value="B"))

    chembl_config = {
        "activity": {"enrich": {"first": {"enabled": True}, "second": {"enabled": True}}}
    }
    pipeline = _PipelineProbe(chembl_config)

    df = pd.DataFrame({"activity_id": [1]})
    enriched = engine.execute(pipeline, df, selected=("second", "first"))

    assert list(enriched.columns)[:3] == ["activity_id", "second", "cfg_second"]
    assert enriched.loc[0, "second"] == "B"
    assert enriched.loc[0, "first"] == "A"
