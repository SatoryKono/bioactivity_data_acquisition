from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from bioetl.chembl.common.enrich import ChemblEnrichmentScenario


class EnrichmentScenarioEngine:
    def __init__(self) -> None:
        self._scenarios: dict[str, ChemblEnrichmentScenario] = {}

    def register(self, scenario: ChemblEnrichmentScenario) -> None:
        self._scenarios[scenario.name] = scenario

    def execute(
        self,
        pipeline: Any,
        df: pd.DataFrame,
        *,
        selected: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        log = pipeline.logger_for(stage="transform", component="enrich")
        chembl_config = getattr(pipeline.config, "chembl", None)
        order = selected or list(self._scenarios.keys())

        for name in order:
            scenario = self._scenarios.get(name)
            if not scenario:
                log.debug("enrich_missing", name=name)
                continue
            if not scenario.is_enabled(chembl_config):
                log.debug("enrich_disabled", name=name)
                continue

            conf = scenario.extract_config(chembl_config, log=log)
            bundle = pipeline.build_chembl_entity_bundle(
                scenario.entity_name,
                source_name="chembl",
                source_config=None,
            )
            df = scenario.transform(
                pipeline,
                df,
                bundle.chembl_client,
                conf,
                log.bind(scenario=name),
            )

        return df
