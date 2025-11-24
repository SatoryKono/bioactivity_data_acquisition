from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pandas as pd

from infrastructure.chembl.enrich import ChemblEnrichmentScenario
from infrastructure.clients.chembl_entity_factory import ChemblClientBundle


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
        # Try config.chembl first, then config.domain.chembl
        chembl_config = getattr(pipeline.config, "chembl", None)
        if chembl_config is None:
            domain = getattr(pipeline.config, "domain", None)
            if domain is not None:
                chembl_config = getattr(domain, "chembl", None)
        if chembl_config is not None:
            if hasattr(chembl_config, "model_dump"):
                chembl_config = chembl_config.model_dump()
            elif hasattr(chembl_config, "dict"):
                chembl_config = chembl_config.dict()
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
            bundle_builder = getattr(
                pipeline, "_build_activity_enrichment_bundle", None
            )

            if callable(bundle_builder):
                bundle = cast(
                    ChemblClientBundle,
                    bundle_builder(
                        scenario.entity_name,
                        client_name=scenario.client_name,
                    ),
                )
            else:
                bundle = pipeline.build_chembl_entity_bundle(
                    scenario.entity_name,
                    source_name="chembl",
                    source_config=None,
                )
                register_client = getattr(pipeline, "register_client", None)
                registered_clients = getattr(
                    pipeline, "_registered_clients", None
                )
                if (
                    callable(register_client)
                    and isinstance(registered_clients, dict)
                    and scenario.client_name not in registered_clients
                ):
                    register_client(scenario.client_name, bundle.api_client)

            df = scenario.transform(
                pipeline,
                df,
                bundle.chembl_client,
                conf,
                log.bind(scenario=name),
            )

        return df
