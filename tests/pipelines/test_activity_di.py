"""DI contract tests for ChemblActivityPipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from bioetl.clients.chembl_entity_factory import ChemblClientBundle
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline


class _FakeChemblClient:
    def handshake(self) -> dict[str, str]:
        return {"chembl_db_version": "fake-release"}


class _FakeEntityClient:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], tuple[str, ...]]] = []

    def iterate_by_ids(self, ids: Any, select_fields: Any = None):
        normalized = list(ids)
        self.calls.append((normalized, tuple(select_fields or ())))
        for identifier in normalized:
            yield {
                "activity_id": identifier,
                "assay_chembl_id": "ASSAY1",
                "testitem_chembl_id": "TEST1",
                "molecule_chembl_id": "MOL1",
            }


@dataclass(slots=True)
class _FakeFactory:
    client: _FakeEntityClient

    def build(self, entity_name: str, **_: Any) -> ChemblClientBundle:
        return ChemblClientBundle(
            entity_name=entity_name,
            source_name="chembl",
            base_url="https://fake.api",
            api_client=object(),
            chembl_client=_FakeChemblClient(),
            entity_client=self.client,
            entity_config=None,
            source_config=None,
        )


def test_activity_pipeline_uses_injected_factory():
    fake_client = _FakeEntityClient()
    factory = _FakeFactory(fake_client)

    pipeline = ChemblActivityPipeline(source=[], client_factory=factory)

    df = pipeline.extract_by_ids(["1", "2"])

    assert fake_client.calls == [(["1", "2"], ())]
    assert isinstance(df, pd.DataFrame)
    assert sorted(df["activity_id"].tolist()) == ["1", "2"]
