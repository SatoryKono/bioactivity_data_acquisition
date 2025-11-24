from __future__ import annotations

from pathlib import Path

import pandas as pd

from application.pipelines.specs.chembl.activity.run import ChemblActivityPipeline
from application.pipelines.specs.chembl.mixins import EnrichmentMixin, NormalizationMixin


class _Normalizer(NormalizationMixin):
    pass


class _Enricher(EnrichmentMixin):
    pass


def test_normalize_records_applies_filters_and_normalizers() -> None:
    records = [
        {"id": 1, "name": " Alice ", "keep": True},
        {"id": 2, "name": "Bob", "keep": False},
    ]

    mixin = _Normalizer()
    normalized = mixin.normalize_records(
        records,
        field_mappings={"identifier": "id", "label": "name"},
        value_normalizers={"label": lambda value: str(value).strip()},
        filters=[lambda record: record.get("keep", False)],
    )

    assert normalized == [{"identifier": 1, "label": "Alice"}]


def test_enrich_records_runs_rules_in_order() -> None:
    records = [{"value": 3}]
    enricher = _Enricher()

    def double(record):
        return {**record, "value": record["value"] * 2}

    def add_flag(record):
        return {**record, "flag": record["value"] > 4}

    enriched = enricher.enrich_records(records, [double, add_flag])

    assert enriched == [{"value": 6, "flag": True}]


def test_activity_pipeline_smoke_run(
    pipeline_config_fixture,
    run_id: str,
    tmp_path: Path,
) -> None:
    source = [
        {"activity_id": 1, "assay_id": "A1", "value": "10"},
        {"activity_id": 2, "assay_id": "A2", "value": None},
    ]

    pipeline = ChemblActivityPipeline(
        pipeline_config_fixture,
        run_id=run_id,
        source=source,
    )
    result = pipeline.run(tmp_path)
    df = result.dataframe

    # Проверяем, что пайплайн отработал и вернул данные
    assert len(df) == 2
    assert "activity_id" in df.columns
    assert "value" in df.columns
    # Проверяем, что значение было преобразовано из строки в число
    assert df.loc[0, "activity_id"] == 1
    assert df.loc[0, "value"] == 10.0
    assert df.loc[1, "activity_id"] == 2
    assert pd.isna(df.loc[1, "value"])
