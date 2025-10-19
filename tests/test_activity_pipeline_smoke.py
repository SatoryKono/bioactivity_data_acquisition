from __future__ import annotations

import pandas as pd

from library.activity.config import load_activity_config
from library.activity.pipeline import run_activity_etl


def test_activity_pipeline_smoke(monkeypatch):
    # Минимальный конфиг поверх дефолтов
    cfg = load_activity_config(None, overrides={
        "sources": {
            "chembl": {
                "enabled": True,
                "http": {"base_url": "https://www.ebi.ac.uk/chembl/api/data"},
                "endpoint": "activity",
                "pagination": {"page_param": None}
            }
        }
    })

    # Пустые входные идентификаторы — проверяем, что конвейер обрабатывает пустой набор
    input_df = pd.DataFrame([
        {"assay_chembl_id": "CHEMBL999"},
    ])

    # Подменяем извлечение: имитируем одну запись
    from library.activity import pipeline as act_pl

    def _fake_extract_activity_from_source(source, client, frame, config):
        return pd.DataFrame([
            {
                "source": "chembl",
                "retrieved_at": "2024-01-01T00:00:00Z",
                "standard_value": 1.0,
                "standard_units": "uM",
            }
        ])

    monkeypatch.setattr(act_pl, "_extract_activity_from_source", _fake_extract_activity_from_source)

    result = run_activity_etl(cfg, input_df)

    assert not result.activity.empty
    assert {"activity_value", "activity_unit"}.issubset(result.activity.columns)
    assert not result.qc.empty

