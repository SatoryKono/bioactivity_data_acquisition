from __future__ import annotations

from library.activity.config import load_activity_config


def test_activity_config_load_with_overrides():
    overrides = {
        "http": {
            "global": {
                "timeout_sec": 15.0,
                "headers": {"Accept": "application/json"},
                "retries": {"total": 3, "backoff_multiplier": 1.5},
            }
        },
        "sources": {
            "chembl": {
                "enabled": True,
                "name": "chembl",
                "endpoint": "activity",
                "pagination": {"page_param": "page", "size_param": "page_size", "size": 50, "max_pages": 2},
                "http": {"base_url": "https://www.ebi.ac.uk/chembl/api/data", "timeout_sec": 60.0, "headers": {}},
                "rate_limit": {"max_calls": 5, "period": 1.0},
            }
        },
        "io": {
            "input": {"activity_csv": "data/input/activity.csv"},
            "output": {"dir": "data/output/activity"},
        },
        "runtime": {"workers": 2, "limit": 10, "dry_run": False},
    }

    cfg = load_activity_config(None, overrides=overrides)

    assert cfg.http.global_.timeout_sec == 15.0
    assert "Accept" in cfg.http.global_.headers
    assert cfg.sources["chembl"].enabled is True
    assert cfg.enabled_sources() == ["chembl"]
    assert str(cfg.io.input.activity_csv).endswith("data/input/activity.csv")
    assert str(cfg.io.output.dir).endswith("data/output/activity")

