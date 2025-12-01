from __future__ import annotations

from pathlib import Path

import yaml

from bioetl.clients.config.loader import load_source_config


def test_load_source_config_applies_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "example.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "base_url": "https://example.test",
                "resources": {
                    "sample": {
                        "path": "sample",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_source_config("example", root=tmp_path)

    resource = config.resources["sample"]
    assert resource.path == "/sample"
    assert resource.paging.type == "none"
    assert resource.paging.default_page_size is None


def test_load_source_config_preserves_paging_settings(tmp_path: Path) -> None:
    config_path = tmp_path / "example.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "base_url": "https://example.test",
                "resources": {
                    "sample": {
                        "path": "/records",
                        "paging": {
                            "type": "page",
                            "page_param": "page",
                            "page_size_param": "page_size",
                            "default_page_size": 20,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_source_config("example", root=tmp_path)

    paging = config.resources["sample"].paging
    assert paging.type == "page"
    assert paging.default_page_size == 20
    assert paging.page_param == "page"
