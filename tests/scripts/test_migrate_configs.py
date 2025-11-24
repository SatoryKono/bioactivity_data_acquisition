from __future__ import annotations

from pathlib import Path

from scripts.migrate_configs import migrate_config_file


def test_migrate_config_moves_select_fields(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
        sources:
          chembl:
            select_fields:
              - field1
        """,
        encoding="utf-8",
    )

    changed = migrate_config_file(config_file, apply=True)

    assert changed is True
    updated = config_file.read_text(encoding="utf-8")
    assert "parameters" in updated
    assert "select_fields" in updated
