from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from bioetl.inventory import analyse_clusters, collect_inventory, load_inventory_config
from bioetl.inventory.models import InventoryRecord


def test_collect_inventory_includes_activity_pipeline() -> None:
    config = load_inventory_config(Path("configs/inventory.yaml"))
    records = collect_inventory(config)
    assert any(
        record.source == "activity" and str(record.path).endswith("src/bioetl/pipelines/activity.py")
        for record in records
    )


def test_analyse_clusters_groups_related_records() -> None:
    config = load_inventory_config(Path("configs/inventory.yaml"))
    timestamp = datetime.now(tz=timezone.utc)
    record_a = InventoryRecord(
        source="activity",
        path=Path("src/demo/a.py"),
        module="demo.a",
        size_kb=1.0,
        loc=10,
        mtime=timestamp,
        top_symbols=("Adapter",),
        imports_top=("pandas", "requests"),
        docstring_first_line="",
        config_keys=(),
        ngrams=frozenset({"client adapter", "adapter parser"}),
        import_tokens=frozenset({"pandas", "requests"}),
    )
    record_b = InventoryRecord(
        source="assay",
        path=Path("src/demo/b.py"),
        module="demo.b",
        size_kb=1.0,
        loc=12,
        mtime=timestamp,
        top_symbols=("Adapter",),
        imports_top=("pandas",),
        docstring_first_line="",
        config_keys=(),
        ngrams=frozenset({"client adapter", "adapter parser"}),
        import_tokens=frozenset({"pandas"}),
    )
    record_c = InventoryRecord(
        source="target",
        path=Path("src/demo/c.py"),
        module="demo.c",
        size_kb=1.0,
        loc=11,
        mtime=timestamp,
        top_symbols=(),
        imports_top=("numpy",),
        docstring_first_line="",
        config_keys=(),
        ngrams=frozenset({"schema validator"}),
        import_tokens=frozenset({"numpy"}),
    )

    clusters = analyse_clusters([record_a, record_b, record_c], config)
    assert any(
        {member.path for member in cluster.members} == {record_a.path, record_b.path}
        for cluster in clusters
    )
