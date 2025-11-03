"""CLI entrypoint for generating and verifying the pipeline inventory snapshot."""
from __future__ import annotations

import csv
import io
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import typer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from bioetl.inventory import (  # noqa: E402  # isort: skip
    analyse_clusters,
    collect_inventory,
    load_inventory_config,
    InventoryConfig,
    InventoryRecord,
)

app = typer.Typer(help="Generate and validate the unified pipeline inventory report")

DEFAULT_CONFIG = Path("configs/inventory.yaml")


def _ensure_up_to_date(path: Path, content: str, label: str) -> None:
    if not path.exists():
        typer.secho(f"{label} is missing: {path}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)
    existing = path.read_text(encoding="utf-8")
    if existing != content:
        typer.secho(f"{label} is stale: rerun inventory generator", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)


def _write_file(path: Path, content: str, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    typer.secho(f"Updated {label}: {path}", fg=typer.colors.GREEN)


def _render_csv(records: Sequence[InventoryRecord]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "source",
            "path",
            "module",
            "size_kb",
            "loc",
            "mtime",
            "top_symbols",
            "imports_top",
            "docstring_first_line",
            "config_keys",
        ]
    )
    for record in records:
        writer.writerow(record.to_csv_row())
    return buffer.getvalue()


def _render_cluster_report(records: Sequence[InventoryRecord], config: InventoryConfig) -> str:
    if records:
        timestamp_value = max(record.mtime for record in records)
    else:
        timestamp_value = datetime.now(tz=timezone.utc)
    timestamp = timestamp_value.isoformat(timespec="seconds")
    lines = [
        "# Pipeline Inventory Clusters",
        "",
        f"Generated on {timestamp}",
        "",
    ]

    clusters = analyse_clusters(records, config)
    if not clusters:
        lines.append("_No clusters matched the configured heuristics._")
    else:
        for index, cluster in enumerate(clusters, start=1):
            lines.append(f"## Cluster {index}")
            for line in cluster.summary_lines():
                lines.append(line)
            lines.append("")
    if not lines[-1]:
        lines.pop()
    return "\n".join(lines) + "\n"


@app.command()
def main(
    config_path: Path = typer.Option(
        DEFAULT_CONFIG,
        "--config",
        "-c",
        help="Path to the inventory configuration file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    check: bool = typer.Option(
        False,
        "--check",
        help="Fail if generated inventory differs from the repository snapshot",
    ),
) -> None:
    """Generate the inventory CSV and cluster report or validate existing snapshots."""

    config = load_inventory_config(config_path)
    records = collect_inventory(config)
    csv_content = _render_csv(records)
    cluster_report = _render_cluster_report(records, config)

    if check:
        _ensure_up_to_date(config.csv_output, csv_content, "Inventory CSV")
        _ensure_up_to_date(config.cluster_report, cluster_report, "Cluster report")
        typer.secho("Inventory snapshot is up to date", fg=typer.colors.GREEN)
        return

    _write_file(config.csv_output, csv_content, "inventory CSV")
    _write_file(config.cluster_report, cluster_report, "cluster report")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    app()
