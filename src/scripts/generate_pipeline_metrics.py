"""Generate or validate the pipeline metrics documentation."""
from __future__ import annotations

import csv
import os
import xml.etree.ElementTree as ET
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence

import typer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASELINE_CSV = PROJECT_ROOT / "artifacts" / "baselines" / "pre_migration" / "PIPELINES.inventory.csv"
DEFAULT_CURRENT_CSV = PROJECT_ROOT / "docs" / "requirements" / "PIPELINES.inventory.csv"
DEFAULT_BASELINE_TEST_LOG = PROJECT_ROOT / "artifacts" / "baselines" / "golden_tests" / "pytest_integration.log"
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "requirements" / "PIPELINES.metrics.md"

PYTEST_COMMAND: Sequence[str] = (
    "pytest",
    "tests",
    "-q",
    "--maxfail=1",
    "--disable-warnings",
)

app = typer.Typer(help="Generate the comparative pipeline metrics report")


@dataclass(frozen=True)
class InventoryRow:
    """Typed representation of a CSV row from the inventory snapshot."""

    source: str
    path: Path
    loc: int
    top_symbols: tuple[str, ...]
    imports: tuple[str, ...]
    is_python: bool

    @property
    def has_pandera_reference(self) -> bool:
        if not self.is_python:
            return False
        path_token = self.path.as_posix().lower()
        if "schema" in path_token or "schemas" in path_token:
            return True
        for token in self.imports:
            lowered = token.lower()
            if not lowered:
                continue
            if "bioetl.schemas" in lowered:
                return True
            if lowered.startswith(".schema") or ".schema." in lowered:
                return True
            if lowered.startswith("bioetl.pandera") or "pandera" in lowered:
                return True
        return False


@dataclass(frozen=True)
class Metrics:
    files: int
    loc: int
    public_symbols: int
    pandera_loc: int


@dataclass(frozen=True)
class Category:
    """Defines a subset of the inventory used for aggregation."""

    label: str
    predicate: Callable[[InventoryRow], bool]


CATEGORY_DEFINITIONS: Sequence[Category] = (
    Category(
        label="Monolithic pipelines",
        predicate=lambda row: row.path.as_posix().startswith("src/bioetl/pipelines/"),
    ),
    Category(
        label="ChEMBL proxies",
        predicate=lambda row: row.path.as_posix().startswith("src/bioetl/sources/chembl/"),
    ),
)


def load_inventory(path: Path) -> list[InventoryRow]:
    if not path.exists():
        msg = f"Inventory snapshot not found: {path}"
        raise FileNotFoundError(msg)

    records: list[InventoryRow] = []
    with path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_path = row.get("path", "").strip()
            if not raw_path:
                continue
            loc_value = int(row.get("loc", 0) or 0)
            top_symbols = tuple(
                symbol for symbol in (row.get("top_symbols", "") or "").split(";") if symbol
            )
            imports = tuple(
                module for module in (row.get("imports_top", "") or "").split(";") if module
            )
            suffix = Path(raw_path).suffix.lower()
            is_python = suffix == ".py"
            records.append(
                InventoryRow(
                    source=row.get("source", "").strip() or "unknown",
                    path=Path(raw_path),
                    loc=loc_value,
                    top_symbols=top_symbols,
                    imports=imports,
                    is_python=is_python,
                )
            )
    return records


def filter_records(records: Iterable[InventoryRow], predicate: Callable[[InventoryRow], bool]) -> list[InventoryRow]:
    return [record for record in records if predicate(record)]


def compute_metrics(records: Sequence[InventoryRow]) -> Metrics:
    files = len(records)
    loc = sum(record.loc for record in records)
    public_symbols = sum(len(record.top_symbols) for record in records)
    pandera_loc = sum(record.loc for record in records if record.has_pandera_reference)
    return Metrics(files=files, loc=loc, public_symbols=public_symbols, pandera_loc=pandera_loc)


def aggregate_metrics(records: Sequence[InventoryRow], categories: Sequence[Category]) -> dict[str, Metrics]:
    metrics: dict[str, Metrics] = {}
    for category in categories:
        subset = filter_records(records, category.predicate)
        metrics[category.label] = compute_metrics(subset)
    combined_subset: list[InventoryRow] = []
    for category in categories:
        combined_subset.extend(filter_records(records, category.predicate))
    metrics["Combined"] = compute_metrics(combined_subset)
    return metrics


def calculate_pandera_ratio(metrics: Metrics) -> float:
    if metrics.loc == 0:
        return 0.0
    return metrics.pandera_loc / metrics.loc


def format_number(value: int) -> str:
    return f"{value:,}".replace(",", "\u202f")


def format_delta(value: int) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{format_number(value)}"


def format_percentage(value: float) -> str:
    return f"{value * 100:.1f}%"


def parse_baseline_test_time(path: Path) -> float | None:
    if not path.exists():
        return None
    pattern = re.compile(r"in\s+([0-9]+(?:\.[0-9]+)?)s")
    for line in reversed(path.read_text(encoding="utf-8").splitlines()):
        match = pattern.search(line)
        if match:
            return float(match.group(1))
    return None


def run_tests() -> tuple[float, str]:
    start = time.perf_counter()
    env = os.environ.copy()
    env.setdefault("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = subprocess.run(
        PYTEST_COMMAND,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    elapsed = time.perf_counter() - start
    output = (result.stdout or "") + (result.stderr or "")
    if result.returncode != 0:
        raise RuntimeError(f"pytest command failed (exit {result.returncode})\n{output}")

    pattern = re.compile(r"in\s+([0-9]+(?:\.[0-9]+)?)s")
    for line in reversed(output.splitlines()):
        match = pattern.search(line)
        if match:
            return float(match.group(1)), output
    return elapsed, output


def parse_junit_duration(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return None
    root = tree.getroot()
    total = 0.0
    for suite in root.iter("testsuite"):
        time_value = suite.attrib.get("time")
        if not time_value:
            continue
        try:
            total += float(time_value)
        except ValueError:
            continue
    return total if total > 0.0 else None


def build_markdown(
    baseline_metrics: dict[str, Metrics],
    current_metrics: dict[str, Metrics],
    baseline_ratio: float,
    current_ratio: float,
    baseline_test_time: float | None,
    current_test_time: float | None,
) -> str:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines: list[str] = []
    lines.append("# Pipeline Metrics Report")
    lines.append("")
    lines.append(f"Generated on {timestamp}")
    lines.append("")

    lines.append("## Code Footprint")
    lines.append("")
    header = (
        "| Category | Files (baseline) | Files (current) | Δ | "
        "LOC (baseline) | LOC (current) | Δ | Public symbols (baseline) | Public symbols (current) | Δ |"
    )
    separator = "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    lines.append(header)
    lines.append(separator)

    for label in [category.label for category in CATEGORY_DEFINITIONS] + ["Combined"]:
        base = baseline_metrics.get(label, Metrics(0, 0, 0, 0))
        current = current_metrics.get(label, Metrics(0, 0, 0, 0))
        lines.append(
            "| {label} | {base_files} | {current_files} | {delta_files} | "
            "{base_loc} | {current_loc} | {delta_loc} | {base_symbols} | {current_symbols} | {delta_symbols} |".format(
                label=label,
                base_files=format_number(base.files),
                current_files=format_number(current.files),
                delta_files=format_delta(current.files - base.files),
                base_loc=format_number(base.loc),
                current_loc=format_number(current.loc),
                delta_loc=format_delta(current.loc - base.loc),
                base_symbols=format_number(base.public_symbols),
                current_symbols=format_number(current.public_symbols),
                delta_symbols=format_delta(current.public_symbols - base.public_symbols),
            )
        )
    lines.append("")

    lines.append("## Pandera Validation Coverage")
    lines.append("")
    lines.append("| Scope | Baseline | Current | Δ |")
    lines.append("| --- | ---: | ---: | ---: |")
    lines.append(
        "| Pipeline family | {baseline} | {current} | {delta} |".format(
            baseline=format_percentage(baseline_ratio),
            current=format_percentage(current_ratio),
            delta=format_percentage(current_ratio - baseline_ratio),
        )
    )
    lines.append("")

    lines.append("## Test Execution Time")
    lines.append("")
    lines.append("| Test suite | Baseline | Current | Δ |")
    lines.append("| --- | ---: | ---: | ---: |")
    if baseline_test_time is not None and current_test_time is not None:
        delta_time = current_test_time - baseline_test_time
        lines.append(
            "| pytest (tests) | {baseline:.2f}s | {current:.2f}s | {delta:+.2f}s |".format(
                baseline=baseline_test_time,
                current=current_test_time,
                delta=delta_time,
            )
        )
    else:
        baseline_value = "n/a" if baseline_test_time is None else f"{baseline_test_time:.2f}s"
        current_value = "n/a" if current_test_time is None else f"{current_test_time:.2f}s"
        lines.append(f"| pytest (tests) | {baseline_value} | {current_value} | n/a |")
    lines.append("")

    lines.append("### Methodology")
    lines.append("")
    lines.append(
        "- Inventory metrics are derived from `PIPELINES.inventory.csv` snapshots "
        "(baseline vs. current). Public symbols count exported names captured during inventory collection."
    )
    lines.append(
        "- Pandera coverage estimates weight the share of lines of code importing Pandera schemas "
        "within monolithic pipelines and ChEMBL proxies."
    )
    lines.append(
        "- Test duration measures `pytest --maxfail=1 --disable-warnings -q tests` wall-clock time."
    )
    lines.append("")

    return "\n".join(lines).strip() + "\n"


@app.command()
def main(
    baseline_csv: Path = typer.Option(
        DEFAULT_BASELINE_CSV,
        "--baseline-csv",
        help="Path to the baseline inventory snapshot",
    ),
    current_csv: Path = typer.Option(
        DEFAULT_CURRENT_CSV,
        "--current-csv",
        help="Path to the current inventory snapshot",
    ),
    output_path: Path = typer.Option(
        DEFAULT_OUTPUT,
        "--output",
        help="Path where the metrics markdown will be written",
    ),
    baseline_test_log: Path = typer.Option(
        DEFAULT_BASELINE_TEST_LOG,
        "--baseline-test-log",
        help="Historical pytest log containing the baseline duration",
    ),
    junit_xml: Path | None = typer.Option(
        None,
        "--junit-xml",
        help="Parse an existing JUnit XML report for current test duration",
    ),
    check: bool = typer.Option(
        False,
        "--check",
        help="Fail if the generated metrics differ from the existing documentation",
    ),
    skip_tests: bool = typer.Option(
        False,
        "--skip-tests",
        help="Skip executing pytest when calculating current metrics",
    ),
) -> None:
    """Generate the metrics document or validate the checked-in snapshot."""

    baseline_records = load_inventory(baseline_csv)
    current_records = load_inventory(current_csv)

    baseline_metrics = aggregate_metrics(baseline_records, CATEGORY_DEFINITIONS)
    current_metrics = aggregate_metrics(current_records, CATEGORY_DEFINITIONS)

    baseline_ratio = calculate_pandera_ratio(baseline_metrics["Combined"])
    current_ratio = calculate_pandera_ratio(current_metrics["Combined"])

    baseline_time = parse_baseline_test_time(baseline_test_log)
    current_time: float | None = None

    if junit_xml is not None:
        current_time = parse_junit_duration(junit_xml)
        if current_time is not None:
            typer.secho(
                f"Using JUnit report for test duration: {junit_xml}",
                fg=typer.colors.BLUE,
            )

    if current_time is None and not skip_tests:
        current_time, output = run_tests()
        typer.echo(output)

    content = build_markdown(
        baseline_metrics=baseline_metrics,
        current_metrics=current_metrics,
        baseline_ratio=baseline_ratio,
        current_ratio=current_ratio,
        baseline_test_time=baseline_time,
        current_test_time=current_time,
    )

    if check:
        if not output_path.exists():
            typer.secho(
                f"Metrics document missing: {output_path}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)
        existing = output_path.read_text(encoding="utf-8")
        if existing != content:
            typer.secho(
                "Metrics document is stale. Re-run the generator.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)
        typer.secho("Metrics snapshot is current", fg=typer.colors.GREEN)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    typer.secho(f"Updated metrics report: {output_path}", fg=typer.colors.GREEN)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    app()
