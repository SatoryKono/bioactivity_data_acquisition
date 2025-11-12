"""Аудит документации BioETL."""

from __future__ import annotations

import csv
import os
import re
from collections.abc import Mapping, Sequence
from operator import itemgetter
from pathlib import Path
from typing import Literal, TypedDict
from uuid import uuid4

from bioetl.core.logger import UnifiedLogger
from bioetl.core.log_events import LogEvents
from bioetl.tools import get_project_root

__all__ = [
    "audit_broken_links",
    "find_lychee_missing",
    "extract_pipeline_info",
    "run_audit",
]


ROOT = get_project_root()
DOCS = ROOT / "docs"

LYCHEE_MISSING = [
    "docs/architecture/00-architecture-overview.md",
    "docs/architecture/03-data-sources-and-spec.md",
    "docs/pipelines/PIPELINES.md",
    "docs/configs/CONFIGS.md",
    "docs/cli/CLI.md",
    "docs/qc/QA_QC.md",
]

ALL_PIPELINES = [
    "activity",
    "assay",
    "target",
    "document",
    "testitem",
    "pubchem",
    "uniprot",
    "iuphar",
    "pubmed",
    "crossref",
    "openalex",
    "semantic_scholar",
    "chembl2uniprot",
]

REQUIRED_SECTIONS = ["cli", "config", "schema", "io", "determinism", "qc", "logging"]


MarkdownLink = tuple[str, str]


class BrokenLink(TypedDict):
    source: str
    link_text: str
    link_path: str
    type: Literal["broken_internal_link"]


class LycheeMissing(TypedDict):
    source: str
    file: str
    type: Literal["declared_but_missing"]


class PipelineInfo(TypedDict):
    pipeline: str
    doc_path: str
    has_cli: bool
    has_config: bool
    has_schema: bool
    has_io: bool
    has_determinism: bool
    has_qc: bool
    has_logging: bool


def read_md_file(path: Path) -> str:
    """Читает markdown файл."""

    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:  # noqa: BLE001 - логируем и возвращаем пустой текст
        log = UnifiedLogger.get(__name__)
        log.error(LogEvents.MARKDOWN_READ_FAILED, path=str(path), error=str(exc))
        return ""


def extract_markdown_links(content: str) -> list[MarkdownLink]:
    """Извлекает все markdown ссылки из содержимого."""

    pattern = r"\[([^\]]+)\]\(([^)]+)\)"
    links: list[MarkdownLink] = re.findall(pattern, content)
    sorted_links = sorted(links, key=itemgetter(1, 0))
    return sorted_links


def check_file_exists(link_path: str, base_path: Path) -> tuple[bool, Path | None]:
    """Проверяет существование файла по ссылке."""

    clean_path = link_path.split("#")[0].split("?")[0]

    if clean_path.startswith("docs/"):
        clean_path = clean_path[5:]

    if os.path.isabs(clean_path):
        candidates = [Path(clean_path)]
    else:
        candidates = [
            base_path.parent / clean_path,
            base_path / clean_path,
            ROOT / clean_path,
        ]

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return True, candidate

    return False, None


def audit_broken_links() -> list[BrokenLink]:
    """Проверяет битые ссылки во всех .md файлах."""

    log = UnifiedLogger.get(__name__)

    broken: list[BrokenLink] = []

    if not DOCS.exists():
        log.warning(LogEvents.DOCS_DIRECTORY_MISSING, docs_path=str(DOCS))
        return broken

    for md_file in sorted(DOCS.rglob("*.md")):
        content = read_md_file(md_file)
        links = extract_markdown_links(content)

        for link_text, link_path in links:
            if link_path.startswith(("http://", "https://", "mailto:")):
                continue

            if link_path.startswith("#"):
                continue

            exists, _ = check_file_exists(link_path, md_file.parent)

            if not exists:
                broken.append(
                    {
                        "source": str(md_file.relative_to(ROOT)),
                        "link_text": link_text,
                        "link_path": link_path,
                        "type": "broken_internal_link",
                    }
                )

    broken.sort(key=lambda item: (item["source"], item["link_path"], item["link_text"]))
    return broken


def find_lychee_missing() -> list[LycheeMissing]:
    """Находит файлы, объявленные в .lychee.toml, но отсутствующие."""

    missing: list[LycheeMissing] = []

    for file_path in sorted(LYCHEE_MISSING):
        full_path = ROOT / file_path
        if not full_path.exists():
            missing.append(
                {
                    "source": ".lychee.toml",
                    "file": file_path,
                    "type": "declared_but_missing",
                }
            )

    missing.sort(key=lambda item: item["file"])
    return missing


def extract_pipeline_info(pipeline_name: str) -> PipelineInfo:
    """Извлекает информацию о пайплайне из документации."""

    info: PipelineInfo = {
        "pipeline": pipeline_name,
        "doc_path": "",
        "has_cli": False,
        "has_config": False,
        "has_schema": False,
        "has_io": False,
        "has_determinism": False,
        "has_qc": False,
        "has_logging": False,
    }

    possible_names = [
        f"{pipeline_name}-chembl-extraction.md",
        f"document-{pipeline_name}-extraction.md",
        f"target-{pipeline_name}-extraction.md",
        f"testitem-{pipeline_name}-extraction.md",
        f"{pipeline_name}-extraction.md",
        f"{pipeline_name}.md",
    ]

    doc_path: Path | None = None
    for name in possible_names:
        candidates = sorted(DOCS.rglob(name))
        if candidates:
            doc_path = candidates[0]
            break

    if not doc_path and pipeline_name in {"activity", "assay", "target", "document", "testitem"}:
        catalog = DOCS / "pipelines" / "10-chembl-pipelines-catalog.md"
        if catalog.exists():
            doc_path = catalog

    if doc_path:
        info["doc_path"] = str(doc_path.relative_to(ROOT))
        content = read_md_file(doc_path).lower()

        info["has_cli"] = bool(re.search(r"cli|command|usage|invocation", content))
        info["has_config"] = bool(re.search(r"config|configuration|yaml|profile", content))
        info["has_schema"] = bool(re.search(r"schema|pandera|validation|column_order", content))
        info["has_io"] = bool(re.search(r"input|output|format|csv|parquet", content))
        info["has_determinism"] = bool(
            re.search(r"determinism|hash_row|hash_business_key|sort|utc", content)
        )
        info["has_qc"] = bool(re.search(r"qc|quality|metric|golden", content))
        info["has_logging"] = bool(re.search(r"log|logging|structured|json|run_id", content))

    return info


def _ensure_parent_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_csv_atomic(
    path: Path, fieldnames: Sequence[str], rows: Sequence[Mapping[str, str]]
) -> None:
    _ensure_parent_directory(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def _write_markdown_atomic(path: Path, lines: Sequence[str]) -> None:
    _ensure_parent_directory(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    content = "\n".join(lines)
    if not content.endswith("\n"):
        content += "\n"
    with tmp_path.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def run_audit(artifacts_dir: Path | None = None) -> None:
    """Запускает аудит документации и формирует артефакты."""

    UnifiedLogger.configure()

    run_id = uuid4().hex
    trace_id = uuid4().hex
    span_id = uuid4().hex[:16]

    UnifiedLogger.bind(
        run_id=run_id,
        pipeline="docs-audit",
        stage="bootstrap",
        dataset="docs",
        component="tools.audit_docs",
        trace_id=trace_id,
        span_id=span_id,
    )
    log = UnifiedLogger.get(__name__)

    log.info(LogEvents.AUDIT_STARTED, docs_path=str(DOCS))

    with UnifiedLogger.stage("link_audit"):
        log = UnifiedLogger.get(__name__)
        broken_links = audit_broken_links()
        lychee_missing = find_lychee_missing()
        log.info(LogEvents.LINK_AUDIT_COMPLETED,
            broken_count=len(broken_links),
            lychee_missing_count=len(lychee_missing),
        )

    with UnifiedLogger.stage("pipeline_inventory"):
        log = UnifiedLogger.get(__name__)
        pipeline_info = [extract_pipeline_info(pipeline) for pipeline in ALL_PIPELINES]
        pipeline_info.sort(key=lambda item: item["pipeline"])
        log.info(LogEvents.PIPELINE_INVENTORY_COMPLETED, pipelines=len(pipeline_info))

    with UnifiedLogger.stage("write_artifacts"):
        log = UnifiedLogger.get(__name__)
        target_dir = artifacts_dir if artifacts_dir is not None else ROOT / "artifacts"
        target_dir.mkdir(parents=True, exist_ok=True)

        gaps_rows: list[dict[str, str]] = []
        for info in pipeline_info:
            missing_core = [
                not info["has_cli"],
                not info["has_config"],
                not info["has_schema"],
            ]
            priority = "HIGH" if sum(missing_core) >= 2 else "MEDIUM"
            gaps_rows.append(
                {
                    "pipeline": info["pipeline"],
                    "doc_path": info["doc_path"] or "N/A",
                    "missing_cli": "Yes" if not info["has_cli"] else "No",
                    "missing_config": "Yes" if not info["has_config"] else "No",
                    "missing_schema": "Yes" if not info["has_schema"] else "No",
                    "missing_io": "Yes" if not info["has_io"] else "No",
                    "missing_determinism": "Yes" if not info["has_determinism"] else "No",
                    "missing_qc": "Yes" if not info["has_qc"] else "No",
                    "missing_logging": "Yes" if not info["has_logging"] else "No",
                    "priority": priority,
                }
            )

        fieldnames = [
            "pipeline",
            "doc_path",
            "missing_cli",
            "missing_config",
            "missing_schema",
            "missing_io",
            "missing_determinism",
            "missing_qc",
            "missing_logging",
            "priority",
        ]
        _write_csv_atomic(target_dir / "GAPS_TABLE.csv", fieldnames, gaps_rows)

        markdown_lines = [
            "# Link Check Report",
            "",
            "## Missing Files from .lychee.toml",
            "",
            "| Source | File | Type | Criticality |",
            "|--------|------|------|-------------|",
        ]
        for missing in lychee_missing:
            markdown_lines.append(
                f"| {missing['source']} | {missing['file']} | {missing['type']} | CRITICAL |"
            )

        markdown_lines.extend(
            [
                "",
                "## Broken Internal Links",
                "",
                "| Source | Link Text | Link Path | Type | Criticality |",
                "|--------|-----------|-----------|------|-------------|",
            ]
        )

        for broken in broken_links[:50]:
            markdown_lines.append(
                "| {source} | {link_text} | {link_path} | {issue_type} | MEDIUM |".format(
                    source=broken["source"],
                    link_text=broken["link_text"][:30],
                    link_path=broken["link_path"],
                    issue_type=broken["type"],
                )
            )

        _write_markdown_atomic(target_dir / "LINKCHECK.md", markdown_lines)
        log.info(LogEvents.ARTIFACTS_WRITTEN, directory=str(target_dir))

    UnifiedLogger.bind(stage="complete")
    log = UnifiedLogger.get(__name__)
    log.info(LogEvents.AUDIT_FINISHED)
