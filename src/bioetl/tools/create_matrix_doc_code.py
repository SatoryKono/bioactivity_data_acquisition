"""Матрица трассировки документации к коду."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bioetl.core.logger import UnifiedLogger
from bioetl.tools import get_project_root

__all__ = [
    "DocCodeMatrix",
    "build_matrix",
    "write_matrix",
]


PROJECT_ROOT = get_project_root()


@dataclass(frozen=True)
class DocCodeMatrix:
    """Результат генерации матрицы Doc↔Code."""

    rows: tuple[dict[str, Any], ...]
    csv_path: Path
    json_path: Path


def build_matrix() -> list[dict[str, Any]]:
    """Создаёт матрицу трассировки Doc↔Code."""

    matrix: list[dict[str, Any]] = []

    base_contracts = [
        {
            "doc_point": "extract signature: extract(*args, **kwargs) -> pd.DataFrame",
            "code_file": "src/bioetl/pipelines/base.py",
            "code_symbol": "PipelineBase.extract",
            "pipeline": "base",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "transform signature: transform(df: pd.DataFrame) -> pd.DataFrame",
            "code_file": "src/bioetl/pipelines/base.py",
            "code_symbol": "PipelineBase.transform",
            "pipeline": "base",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "validate signature: validate(df: pd.DataFrame) -> pd.DataFrame",
            "code_file": "src/bioetl/pipelines/base.py",
            "code_symbol": "PipelineBase.validate",
            "pipeline": "base",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "write signature: write(df: pd.DataFrame, output_path: Path, extended: bool = False) -> RunResult",
            "code_file": "src/bioetl/pipelines/base.py",
            "code_symbol": "PipelineBase.write",
            "pipeline": "base",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "run signature: run(output_path: Path, extended: bool = False, *args, **kwargs) -> RunResult",
            "code_file": "src/bioetl/pipelines/base.py",
            "code_symbol": "PipelineBase.run",
            "pipeline": "base",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
    ]

    activity_contracts = [
        {
            "doc_point": "CLI command: activity_chembl",
            "code_file": "src/bioetl/cli/registry.py",
            "code_symbol": "COMMAND_REGISTRY['activity_chembl']",
            "pipeline": "activity",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "Config: configs/pipelines/activity/activity_chembl.yaml",
            "code_file": "configs/pipelines/activity/activity_chembl.yaml",
            "code_symbol": "activity.yaml",
            "pipeline": "activity",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "Class: ChemblActivityPipeline(PipelineBase)",
            "code_file": "src/bioetl/pipelines/chembl/activity.py",
            "code_symbol": "ChemblActivityPipeline",
            "pipeline": "activity",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
    ]

    assay_contracts = [
        {
            "doc_point": "CLI command: assay_chembl",
            "code_file": "src/bioetl/cli/registry.py",
            "code_symbol": "COMMAND_REGISTRY['assay_chembl']",
            "pipeline": "assay",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "Config: configs/pipelines/assay/assay_chembl.yaml",
            "code_file": "configs/pipelines/assay/assay_chembl.yaml",
            "code_symbol": "assay.yaml",
            "pipeline": "assay",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "Class: ChemblAssayPipeline(PipelineBase)",
            "code_file": "src/bioetl/pipelines/chembl/assay.py",
            "code_symbol": "ChemblAssayPipeline",
            "pipeline": "assay",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
    ]

    testitem_contracts = [
        {
            "doc_point": "CLI command: testitem",
            "code_file": "src/bioetl/cli/registry.py",
            "code_symbol": "COMMAND_REGISTRY['testitem']",
            "pipeline": "testitem",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "Config: configs/pipelines/testitem/testitem_chembl.yaml",
            "code_file": "configs/pipelines/testitem/testitem_chembl.yaml",
            "code_symbol": "testitem.yaml",
            "pipeline": "testitem",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
        {
            "doc_point": "Class: TestItemChemblPipeline(PipelineBase)",
            "code_file": "src/bioetl/pipelines/chembl/testitem.py",
            "code_symbol": "TestItemChemblPipeline",
            "pipeline": "testitem",
            "contract": "обязательный",
            "status": "ok",
            "action": "verify",
        },
    ]

    matrix.extend(base_contracts)
    matrix.extend(activity_contracts)
    matrix.extend(assay_contracts)
    matrix.extend(testitem_contracts)

    return matrix


def write_matrix(artifacts_dir: Path | None = None) -> DocCodeMatrix:
    """Записывает матрицу в CSV и JSON, возвращает результат."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    rows = build_matrix()
    target_dir = artifacts_dir if artifacts_dir is not None else PROJECT_ROOT / "artifacts"
    target_dir.mkdir(parents=True, exist_ok=True)

    csv_path = target_dir / "matrix-doc-code.csv"
    json_path = target_dir / "matrix-doc-code.json"

    csv_tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    json_tmp = json_path.with_suffix(json_path.suffix + ".tmp")

    with csv_tmp.open("w", newline="", encoding="utf-8") as csv_handle:
        writer = csv.DictWriter(
            csv_handle,
            fieldnames=[
                "doc_point",
                "code_file",
                "code_symbol",
                "pipeline",
                "contract",
                "status",
                "action",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
        csv_handle.flush()

    with json_tmp.open("w", encoding="utf-8") as json_handle:
        json.dump(rows, json_handle, indent=2, ensure_ascii=False)
        json_handle.flush()

    csv_tmp.replace(csv_path)
    json_tmp.replace(json_path)

    log.info(
        "doc_code_matrix_written",
        rows=len(rows),
        csv=str(csv_path),
        json=str(json_path),
    )

    return DocCodeMatrix(rows=tuple(rows), csv_path=csv_path, json_path=json_path)
