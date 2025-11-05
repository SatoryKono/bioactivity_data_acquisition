#!/usr/bin/env python3
"""Создание матрицы трассировки Doc↔Code для base/activity/assay/testitem."""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).parent.parent
AUDIT_RESULTS = ROOT / "audit_results"
AUDIT_RESULTS.mkdir(exist_ok=True)


def create_matrix() -> List[Dict[str, Any]]:
    """Создаёт матрицу трассировки Doc↔Code."""
    matrix = []
    
    # PipelineBase контракты
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
    
    # Activity контракты
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
            "doc_point": "Config: configs/pipelines/chembl/activity.yaml",
            "code_file": "configs/pipelines/chembl/activity.yaml",
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
    
    # Assay контракты
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
            "doc_point": "Config: configs/pipelines/chembl/assay.yaml",
            "code_file": "configs/pipelines/chembl/assay.yaml",
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
    
    # TestItem контракты
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
            "doc_point": "Config: configs/pipelines/chembl/testitem.yaml",
            "code_file": "configs/pipelines/chembl/testitem.yaml",
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


def main():
    """Основная функция создания матрицы."""
    print("Creating Doc<->Code matrix...")
    matrix = create_matrix()
    
    # Сохраняем в CSV
    csv_file = AUDIT_RESULTS / "matrix-doc-code.csv"
    with csv_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["doc_point", "code_file", "code_symbol", "pipeline", "contract", "status", "action"],
        )
        writer.writeheader()
        writer.writerows(matrix)
    
    print(f"Matrix saved to {csv_file}")
    
    # Сохраняем в JSON
    json_file = AUDIT_RESULTS / "matrix-doc-code.json"
    with json_file.open("w", encoding="utf-8") as f:
        json.dump(matrix, f, indent=2, ensure_ascii=False)
    
    print(f"Matrix saved to {json_file}")
    print(f"\nTotal contracts: {len(matrix)}")
    
    # Статистика по статусам
    status_counts = {}
    for item in matrix:
        status = item["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print("\nStatus breakdown:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")


if __name__ == "__main__":
    main()

