#!/usr/bin/env python3
"""Determinism check: повторные прогоны activity/assay/testitem в --dry-run."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).parent.parent
ARTIFACTS_DIR = ROOT / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def run_pipeline(pipeline: str, config_path: Path, output_dir: Path, run_id: int) -> Dict[str, Any]:
    """Запускает пайплайн с --dry-run и возвращает результаты."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "bioetl.cli.main",
            pipeline,
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
            "--dry-run",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    
    return {
        "run_id": run_id,
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "stdout_hash": hashlib.sha256(result.stdout.encode()).hexdigest() if result.stdout else None,
        "stderr_hash": hashlib.sha256(result.stderr.encode()).hexdigest() if result.stderr else None,
    }


def main():
    """Основная функция determinism check."""
    pipelines = [
        ("activity_chembl", ROOT / "configs" / "pipelines" / "chembl" / "activity.yaml"),
        ("assay_chembl", ROOT / "configs" / "pipelines" / "chembl" / "assay.yaml"),
        ("testitem", ROOT / "configs" / "pipelines" / "chembl" / "testitem.yaml"),
    ]
    
    output_dir = ROOT / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report = {}
    
    for pipeline_name, config_path in pipelines:
        if not config_path.exists():
            print(f"Warning: Config file not found: {config_path}")
            continue
        
        print(f"Running {pipeline_name} twice...")
        run1 = run_pipeline(pipeline_name, config_path, output_dir, 1)
        run2 = run_pipeline(pipeline_name, config_path, output_dir, 2)
        
        matches = {
            "exit_code": run1["exit_code"] == run2["exit_code"],
            "stdout_hash": run1["stdout_hash"] == run2["stdout_hash"],
            "stderr_hash": run1["stderr_hash"] == run2["stderr_hash"],
        }
        
        report[pipeline_name] = {
            "run1": run1,
            "run2": run2,
            "matches": matches,
            "deterministic": all(matches.values()),
        }
    
    output_file = ARTIFACTS_DIR / "determinism-check-report.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"Determinism check report saved to {output_file}")
    
    # Выводим краткую статистику
    for pipeline_name, result in report.items():
        status = "PASS" if result["deterministic"] else "FAIL"
        print(f"  {pipeline_name}: {status}")


if __name__ == "__main__":
    main()

