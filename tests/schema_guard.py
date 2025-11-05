#!/usr/bin/env python3
"""Schema guard: validate pipeline configurations against Pydantic models.

This script loads activity_chembl.yaml and assay_chembl.yaml configurations, validates them
through load_config (which uses Pydantic models), and reports any validation errors.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
AUDIT_RESULTS = ROOT / "audit_results"
CONFIGS = ROOT / "configs" / "pipelines" / "chembl"


def validate_config(config_path: Path) -> tuple[bool, dict[str, Any]]:
    """Validate a configuration file using load_config."""
    try:
        from bioetl.config.loader import load_config

        config = load_config(config_path)
        return True, {
            "config": config,
            "pipeline_name": config.pipeline.name if hasattr(config, "pipeline") else None,
            "validation_errors": [],
        }
    except Exception as e:
        return False, {
            "config": None,
            "pipeline_name": None,
            "validation_errors": [str(e)],
            "exception_type": type(e).__name__,
        }


def check_required_fields(config: Any, pipeline_name: str) -> list[str]:
    """Check that required fields are present in the config."""
    errors: list[str] = []

    # Check pipeline.name
    if not hasattr(config, "pipeline") or not hasattr(config.pipeline, "name"):
        errors.append("Missing required field: pipeline.name")
    elif config.pipeline.name != pipeline_name:
        errors.append(f"pipeline.name mismatch: expected {pipeline_name}, got {config.pipeline.name}")

    # Check sources.chembl
    if not hasattr(config, "sources") or not hasattr(config.sources, "chembl"):
        errors.append("Missing required field: sources.chembl")
    else:
        chembl_source = config.sources.chembl
        if not hasattr(chembl_source, "batch_size"):
            errors.append("Missing required field: sources.chembl.batch_size")
        elif hasattr(chembl_source, "batch_size") and chembl_source.batch_size > 25:
            errors.append(f"Invalid batch_size: {chembl_source.batch_size} (must be <= 25)")

    # Check determinism.sort
    if not hasattr(config, "determinism") or not hasattr(config.determinism, "sort"):
        errors.append("Missing required field: determinism.sort")
    else:
        sort_config = config.determinism.sort
        if not hasattr(sort_config, "by") or not sort_config.by:
            errors.append("Missing required field: determinism.sort.by")

    return errors


def main() -> int:
    """Main entry point."""
    print("Validating pipeline configurations...\n")

    configs_to_check = [
        ("activity", CONFIGS / "activity_chembl.yaml"),
        ("assay", CONFIGS / "assay_chembl.yaml"),
    ]

    results: dict[str, dict[str, Any]] = {}

    for pipeline_name, config_path in configs_to_check:
        print(f"Validating {pipeline_name} ({config_path.relative_to(ROOT)})...")

        if not config_path.exists():
            results[pipeline_name] = {
                "valid": False,
                "errors": [f"Configuration file not found: {config_path}"],
            }
            print(f"  ✗ File not found\n")
            continue

        valid, result = validate_config(config_path)

        if valid:
            # Check required fields
            field_errors = check_required_fields(result["config"], pipeline_name)
            if field_errors:
                result["validation_errors"].extend(field_errors)
                valid = False

        results[pipeline_name] = {
            "valid": valid,
            "config_path": str(config_path.relative_to(ROOT)),
            "pipeline_name": result.get("pipeline_name"),
            "errors": result.get("validation_errors", []),
            "exception_type": result.get("exception_type"),
        }

        if valid:
            print(f"  ✓ Valid\n")
        else:
            print(f"  ✗ Invalid: {len(result['validation_errors'])} error(s)\n")
            for error in result["validation_errors"]:
                print(f"    - {error}")

    # Generate report
    AUDIT_RESULTS.mkdir(exist_ok=True)
    report_path = AUDIT_RESULTS / "SCHEMA_GUARD_REPORT.md"

    total_valid = sum(1 for r in results.values() if r["valid"])
    total_invalid = len(results) - total_valid

    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Schema Guard Report\n\n")
        f.write("**Purpose**: Validate pipeline configurations against Pydantic models.\n\n")
        f.write(f"**Total configs tested**: {len(results)}\n\n")
        f.write(f"- ✅ Valid: {total_valid}\n")
        f.write(f"- ❌ Invalid: {total_invalid}\n\n")

        for pipeline_name, result in results.items():
            f.write(f"## {pipeline_name}\n\n")
            f.write(f"**Config Path**: `{result['config_path']}`\n\n")
            f.write(f"**Status**: {'✅ Valid' if result['valid'] else '❌ Invalid'}\n\n")

            if result.get("pipeline_name"):
                f.write(f"**Pipeline Name**: `{result['pipeline_name']}`\n\n")

            if result["errors"]:
                f.write("**Validation Errors**:\n\n")
                for error in result["errors"]:
                    f.write(f"- {error}\n")
                f.write("\n")

            if result.get("exception_type"):
                f.write(f"**Exception Type**: `{result['exception_type']}`\n\n")

    print(f"Report saved to {report_path}")

    # Return non-zero if any configs are invalid
    return 1 if total_invalid > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
