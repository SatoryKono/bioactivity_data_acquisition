"""Validate pipeline configurations and schema registry consistency."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.core.logging import UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.schemas import SCHEMA_REGISTRY
from bioetl.tools import get_project_root

__all__ = ["run_schema_guard"]


PROJECT_ROOT = get_project_root()
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
CONFIGS = PROJECT_ROOT / "configs" / "pipelines" / "chembl"


def _validate_config(config_path: Path) -> tuple[bool, dict[str, Any]]:
    """Load a pipeline config and return validation metadata."""
    try:
        from bioetl.config.loader import load_config

        config = load_config(config_path)
        return True, {
            "config": config,
            "pipeline_name": getattr(getattr(config, "pipeline", None), "name", None),
            "validation_errors": [],
        }
    except Exception as exc:  # noqa: BLE001 - configuration failure surfaced
        return False, {
            "config": None,
            "pipeline_name": None,
            "validation_errors": [str(exc)],
            "exception_type": type(exc).__name__,
        }


def _check_required_fields(config: Any, pipeline_name: str) -> list[str]:
    """Validate required configuration fields for the specified pipeline."""
    errors: list[str] = []

    if not hasattr(config, "pipeline") or not hasattr(config.pipeline, "name"):
        errors.append("Missing required field: pipeline.name")
    elif config.pipeline.name != pipeline_name:
        errors.append(
            f"pipeline.name mismatch: expected {pipeline_name}, got {config.pipeline.name}"
        )

    if not hasattr(config, "sources") or not hasattr(config.sources, "chembl"):
        errors.append("Missing required field: sources.chembl")
    else:
        chembl_source = config.sources.chembl
        if not hasattr(chembl_source, "batch_size"):
            errors.append("Missing required field: sources.chembl.batch_size")
        elif getattr(chembl_source, "batch_size", 0) > 25:
            errors.append(f"Invalid batch_size: {chembl_source.batch_size} (must be <= 25)")

    if not hasattr(config, "determinism") or not hasattr(config.determinism, "sort"):
        errors.append("Missing required field: determinism.sort")
    else:
        sort_config = config.determinism.sort
        if not getattr(sort_config, "by", None):
            errors.append("Missing required field: determinism.sort.by")

    return errors


def _validate_schema_registry() -> list[str]:
    """Validate schema registry invariants and return error messages."""
    errors: list[str] = []
    for identifier, entry in SCHEMA_REGISTRY.as_mapping().items():
        schema_columns = list(entry.schema.columns.keys())
        column_order = list(entry.column_order)

        if len(set(column_order)) != len(column_order):
            errors.append(f"{identifier}: column_order contains duplicates")

        missing = [name for name in column_order if name not in schema_columns]
        if missing:
            errors.append(f"{identifier}: column_order references missing columns {missing}")

        for hashed_column in ("hash_row", "hash_business_key"):
            if hashed_column not in schema_columns:
                errors.append(f"{identifier}: missing required column '{hashed_column}'")
            elif hashed_column not in column_order:
                errors.append(f"{identifier}: '{hashed_column}' not present in column_order")

        module_path, _ = identifier.rsplit(".", 1)
        module = __import__(module_path, fromlist=["__name__"])
        declared_version = getattr(module, "SCHEMA_VERSION", None)
        if declared_version is not None and str(declared_version) != entry.version:
            errors.append(
                f"{identifier}: version mismatch (registry={entry.version}, module={declared_version})"
            )

    return errors


def _write_report(results: dict[str, dict[str, Any]], registry_errors: list[str]) -> Path:
    """Write a schema guard markdown report and return its path."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = ARTIFACTS_DIR / "schema_guard_report.md"
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")

    total_valid = sum(1 for item in results.values() if item["valid"])
    total_invalid = len(results) - total_valid

    with tmp.open("w", encoding="utf-8") as handle:
        handle.write("# Schema Guard Report\n\n")
        handle.write("**Purpose**: Validate pipeline configurations against Pydantic models.\n\n")
        handle.write(f"**Total configs tested**: {len(results)}\n\n")
        handle.write(f"- ✅ Valid: {total_valid}\n")
        handle.write(f"- ❌ Invalid: {total_invalid}\n\n")

        for pipeline_name, result in results.items():
            handle.write(f"## {pipeline_name}\n\n")
            status = "✅ Valid" if result["valid"] else "❌ Invalid"
            handle.write(f"**Config Path**: `{result['config_path']}`\n\n")
            handle.write(f"**Status**: {status}\n\n")

            if result.get("pipeline_name"):
                handle.write(f"**Pipeline Name**: `{result['pipeline_name']}`\n\n")

            if result["errors"]:
                handle.write("**Validation Errors**:\n\n")
                for error in result["errors"]:
                    handle.write(f"- {error}\n")
                handle.write("\n")

            if result.get("exception_type"):
                handle.write(f"**Exception Type**: `{result['exception_type']}`\n\n")

        handle.write("## Schema Registry\n\n")
        if registry_errors:
            handle.write("**Status**: ❌ Invalid\n\n")
            handle.write("**Errors**:\n\n")
            for error in registry_errors:
                handle.write(f"- {error}\n")
            handle.write("\n")
        else:
            handle.write("**Status**: ✅ Valid\n\n")

    tmp.replace(report_path)
    return report_path


def run_schema_guard() -> tuple[dict[str, dict[str, Any]], list[str], Path]:
    """Execute configuration and schema validation, returning results and report path."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    configs_to_check = {
        "activity": CONFIGS / "activity.yaml",
        "assay": CONFIGS / "assay.yaml",
    }

    results: dict[str, dict[str, Any]] = {}

    for pipeline_name, config_path in configs_to_check.items():
        log.info(LogEvents.VALIDATING_CONFIG, pipeline=pipeline_name, path=str(config_path))
        if not config_path.exists():
            results[pipeline_name] = {
                "valid": False,
                "config_path": str(config_path.relative_to(PROJECT_ROOT)),
                "pipeline_name": None,
                "errors": [f"Configuration file not found: {config_path}"],
                "exception_type": None,
            }
            log.warning(LogEvents.CONFIG_MISSING, pipeline=pipeline_name)
            continue

        valid, result = _validate_config(config_path)

        if valid:
            field_errors = _check_required_fields(result["config"], pipeline_name)
            if field_errors:
                result["validation_errors"].extend(field_errors)
                valid = False

        results[pipeline_name] = {
            "valid": valid,
            "config_path": str(config_path.relative_to(PROJECT_ROOT)),
            "pipeline_name": result.get("pipeline_name"),
            "errors": result.get("validation_errors", []),
            "exception_type": result.get("exception_type"),
        }

        if valid:
            log.info(LogEvents.CONFIG_VALID, pipeline=pipeline_name)
        else:
            log.warning(LogEvents.CONFIG_INVALID,
                pipeline=pipeline_name,
                errors=len(results[pipeline_name]["errors"]),
            )

    registry_errors = _validate_schema_registry()
    if registry_errors:
        log.warning(LogEvents.SCHEMA_REGISTRY_INVALID, errors=len(registry_errors))
    else:
        log.info(LogEvents.SCHEMA_REGISTRY_VALID)

    report_path = _write_report(results, registry_errors)
    log.info(LogEvents.SCHEMA_GUARD_REPORT_WRITTEN, path=str(report_path))

    return results, registry_errors, report_path
