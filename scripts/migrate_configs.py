"""Validate and migrate pipeline configuration files."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, MutableMapping, Sequence

import yaml

from bioetl.config.loader import ConfigValidationError, load_pipeline_config

_CONFIG_EXTENSIONS: tuple[str, ...] = (".yaml", ".yml")


def discover_pipeline_configs(root: Path) -> list[Path]:
    """Return all pipeline config files under ``root`` sorted for stability."""

    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in _CONFIG_EXTENSIONS
    )


def _migrate_select_fields(payload: MutableMapping[str, object]) -> bool:
    """Move legacy ``select_fields`` into ``parameters.select_fields`` blocks."""

    sources = payload.get("sources")
    if not isinstance(sources, MutableMapping):
        return False

    changed = False
    for source_payload in sources.values():
        if not isinstance(source_payload, MutableMapping):
            continue
        legacy_select = source_payload.pop("select_fields", None)
        if legacy_select is None:
            continue
        parameters = source_payload.get("parameters")
        if not isinstance(parameters, MutableMapping):
            parameters = {}
        if "select_fields" not in parameters:
            parameters["select_fields"] = legacy_select
            source_payload["parameters"] = parameters
            changed = True
    return changed


def migrate_config_file(path: Path, *, apply: bool = False) -> bool:
    """Apply in-place migrations and return ``True`` when changes occurred."""

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, MutableMapping):
        return False

    changed = _migrate_select_fields(payload)
    if changed and apply:
        with path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)
    return changed


def _validate_configs(config_paths: Sequence[Path]) -> list[tuple[Path, Exception]]:
    failures: list[tuple[Path, Exception]] = []
    for path in config_paths:
        try:
            load_pipeline_config(path)
        except (ConfigValidationError, FileNotFoundError) as exc:
            failures.append((path, exc))
    return failures


def _format_failures(failures: Iterable[tuple[Path, Exception]]) -> str:
    return "\n".join(f"- {path}: {exc}" for path, exc in failures)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("configs/pipelines"),
        help="Root directory containing pipeline configs.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply migrations in-place instead of reporting them.",
    )
    args = parser.parse_args(argv)

    root = args.root.expanduser()
    config_paths = discover_pipeline_configs(root)
    if not config_paths:
        print(f"No configs found under {root}")
        return 0

    migrated: list[Path] = []
    for path in config_paths:
        if migrate_config_file(path, apply=args.apply):
            migrated.append(path)

    failures = _validate_configs(config_paths)

    if migrated and not args.apply:
        print("Configurations require migration. Re-run with --apply to update:")
        print("\n".join(str(path) for path in migrated))
        return 1

    if failures:
        print("Configuration validation failed:\n" + _format_failures(failures))
        return 1

    if migrated:
        print("Applied migrations:\n" + "\n".join(str(path) for path in migrated))
    else:
        print("All configurations are valid")
    return 0


if __name__ == "__main__":  # pragma: no cover - manual execution
    raise SystemExit(main())
