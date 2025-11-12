"""Static analysis helpers for enforcing the CLI ↔ QC import boundary."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

QC_MODULE_PREFIX = "bioetl.qc"
DEFAULT_PACKAGE = "bioetl.cli"
SRC_ROOT = Path("src")


@dataclass(frozen=True, slots=True)
class Violation:
    """Represents a boundary violation with the full import chain."""

    chain: tuple[str, ...]
    source_path: Path

    def format_chain(self) -> str:
        """Return a stable string representation of the import chain."""

        return " -> ".join(self.chain)


@dataclass(frozen=True, slots=True)
class ModuleRecord:
    """Metadata about a CLI module collected during graph construction."""

    path: Path
    module_name: str
    is_package: bool


@dataclass(frozen=True, slots=True)
class ModuleAnalysis:
    """Parsed import information for a module."""

    direct_qc_refs: tuple[str, ...]
    cli_dependencies: tuple[str, ...]


def collect_qc_boundary_violations(
    *,
    cli_root: Path | None = None,
    package: str = DEFAULT_PACKAGE,
) -> list[Violation]:
    """Return all CLI modules that (directly or indirectly) import ``bioetl.qc``."""

    root = cli_root or SRC_ROOT.joinpath(*package.split("."))
    module_records = _discover_modules(root=root, package=package)
    analyses = {
        name: _analyze_module(record, module_records) for name, record in module_records.items()
    }

    cache: dict[str, list[tuple[str, ...]]] = {}
    chains: set[tuple[str, ...]] = set()

    for module_name in sorted(module_records):
        for chain in _collect_chains(
            module_name=module_name,
            analyses=analyses,
            cache=cache,
            stack=(),
        ):
            chains.add(chain)

    violations = [
        Violation(chain=chain, source_path=module_records[chain[0]].path) for chain in chains
    ]
    violations.sort(key=lambda item: item.chain)
    return violations


def _discover_modules(*, root: Path, package: str) -> dict[str, ModuleRecord]:
    module_records: dict[str, ModuleRecord] = {}

    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root)
        parts = list(relative.with_suffix("").parts)
        if parts and parts[-1] == "__init__":
            is_package = True
            parts = parts[:-1]
        else:
            is_package = False

        module_suffix = ".".join(parts)
        if module_suffix:
            module_name = f"{package}.{module_suffix}"
        else:
            module_name = package

        module_records[module_name] = ModuleRecord(
            path=path,
            module_name=module_name,
            is_package=is_package,
        )

    return module_records


def _analyze_module(record: ModuleRecord, module_records: dict[str, ModuleRecord]) -> ModuleAnalysis:
    source = record.path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(record.path))

    direct_qc_refs: set[str] = set()
    cli_dependencies: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if _is_qc_module(name):
                    direct_qc_refs.add(_normalize_qc_name(name))
                for candidate in _iter_cli_candidates(name, module_records):
                    cli_dependencies.add(candidate)
        elif isinstance(node, ast.ImportFrom):
            base_module = _resolve_import_from(
                current=record,
                module=node.module,
                level=node.level,
            )

            targets = _resolve_import_targets(base_module, node.names)
            for target in targets:
                if _is_qc_module(target):
                    direct_qc_refs.add(_normalize_qc_name(target))
                for candidate in _iter_cli_candidates(target, module_records):
                    cli_dependencies.add(candidate)

    return ModuleAnalysis(
        direct_qc_refs=tuple(sorted(direct_qc_refs)),
        cli_dependencies=tuple(sorted(cli_dependencies)),
    )


def _collect_chains(
    *,
    module_name: str,
    analyses: dict[str, ModuleAnalysis],
    cache: dict[str, list[tuple[str, ...]]],
    stack: tuple[str, ...],
) -> Iterable[tuple[str, ...]]:
    if module_name in cache:
        return cache[module_name]

    if module_name in stack:
        return ()

    analysis = analyses[module_name]
    chains: list[tuple[str, ...]] = []

    for direct in analysis.direct_qc_refs:
        chains.append((module_name, direct))

    for dependency in analysis.cli_dependencies:
        for sub_chain in _collect_chains(
            module_name=dependency,
            analyses=analyses,
            cache=cache,
            stack=stack + (module_name,),
        ):
            chains.append((module_name, *sub_chain))

    cache[module_name] = chains
    return chains


def _resolve_import_from(*, current: ModuleRecord, module: str | None, level: int) -> str | None:
    if level == 0:
        return module

    base_parts = current.module_name.split(".")
    if not current.is_package:
        base_parts = base_parts[:-1]

    if level > len(base_parts):
        return module

    parent_length = len(base_parts) - (level - 1)
    parent_parts = base_parts[:parent_length]

    if module:
        parent_parts.extend(module.split("."))

    if not parent_parts:
        return None

    return ".".join(parent_parts)


def _resolve_import_targets(base: str | None, aliases: list[ast.alias]) -> set[str]:
    targets: set[str] = set()
    if base is not None:
        targets.add(base)

    if base:
        for alias in aliases:
            if alias.name == "*":
                continue
            targets.add(f"{base}.{alias.name}")
    elif not base:
        for alias in aliases:
            if alias.name != "*":
                targets.add(alias.name)

    return targets


def _iter_cli_candidates(name: str, module_records: dict[str, ModuleRecord]) -> Iterable[str]:
    if name in module_records:
        yield name


def _is_qc_module(module_name: str) -> bool:
    return module_name == QC_MODULE_PREFIX or module_name.startswith(f"{QC_MODULE_PREFIX}.")


def _normalize_qc_name(module_name: str) -> str:
    parts = module_name.split(".")
    prefix_parts = QC_MODULE_PREFIX.split(".")
    return ".".join(prefix_parts + parts[len(prefix_parts) :])


