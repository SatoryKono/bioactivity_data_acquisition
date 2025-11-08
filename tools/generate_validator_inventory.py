"""Generate inventory of validator/normalizer utilities.

The script scans ``src/bioetl/schemas`` and ``src/bioetl/core/validators.py``
to build a catalogue of helper functions that participate in data validation
and normalisation. It emits two artefacts:

* ``build/validators_inventory.json`` – machine-readable description;
* ``docs/schemas/00-inventory.md`` – human-readable summary table.

Usage
-----
Run the script from the project root:

.. code-block:: bash

    python tools/generate_validator_inventory.py

The command requires no arguments and overwrites the target artefacts.
"""

from __future__ import annotations

import ast
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"

SCHEMA_ROOT = SRC_ROOT / "bioetl" / "schemas"
CORE_VALIDATORS_FILE = SRC_ROOT / "bioetl" / "core" / "validators.py"

OUTPUT_JSON = PROJECT_ROOT / "build" / "validators_inventory.json"
OUTPUT_MD = PROJECT_ROOT / "docs" / "schemas" / "00-inventory.md"

FUNCTION_PREFIXES = (
    "is_valid_",
    "validate_",
    "normalize_",
    "coerce_",
    "ensure_",
    "is_",
    "assert_",
)


@dataclass
class FunctionInfo:
    """Container describing a discovered helper function."""

    module: str
    path: Path
    name: str
    classification: str
    doc: str | None
    usages: set[str] = field(default_factory=set)
    notes: list[str] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.module}.{self.name}"

    def to_json(self) -> Mapping[str, Any]:
        return {
            "name": self.name,
            "module": self.module,
            "path": str(self.path),
            "classification": self.classification,
            "doc": self.doc,
            "usages": sorted(self.usages),
            "notes": self.notes,
        }


def iter_target_files() -> Iterator[Path]:
    yield CORE_VALIDATORS_FILE
    for path in sorted(SCHEMA_ROOT.rglob("*.py")):
        yield path


def strip_private_prefix(name: str) -> str:
    return name.lstrip("_")


def is_target_function(name: str) -> bool:
    public_name = strip_private_prefix(name)
    return any(public_name.startswith(prefix) for prefix in FUNCTION_PREFIXES)


def classify_function(name: str) -> str:
    public_name = strip_private_prefix(name)
    if public_name.startswith(("normalize_", "coerce_")):
        return "normalizer"
    if public_name.startswith(("ensure_", "validate_", "is_valid_", "is_", "assert_")):
        return "validator"
    return "utility"


def path_to_module(path: Path) -> str:
    relative = path.with_suffix("")
    if relative.is_absolute():
        relative = relative.relative_to(SRC_ROOT)
    parts = list(relative.parts)
    return ".".join(parts)


def collect_functions() -> dict[str, FunctionInfo]:
    functions: dict[str, FunctionInfo] = {}
    for file_path in iter_target_files():
        module_name = path_to_module(file_path)
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and is_target_function(node.name):
                doc = ast.get_docstring(node)
                info = FunctionInfo(
                    module=module_name,
                    path=file_path,
                    name=node.name,
                    classification=classify_function(node.name),
                    doc=doc.splitlines()[0] if doc else None,
                )
                functions[info.full_name] = info
    return functions


def resolve_import_module(
    current_module: str, *, level: int, module: str | None
) -> str:
    if level == 0:
        return module or ""

    parts = current_module.split(".")
    if not current_module.endswith("__init__"):
        parts = parts[:-1]

    levels_up = level - 1
    if levels_up:
        if levels_up >= len(parts):
            parts = []
        else:
            parts = parts[: len(parts) - levels_up]

    if module:
        parts.extend(module.split("."))

    return ".".join(part for part in parts if part)


def analyse_usages(functions: Mapping[str, FunctionInfo]) -> None:
    alias_objects: Dict[str, str]
    alias_modules: Dict[str, str]

    def record_usage(function_name: str, module: str) -> None:
        if function_name in functions:
            functions[function_name].usages.add(module)

    def resolve_candidates(expr: ast.AST, module: str) -> set[str]:
        candidates: set[str] = set()
        if isinstance(expr, ast.Name):
            name = expr.id
            target = alias_objects.get(name)
            if target:
                candidates.add(target)
            if module:
                candidates.add(f"{module}.{name}")
        elif isinstance(expr, ast.Attribute):
            attr_parts: list[str] = []
            current = expr
            while isinstance(current, ast.Attribute):
                attr_parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                base_name = current.id
                attr_parts.append(base_name)
                attr_parts.reverse()
                base_resolved = alias_modules.get(attr_parts[0], attr_parts[0])
                candidate = ".".join([base_resolved, *attr_parts[1:]])
                candidates.add(candidate)
                dotted = ".".join(attr_parts)
                candidates.add(dotted)
        return candidates

    python_files = sorted(PROJECT_ROOT.rglob("*.py"))
    for file_path in python_files:
        if "__pycache__" in file_path.parts:
            continue
        if file_path.name == "__init__.py" and file_path.stat().st_size == 0:
            continue
        module_name = path_to_module(file_path.relative_to(SRC_ROOT)) if file_path.is_relative_to(SRC_ROOT) else None
        if module_name is None:
            if file_path.is_relative_to(PROJECT_ROOT / "tests"):
                module_name = "tests." + file_path.with_suffix("").relative_to(PROJECT_ROOT / "tests").as_posix().replace("/", ".")
            else:
                continue

        source = file_path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source, filename=str(file_path))
        except SyntaxError:
            continue

        alias_objects = {}
        alias_modules = {}

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.asname or alias.name.split(".")[-1]
                    alias_modules[name] = alias.name
            elif isinstance(node, ast.ImportFrom):
                base_module = resolve_import_module(
                    module_name, level=node.level, module=node.module
                )
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    alias_name = alias.asname or alias.name
                    target = ".".join(filter(None, [base_module, alias.name]))
                    alias_objects[alias_name] = target
                    alias_modules.setdefault(alias_name, target)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for candidate in resolve_candidates(node.func, module_name):
                    record_usage(candidate, module_name)
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                for candidate in resolve_candidates(node, module_name):
                    record_usage(candidate, module_name)
            elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
                for candidate in resolve_candidates(node, module_name):
                    record_usage(candidate, module_name)


def enrich_notes(functions: Mapping[str, FunctionInfo]) -> None:
    names_to_modules: dict[str, list[str]] = defaultdict(list)
    for info in functions.values():
        names_to_modules[info.name].append(info.module)

    for info in functions.values():
        duplicates = [module for module in names_to_modules[info.name] if module != info.module]
        if duplicates:
            info.notes.append(
                "Дубликат имени в: " + ", ".join(sorted(duplicates))
            )
        if info.doc:
            info.notes.append(info.doc)
        if not info.usages:
            info.notes.append("Не обнаружено прямых использований")


def write_outputs(functions: Mapping[str, FunctionInfo]) -> None:
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)

    ordered = sorted(functions.values(), key=lambda item: item.full_name)

    data = [info.to_json() for info in ordered]
    OUTPUT_JSON.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    lines = ["# Инвентарь валидаторов и нормализаторов", ""]
    lines.append("| Функция | Тип | Использование | Заметки |")
    lines.append("| --- | --- | --- | --- |")
    for info in ordered:
        uses = "<br>".join(sorted(info.usages)) or "—"
        notes = "<br>".join(info.notes) if info.notes else "—"
        lines.append(
            f"| `{info.full_name}` | {info.classification} | {uses} | {notes} |"
        )
    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    functions = collect_functions()
    analyse_usages(functions)
    enrich_notes(functions)
    write_outputs(functions)


if __name__ == "__main__":
    main()

