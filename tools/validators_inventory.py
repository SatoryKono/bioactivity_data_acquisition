"""Generate inventory of validators and normalizers across schemas and validators modules."""
from __future__ import annotations

import ast
import json
import os
import tempfile
from collections import defaultdict
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import blake2b
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = PROJECT_ROOT / "src" / "bioetl"
SCHEMAS_ROOT = SRC_ROOT / "schemas"
VALIDATORS_FILE = SRC_ROOT / "core" / "validators.py"
TARGET_FILES = [VALIDATORS_FILE] + sorted(SCHEMAS_ROOT.rglob("*.py"))

IGNORED_FILES: set[Path] = {
    path for path in TARGET_FILES if path.name == "__init__.py"
}


@dataclass(slots=True)
class FunctionInfo:
    module: str
    name: str
    qualname: str
    filepath: Path
    lineno: int
    is_private: bool
    kind: str
    tags: set[str] = field(default_factory=set)
    duplicate_hash: str = ""
    callers: set[str] = field(default_factory=set)


def path_to_module(path: Path) -> str:
    relative = path.relative_to(SRC_ROOT)
    module_parts = list(relative.with_suffix("").parts)
    return "bioetl." + ".".join(module_parts)


def classify_function(name: str) -> str:
    normalized = name.lstrip("_")
    prefix_map: Mapping[str, str] = {
        "normalize_": "normalizer",
        "coerce_": "normalizer",
        "ensure_": "validator",
        "assert_": "validator",
        "validate_": "validator",
        "is_valid_": "validator",
        "is_": "validator",
    }
    for prefix, kind in prefix_map.items():
        if normalized.startswith(prefix):
            return kind
    return "utility"


def normalized_function_hash(node: ast.FunctionDef) -> str:
    body_nodes: Sequence[ast.stmt] = node.body
    if body_nodes:
        first_stmt = body_nodes[0]
        if isinstance(first_stmt, ast.Expr) and isinstance(first_stmt.value, ast.Constant) and isinstance(first_stmt.value.value, str):
            body_nodes = body_nodes[1:]
    module = ast.Module(body=list(body_nodes), type_ignores=[])
    dump_payload = {
        "args": ast.dump(node.args, include_attributes=False),
        "body": ast.dump(module, include_attributes=False),
    }
    serialized = json.dumps(dump_payload, sort_keys=True)
    digest = blake2b(serialized.encode("utf-8"), digest_size=16).hexdigest()
    return digest


def extract_tags(node: ast.FunctionDef) -> set[str]:
    tags: set[str] = set()
    for subnode in ast.walk(node):
        if isinstance(subnode, ast.Call):
            func = subnode.func
            if isinstance(func, ast.Name):
                if func.id in {"iter", "is_iterable"}:
                    tags.add("iterable-check")
                if func.id == "sorted":
                    tags.add("sorting")
                if func.id == "len":
                    tags.add("length-check")
            elif isinstance(func, ast.Attribute):
                attr_chain = resolve_attribute_name(func)
                if attr_chain in {"json.loads", "json.dumps"}:
                    tags.add("json-validation")
                if attr_chain.endswith(".strip"):
                    tags.add("whitespace-normalization")
                if attr_chain.endswith(".lower"):
                    tags.add("normalization")
                if attr_chain.endswith(".keys"):
                    tags.add("dict-keys")
                if attr_chain.endswith(".items"):
                    tags.add("dict-items")
        if isinstance(subnode, ast.Compare):
            for comparator in subnode.comparators:
                if isinstance(comparator, ast.Constant):
                    if comparator.value == 0:
                        tags.add("non-empty-check")
                    if isinstance(comparator.value, str) and comparator.value.startswith("http"):
                        tags.add("url-check")
        if isinstance(subnode, ast.Attribute):
            attr_chain = resolve_attribute_name(subnode)
            if attr_chain.startswith("datetime"):
                tags.add("datetime-check")
            if "timezone" in attr_chain or ".tz" in attr_chain:
                tags.add("timezone")
            if attr_chain.endswith(".strip"):
                tags.add("whitespace-normalization")
            if attr_chain.endswith(".lower"):
                tags.add("normalization")
            if attr_chain.endswith(".unique"):
                tags.add("uniqueness")
        if isinstance(subnode, ast.Name):
            if subnode.id in {"iter", "iterable"}:
                tags.add("iterable-check")
            if subnode.id in {"json", "json_module"}:
                tags.add("json-validation")
    return tags


def resolve_attribute_name(node: ast.Attribute) -> str:
    parts: list[str] = []
    current: ast.AST | None = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    parts.reverse()
    return ".".join(parts)


def collect_functions() -> dict[str, FunctionInfo]:
    functions: dict[str, FunctionInfo] = {}
    for path in TARGET_FILES:
        if path in IGNORED_FILES:
            continue
        module = path_to_module(path)
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef):
                qualname = f"{module}.{node.name}"
                info = FunctionInfo(
                    module=module,
                    name=node.name,
                    qualname=qualname,
                    filepath=path,
                    lineno=node.lineno,
                    is_private=node.name.startswith("_"),
                    kind=classify_function(node.name),
                )
                info.duplicate_hash = normalized_function_hash(node)
                info.tags = extract_tags(node)
                functions[qualname] = info
    return functions


@dataclass(slots=True)
class ImportAlias:
    module: str
    name: str | None = None


def build_module_imports(tree: ast.Module) -> MutableMapping[str, ImportAlias]:
    aliases: MutableMapping[str, ImportAlias] = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                asname = alias.asname or name
                aliases[asname] = ImportAlias(module=name)
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                asname = alias.asname or alias.name
                aliases[asname] = ImportAlias(module=node.module, name=alias.name)
    return aliases


def resolve_call(func: ast.AST, imports: Mapping[str, ImportAlias]) -> tuple[str, str] | None:
    if isinstance(func, ast.Name):
        alias = imports.get(func.id)
        if alias and alias.name:
            return alias.module, alias.name
        if alias and not alias.name:
            return alias.module, "__call__"
    elif isinstance(func, ast.Attribute):
        attr_chain: list[str] = []
        current: ast.AST | None = func
        while isinstance(current, ast.Attribute):
            attr_chain.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            alias = imports.get(current.id)
            if alias:
                attr_chain.append(alias.module if alias.name is None else alias.name)
                return alias.module, ".".join(reversed(attr_chain))
        attr_full = resolve_attribute_name(func)
        if attr_full.count(".") >= 1:
            module_part, attr_part = attr_full.rsplit(".", 1)
            return module_part, attr_part
    return None


def collect_usages(functions: Mapping[str, FunctionInfo]) -> None:
    target_modules = {info.module for info in functions.values()}
    for path in SRC_ROOT.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        imports = build_module_imports(tree)
        module_name = path_to_module(path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                resolved = resolve_call(node.func, imports)
                if resolved is None:
                    continue
                module, attr = resolved
                if module in target_modules:
                    possible_names: list[str] = []
                    if attr == "__call__" and isinstance(node.func, ast.Attribute):
                        possible_names.append(node.func.attr)
                    else:
                        possible_names.append(attr.split(".")[-1])
                    for candidate in possible_names:
                        qualname = f"{module}.{candidate}"
                        if qualname in functions:
                            functions[qualname].callers.add(module_name)


def detect_duplicates(functions: Mapping[str, FunctionInfo]) -> Mapping[str, list[str]]:
    duplicates: MutableMapping[str, list[str]] = defaultdict(list)
    for info in functions.values():
        duplicates[info.duplicate_hash].append(info.qualname)
    return duplicates


def build_notes(info: FunctionInfo, duplicates: Mapping[str, list[str]]) -> list[str]:
    notes: list[str] = []
    if info.tags:
        notes.append("tags=" + ",".join(sorted(info.tags)))
    dup_candidates = [name for name in duplicates[info.duplicate_hash] if name != info.qualname]
    if dup_candidates:
        notes.append("duplicates=" + ",".join(sorted(dup_candidates)))
    if not notes:
        notes.append("n/a")
    return notes


def write_json(functions: Mapping[str, FunctionInfo], duplicates: Mapping[str, list[str]]) -> None:
    output_path = PROJECT_ROOT / "build" / "validators_inventory.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    functions_payload: list[dict[str, Any]] = []
    for qualname in sorted(functions.keys()):
        info = functions[qualname]
        functions_payload.append(
            {
                "qualname": info.qualname,
                "module": info.module,
                "name": info.name,
                "filepath": str(info.filepath.relative_to(PROJECT_ROOT)),
                "lineno": info.lineno,
                "is_private": info.is_private,
                "kind": info.kind,
                "tags": sorted(info.tags),
                "callers": sorted(info.callers),
                "duplicates": [name for name in sorted(duplicates[info.duplicate_hash]) if name != info.qualname],
            }
        )
    payload: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "functions": functions_payload,
    }
    serialized = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
    write_atomically(output_path, serialized.encode("utf-8"))


def write_markdown(functions: Mapping[str, FunctionInfo], duplicates: Mapping[str, list[str]]) -> None:
    output_path = PROJECT_ROOT / "docs" / "schemas" / "00-inventory.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Инвентарь валидаторов и нормализаторов",
        "",
        "| Function | Kind | Modules | Notes |",
        "| --- | --- | --- | --- |",
    ]
    for qualname in sorted(functions.keys()):
        info = functions[qualname]
        modules = ", ".join(sorted(info.callers)) if info.callers else "n/a"
        notes = "; ".join(build_notes(info, duplicates))
        lines.append(f"| `{info.qualname}` | {info.kind} | {modules} | {notes} |")
    content = "\n".join(lines) + "\n"
    write_atomically(output_path, content.encode("utf-8"))


def write_atomically(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp_file:
        tmp_file.write(payload)
        tmp_file.flush()
        os.fsync(tmp_file.fileno())
        tmp_path = Path(tmp_file.name)
    tmp_path.replace(path)


def main() -> None:
    functions = collect_functions()
    collect_usages(functions)
    duplicates = detect_duplicates(functions)
    write_json(functions, duplicates)
    write_markdown(functions, duplicates)


if __name__ == "__main__":
    main()
