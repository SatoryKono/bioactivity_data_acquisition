from __future__ import annotations

import ast
import csv
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
BIOETL_ROOT = REPO_ROOT / "src" / "bioetl"

TARGET_SUBDIRS = [
    BIOETL_ROOT / "clients",
    BIOETL_ROOT / "config",
    BIOETL_ROOT / "qc",
    BIOETL_ROOT / "schemas",
    BIOETL_ROOT / "cli",
]

# Normalise schema/shemas typo if present
SCHEMAS_DIR = BIOETL_ROOT / "schemas"
SHEMAS_DIR = BIOETL_ROOT / "shemas"

NAMING_RULES_PATH = REPO_ROOT / "naming_rules.json"


@dataclass
class Rule:
    rule_id: str
    scope: str
    kind: str
    pattern: str


def load_rules() -> List[Rule]:
    with NAMING_RULES_PATH.open(encoding="utf-8") as fh:
        data = json.load(fh)
    return [
        Rule(rule["id"], rule["scope"], rule["kind"], rule["pattern"])
        for rule in data["rules"]
    ]


RULES = load_rules()

KIND_ALIASES: Dict[str, Tuple[str, ...]] = {
    "pkg": ("module",),
    "module": ("module", "file"),
    "script": ("file",),
    "class": ("class",),
    "func": ("func",),
    "const": ("const",),
    "exception": ("exception",),
}

STATUS_OK = "OK"
STATUS_VIOL = "VIOL"


@dataclass
class InventoryItem:
    path: str
    kind: str
    name: str
    pattern_expected: str
    pattern_actual: str
    status: str


def gather_scope_mapping() -> Dict[Path, str]:
    scope_map: Dict[Path, str] = {}
    for subdir in TARGET_SUBDIRS:
        scope_map[subdir.resolve()] = subdir.relative_to(BIOETL_ROOT).parts[0]
    if SHEMAS_DIR.exists() and not SCHEMAS_DIR.exists():
        scope_map[SHEMAS_DIR.resolve()] = "schemas"
    elif SHEMAS_DIR.exists() and SCHEMAS_DIR.exists():
        scope_map[SHEMAS_DIR.resolve()] = "schemas"
    return scope_map


SCOPE_MAP = gather_scope_mapping()


def find_scope(path: Path) -> str:
    abs_path = path.resolve()
    for target, scope in SCOPE_MAP.items():
        try:
            abs_path.relative_to(target)
        except ValueError:
            continue
        return scope
    raise ValueError(f"Path {path} outside target directories")


def select_rule(scope: str, kind: str) -> Rule:
    aliases = KIND_ALIASES.get(kind, (kind,))
    candidates: List[Tuple[int, int, int, Rule]] = []
    for rule in RULES:
        if rule.kind not in aliases:
            continue
        kind_rank = aliases.index(rule.kind)
        if rule.scope == scope:
            scope_rank = 0
        elif rule.scope == "all":
            scope_rank = 1
        else:
            continue
        candidates.append((scope_rank, kind_rank, -len(rule.pattern), rule))
    if not candidates:
        raise ValueError(f"No rule for scope={scope}, kind={kind}")
    candidates.sort()
    return candidates[0][3]


def match_status(pattern: str, name: str) -> str:
    return STATUS_OK if re.fullmatch(pattern, name) else STATUS_VIOL


def iter_python_files(base: Path) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(base):
        dir_path = Path(dirpath)
        if "__pycache__" in dir_path.parts:
            continue
        for filename in filenames:
            if filename.endswith(".py"):
                yield dir_path / filename


def is_constant_name(name: str) -> bool:
    return name.isupper() and re.fullmatch(r"[A-Z][A-Z0-9_]*", name) is not None


def classify_class(node: ast.ClassDef) -> str:
    if node.name.endswith("Error"):
        return "exception"
    return "class"


def collect_inventory() -> List[InventoryItem]:
    entries: List[InventoryItem] = []
    target_dirs = list(TARGET_SUBDIRS)
    if SHEMAS_DIR.exists() and SHEMAS_DIR not in target_dirs:
        target_dirs.append(SHEMAS_DIR)

    # packages
    for target in target_dirs:
        if not target.exists():
            continue
        scope = find_scope(target)
        for dirpath, dirnames, filenames in os.walk(target):
            dir_path = Path(dirpath)
            if "__pycache__" in dir_path.parts:
                continue
            if "__init__.py" in filenames:
                rel_dir = dir_path.relative_to(REPO_ROOT)
                rule = select_rule(scope, "pkg")
                pattern_actual = dir_path.name
                entries.append(
                    InventoryItem(
                        path=rel_dir.as_posix(),
                        kind="pkg",
                        name=dir_path.name,
                        pattern_expected=f"{rule.rule_id}: {rule.pattern}",
                        pattern_actual=pattern_actual,
                        status=match_status(rule.pattern, pattern_actual),
                    )
                )

    # modules and contents
    for target in target_dirs:
        if not target.exists():
            continue
        scope = find_scope(target)
        for file_path in iter_python_files(target):
            rel_path = file_path.relative_to(REPO_ROOT)
            module_rule = select_rule(scope, "module")
            module_actual = file_path.name if module_rule.kind == "file" else file_path.stem
            entries.append(
                InventoryItem(
                    path=rel_path.as_posix(),
                    kind="module",
                    name=file_path.stem,
                    pattern_expected=f"{module_rule.rule_id}: {module_rule.pattern}",
                    pattern_actual=module_actual,
                    status=match_status(module_rule.pattern, module_actual),
                )
            )
            try:
                source = file_path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            try:
                tree = ast.parse(source, filename=str(rel_path))
            except SyntaxError:
                continue
            for node in tree.body:
                if isinstance(node, ast.ClassDef):
                    kind = classify_class(node)
                    rule = select_rule(scope, kind)
                    entries.append(
                        InventoryItem(
                            path=rel_path.as_posix(),
                            kind=kind,
                            name=node.name,
                            pattern_expected=f"{rule.rule_id}: {rule.pattern}",
                            pattern_actual=node.name,
                            status=match_status(rule.pattern, node.name),
                        )
                    )
                elif isinstance(node, ast.FunctionDef):
                    rule = select_rule(scope, "func")
                    entries.append(
                        InventoryItem(
                            path=rel_path.as_posix(),
                            kind="func",
                            name=node.name,
                            pattern_expected=f"{rule.rule_id}: {rule.pattern}",
                            pattern_actual=node.name,
                            status=match_status(rule.pattern, node.name),
                        )
                    )
                elif isinstance(node, ast.Assign):
                    for target_node in node.targets:
                        if isinstance(target_node, ast.Name):
                            name = target_node.id
                            if is_constant_name(name):
                                rule = select_rule(scope, "const")
                                entries.append(
                                    InventoryItem(
                                        path=rel_path.as_posix(),
                                        kind="const",
                                        name=name,
                                        pattern_expected=f"{rule.rule_id}: {rule.pattern}",
                                        pattern_actual=name,
                                        status=match_status(rule.pattern, name),
                                    )
                                )
                elif isinstance(node, ast.AnnAssign):
                    target_node = node.target
                    if isinstance(target_node, ast.Name):
                        name = target_node.id
                        if is_constant_name(name):
                            rule = select_rule(scope, "const")
                            entries.append(
                                InventoryItem(
                                    path=rel_path.as_posix(),
                                    kind="const",
                                    name=name,
                                    pattern_expected=f"{rule.rule_id}: {rule.pattern}",
                                    pattern_actual=name,
                                    status=match_status(rule.pattern, name),
                                )
                            )

    if SHEMAS_DIR.exists() and SCHEMAS_DIR.exists():
        rel_path = SHEMAS_DIR.relative_to(REPO_ROOT)
        rule = select_rule("schemas", "pkg")
        entries.append(
            InventoryItem(
                path=rel_path.as_posix(),
                kind="pkg",
                name=SHEMAS_DIR.name,
                pattern_expected=f"{rule.rule_id}: {rule.pattern}",
                pattern_actual=SHEMAS_DIR.name,
                status=STATUS_VIOL,
            )
        )
    return entries


def build_summary(items: List[InventoryItem]) -> Dict[str, Dict[str, Dict[str, int]]]:
    by_directory: DefaultDict[str, Counter[str]] = defaultdict(Counter)
    by_kind: DefaultDict[str, Counter[str]] = defaultdict(Counter)
    for item in items:
        parts = Path(item.path).parts
        directory_key = "/".join(parts[:3]) if len(parts) >= 3 else str(Path(item.path).parent)
        by_directory[directory_key][item.status] += 1
        by_directory[directory_key]["total"] += 1
        by_kind[item.kind][item.status] += 1
        by_kind[item.kind]["total"] += 1
    summary: Dict[str, Dict[str, Dict[str, int]]] = {
        "by_directory": {key: dict(counter) for key, counter in sorted(by_directory.items())},
        "by_kind": {key: dict(counter) for key, counter in sorted(by_kind.items())},
    }
    return summary


def atomic_write_csv(path: Path, rows: List[InventoryItem]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["path", "kind", "name", "pattern_expected", "pattern_actual", "status"])
        for row in rows:
            writer.writerow([row.path, row.kind, row.name, row.pattern_expected, row.pattern_actual, row.status])
    os.replace(tmp_path, path)


def atomic_write_json(path: Path, data: Dict[str, Dict[str, Dict[str, int]]]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(tmp_path, path)


def main() -> None:
    items = collect_inventory()
    items.sort(key=lambda item: (item.path, item.kind, item.name))
    summary = build_summary(items)
    output_dir = REPO_ROOT
    atomic_write_csv(output_dir / "inventory.csv", items)
    atomic_write_json(output_dir / "inventory_summary.json", summary)


if __name__ == "__main__":
    main()

