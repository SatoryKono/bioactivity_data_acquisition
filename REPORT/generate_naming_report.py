"""Формирование отчёта о нарушениях нейминга и предложений переименований.

Скрипт читает `inventory.csv` и `naming_rules.json`, вычисляя корректные имена
для всех сущностей со статусом VIOL. Результат сохраняется в `violations.csv`
и `rename_map.json` в корне репозитория.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
INVENTORY_PATH = ROOT_DIR / "inventory.csv"
RULES_PATH = ROOT_DIR / "naming_rules.json"
VIOLATIONS_OUTPUT = ROOT_DIR / "violations.csv"
RENAME_MAP_OUTPUT = ROOT_DIR / "rename_map.json"


class Scope(str, Enum):
    """Тип сущности, требующей переименования."""

    FILE = "file"
    SYMBOL = "symbol"


class Impact(str, Enum):
    """Категория влияния переименования."""

    CLI = "cli"
    IMPORTS = "imports"
    CONFIGS = "configs"
    TESTS = "tests"
    DOCS = "docs"


@dataclass(frozen=True)
class Rule:
    """Описание правила нейминга."""

    rule_id: str
    scope: str
    kind: str
    pattern: str


@dataclass
class InventoryRow:
    """Строка из inventory.csv."""

    path: Path
    kind: str
    name: str
    rule: Rule
    status: str


@dataclass
class Suggestion:
    """Предложение переименования."""

    rule_id: str
    found_name: str
    suggested_name: str
    path: Path
    scope: Scope
    impact: Impact


@dataclass
class RenameOperation:
    """Операция переименования для rename_map.json."""

    old_path: str
    new_path: str
    scope: Scope
    rationale_rule_id: str


def load_rules() -> Dict[str, Rule]:
    """Загружает правила нейминга из JSON."""
    with RULES_PATH.open("r", encoding="utf-8") as fp:
        payload = json.load(fp)
    rules: Dict[str, Rule] = {}
    for raw in payload.get("rules", []):
        rule = Rule(
            rule_id=raw["id"],
            scope=raw["scope"],
            kind=raw["kind"],
            pattern=raw["pattern"],
        )
        rules[rule.rule_id] = rule
    return rules


def load_inventory(rules: Dict[str, Rule]) -> List[InventoryRow]:
    """Загружает inventory.csv и сопоставляет строки с правилами."""
    rows: List[InventoryRow] = []
    with INVENTORY_PATH.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for raw in reader:
            path = Path(raw["path"])
            kind = raw["kind"]
            name = raw["name"]
            status = raw["status"]
            rule_ref = raw["pattern_expected"].split(":")[0].strip()
            rule = rules[rule_ref]
            rows.append(
                InventoryRow(
                    path=path,
                    kind=kind,
                    name=name,
                    rule=rule,
                    status=status,
                )
            )
    return rows


def determine_scope(kind: str) -> Scope:
    """Возвращает область применения для записи."""
    return Scope.FILE if kind in {"module", "pkg"} else Scope.SYMBOL


def determine_impact(path: Path) -> Impact:
    """Определяет тип влияния переименования."""
    text = str(path).replace("\\", "/")
    if text.startswith("src/bioetl/cli/"):
        return Impact.CLI
    if text.startswith("src/bioetl/config/"):
        return Impact.CONFIGS
    if text.startswith("tests"):
        return Impact.TESTS
    if text.startswith("docs"):
        return Impact.DOCS
    return Impact.IMPORTS


def sanitize_snake_case(name: str) -> str:
    """Удаляет лидирующие подчёркивания и нормализует snake_case."""
    stripped = name.lstrip("_")
    if not stripped:
        stripped = "value"
    if not stripped[0].isalpha():
        stripped = f"n_{stripped}"
    return stripped


def suggest_for_cli(path: Path) -> str:
    """Предлагает имя файла CLI согласно NR-004."""
    parent = path.parent.name
    stem = path.stem
    if stem == "__init__":
        stem = parent or "cli"
    return f"cli_{stem}{path.suffix}"


def suggest_for_client(path: Path) -> str:
    """Предлагает имя файла клиента согласно NR-005."""
    stem = path.stem
    if stem == "__init__":
        stem = path.parent.name or "client"
    return f"client_{stem}{path.suffix}"


def suggest_for_schema_file(path: Path) -> str:
    """Предлагает имя файла схемы согласно NR-007."""
    stem = path.stem
    if stem == "__init__":
        stem = path.parent.name or "schema"
    if not stem.endswith("_schema"):
        stem = f"{stem}_schema"
    return f"{stem}{path.suffix}"


def suggest_for_config_file(path: Path) -> str:
    """Предлагает имя конфигурационного файла согласно NR-008."""
    stem = path.stem
    if stem == "__init__":
        stem = path.parent.name or "config"
    return f"{stem}.yaml"


def suggest_for_rule_nr001(path: Path) -> str:
    """Формирует имя для правила NR-001."""
    layer = "schema"
    domain = path.parent.name or "module"
    base = path.stem
    actions_map = {
        "__init__": "run",
        "metrics": "map",
        "report": "run",
    }
    action = actions_map.get(base, "validate")
    provider_lookup = {
        "__init__": "pkg",
        "metrics": "metrics",
        "report": "report",
    }
    provider = (
        base
        if base not in provider_lookup
        else provider_lookup[base]
    )
    return f"{layer}_{domain}_{action}_{provider}{path.suffix}"


def suggest_name(row: InventoryRow) -> str:
    """Возвращает предложенное имя."""
    if row.rule.rule_id == "NR-004":
        return suggest_for_cli(row.path)
    if row.rule.rule_id == "NR-005":
        return suggest_for_client(row.path)
    if row.rule.rule_id == "NR-007":
        return suggest_for_schema_file(row.path)
    if row.rule.rule_id == "NR-008":
        return suggest_for_config_file(row.path)
    if row.rule.rule_id == "NR-001":
        return suggest_for_rule_nr001(row.path)
    if row.rule.rule_id == "NR-006" and row.kind == "class":
        return row.name if row.name.endswith("Client") else f"{row.name}Client"
    if row.rule.rule_id == "NR-011" and row.kind == "func":
        return sanitize_snake_case(row.name)
    return row.name


def build_suggestions(rows: Iterable[InventoryRow]) -> List[Suggestion]:
    """Генерирует предложения переименований."""
    suggestions: List[Suggestion] = []
    for row in rows:
        if row.status != "VIOL":
            continue
        suggested_name = suggest_name(row)
        scope = determine_scope(row.kind)
        impact = determine_impact(row.path)
        suggestions.append(
            Suggestion(
                rule_id=row.rule.rule_id,
                found_name=row.name,
                suggested_name=suggested_name,
                path=row.path,
                scope=scope,
                impact=impact,
            )
        )
    suggestions.sort(key=lambda item: (str(item.path), item.found_name))
    return suggestions


def detect_shemas_paths(rows: Sequence[InventoryRow]) -> List[Tuple[Path, Path]]:
    """Находит пути с опечаткой shemas и возвращает пары (старый, новый)."""
    pairs: List[Tuple[Path, Path]] = []
    for row in rows:
        if "shemas" in str(row.path):
            corrected = Path(str(row.path).replace("shemas", "schemas"))
            pairs.append((row.path, corrected))
    return pairs


def write_violations_csv(suggestions: Sequence[Suggestion]) -> None:
    """Сохраняет таблицу нарушений."""
    fieldnames = ["rule_id", "found_name", "suggested_name", "path", "scope", "impact"]
    with VIOLATIONS_OUTPUT.with_suffix(".tmp").open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for item in suggestions:
            writer.writerow(
                {
                    "rule_id": item.rule_id,
                    "found_name": item.found_name,
                    "suggested_name": item.suggested_name,
                    "path": str(item.path).replace("\\", "/"),
                    "scope": item.scope.value,
                    "impact": item.impact.value,
                }
            )
    VIOLATIONS_OUTPUT.with_suffix(".tmp").replace(VIOLATIONS_OUTPUT)


def build_rename_map(
    suggestions: Sequence[Suggestion],
    shemas_pairs: Sequence[Tuple[Path, Path]],
) -> List[RenameOperation]:
    """Формирует список операций переименования."""
    operations: List[RenameOperation] = []
    for item in suggestions:
        if item.scope == Scope.FILE:
            old_path = str(item.path).replace("\\", "/")
            new_file = item.path.with_name(item.suggested_name)
            new_path = str(new_file).replace("\\", "/")
        else:
            old_path = f"{item.path}::{item.found_name}".replace("\\", "/")
            new_path = f"{item.path}::{item.suggested_name}".replace("\\", "/")
        operations.append(
            RenameOperation(
                old_path=old_path,
                new_path=new_path,
                scope=item.scope,
                rationale_rule_id=item.rule_id,
            )
        )
    for src, dst in shemas_pairs:
        operations.append(
            RenameOperation(
                old_path=str(src).replace("\\", "/"),
                new_path=str(dst).replace("\\", "/"),
                scope=Scope.FILE,
                rationale_rule_id="NR-007",
            )
        )
    deduplicated: Dict[str, RenameOperation] = {}
    targets = set()
    for op in sorted(operations, key=lambda x: (x.old_path, x.new_path)):
        if op.new_path in targets:
            continue
        deduplicated[op.old_path] = op
        targets.add(op.new_path)
    return list(deduplicated.values())


def write_rename_map(operations: Sequence[RenameOperation]) -> None:
    """Сохраняет JSON с операциями переименования."""
    payload = [
        {
            "old_path": op.old_path,
            "new_path": op.new_path,
            "scope": op.scope.value,
            "rationale_rule_id": op.rationale_rule_id,
        }
        for op in sorted(operations, key=lambda x: (x.old_path, x.new_path))
    ]
    tmp_path = RENAME_MAP_OUTPUT.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    tmp_path.replace(RENAME_MAP_OUTPUT)


def main() -> None:
    """Точка входа."""
    rules = load_rules()
    rows = load_inventory(rules)
    suggestions = build_suggestions(rows)
    write_violations_csv(suggestions)
    shemas_pairs = detect_shemas_paths(rows)
    operations = build_rename_map(suggestions, shemas_pairs)
    write_rename_map(operations)


if __name__ == "__main__":
    main()


