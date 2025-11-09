"""Инструмент для детектирования нарушений правил наименования."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import typer

from bioetl.tools import get_project_root, load_typer_app

__all__ = [
    "NamingViolation",
    "NamingViolationCategory",
    "collect_naming_violations",
    "format_markdown_table",
    "app",
    "run",
]


SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
PRIVATE_SNAKE_CASE = re.compile(r"^_[a-z][a-z0-9_]*$")
PASCAL_CASE = re.compile(r"^_?[A-Z][A-Za-z0-9]*$")
CONSTANT_CASE = re.compile(r"^_?[A-Z][A-Z0-9_]*$")


class NamingViolationCategory(Enum):
    """Категория нарушения правила именования."""

    MODULE = auto()
    CLASS = auto()
    FUNCTION = auto()
    METHOD = auto()
    CONSTANT = auto()


@dataclass(frozen=True)
class NamingViolation:
    """Структура, описывающая единичное нарушение правила именования."""

    path: Path
    category: NamingViolationCategory
    identifier: str
    rule_id: str
    rationale: str


class _Scope(Enum):
    MODULE = auto()
    CLASS = auto()
    FUNCTION = auto()


class _ViolationCollector(ast.NodeVisitor):
    """Обходит AST-модуль и собирает нарушения правил именования."""

    def __init__(self, module_path: Path) -> None:
        self._module_path = module_path
        self._scope_stack: list[_Scope] = [_Scope.MODULE]
        self._violations: list[NamingViolation] = []
        self._import_aliases: set[str] = set()

    def collect_import_aliases(self, module: ast.AST) -> None:
        for node in ast.walk(module):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.asname:
                        self._import_aliases.add(alias.asname)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.asname:
                        self._import_aliases.add(alias.asname)

    def iter_violations(self) -> Iterator[NamingViolation]:
        """Возвращает обнаруженные нарушения."""

        yield from self._violations

    # --------------------------------------------------------------------- #
    # AST visitors
    # --------------------------------------------------------------------- #

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: D401 - стандартный визитор
        """Обрабатывает объявление класса."""

        self._maybe_register_class_violation(node.name)

        self._scope_stack.append(_Scope.CLASS)
        self.generic_visit(node)
        self._scope_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._maybe_register_function_violation(node.name)

        self._scope_stack.append(_Scope.FUNCTION)
        self.generic_visit(node)
        self._scope_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._maybe_register_function_violation(node.name)

        self._scope_stack.append(_Scope.FUNCTION)
        self.generic_visit(node)
        self._scope_stack.pop()

    def visit_Assign(self, node: ast.Assign) -> None:
        if self._in_module_scope():
            for target in node.targets:
                self._maybe_register_constant_violation(target, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if self._in_module_scope():
            self._maybe_register_constant_violation(node.target, node.value)
        self.generic_visit(node)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _in_module_scope(self) -> bool:
        return len(self._scope_stack) == 1 and self._scope_stack[-1] == _Scope.MODULE

    def _maybe_register_class_violation(self, name: str) -> None:
        if name.startswith("__") and name.endswith("__"):
            return
        if PASCAL_CASE.match(name):
            return
        self._violations.append(
            NamingViolation(
                path=self._module_path,
                category=NamingViolationCategory.CLASS,
                identifier=name,
                rule_id="C010",
                rationale="Классы должны использовать PascalCase, допускается только ведущий '_' для защищённых типов.",
            )
        )

    def _maybe_register_function_violation(self, name: str) -> None:
        if name.startswith("__") and name.endswith("__"):
            return
        if name.startswith("visit_") and name[6:] and name[6:].lower() != name[6:]:
            # Специальная сигнатура методов ast.NodeVisitor, допускаем CamelCase после префикса.
            return
        if name.startswith("_"):
            if PRIVATE_SNAKE_CASE.match(name):
                return
            self._violations.append(
                NamingViolation(
                    path=self._module_path,
                    category=self._current_function_category(),
                    identifier=name,
                    rule_id="FN021",
                    rationale="Приватные функции и методы должны использовать snake_case.",
                )
            )
            return
        if SNAKE_CASE.match(name):
            return
        self._violations.append(
            NamingViolation(
                path=self._module_path,
                category=self._current_function_category(),
                identifier=name,
                rule_id="FN020",
                rationale="Публичные функции и методы должны использовать snake_case.",
            )
        )

    def _current_function_category(self) -> NamingViolationCategory:
        return (
            NamingViolationCategory.METHOD
            if self._scope_stack and self._scope_stack[-1] == _Scope.CLASS
            else NamingViolationCategory.FUNCTION
        )

    def _maybe_register_constant_violation(self, target: ast.expr, value: ast.AST | None) -> None:
        if not isinstance(target, ast.Name):
            return
        if value is None or not _looks_like_literal(value):
            return
        name = target.id
        if name in self._import_aliases:
            return
        if name.startswith("__") and name.endswith("__"):
            return
        if CONSTANT_CASE.match(name):
            return
        self._violations.append(
            NamingViolation(
                path=self._module_path,
                category=NamingViolationCategory.CONSTANT,
                identifier=name,
                rule_id="K001",
                rationale="Глобальные константы должны быть в UPPER_SNAKE_CASE (разрешён лидирующий '_').",
            )
        )


def _looks_like_literal(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.UnaryOp):
        return _looks_like_literal(node.operand)
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return all(_looks_like_literal(element) for element in node.elts)
    if isinstance(node, ast.Dict):
        return all(
            _looks_like_literal(key) and _looks_like_literal(value)
            for key, value in zip(node.keys, node.values, strict=False)
        )
    return False


def _iter_python_files(root: Path) -> Iterator[Path]:
    for path in sorted(root.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        yield path


def collect_naming_violations(root: Path) -> list[NamingViolation]:
    """Сканирует исходники и возвращает упорядоченный список нарушений."""

    violations: list[NamingViolation] = []
    for file_path in _iter_python_files(root):
        source = file_path.read_text(encoding="utf-8")
        try:
            module = ast.parse(source, filename=str(file_path))
        except SyntaxError as exc:  # pragma: no cover - синтаксическая ошибка модуля
            violations.append(
                NamingViolation(
                    path=file_path,
                    category=NamingViolationCategory.MODULE,
                    identifier=file_path.stem,
                    rule_id="F000",
                    rationale=f"Не удалось разобрать модуль: {exc}",
                )
            )
            continue
        visitor = _ViolationCollector(file_path)
        visitor.collect_import_aliases(module)
        visitor.visit(module)
        violations.extend(visitor.iter_violations())
    violations.sort(key=lambda item: (str(item.path), item.rule_id, item.identifier))
    return violations


def format_markdown_table(violations: Sequence[NamingViolation]) -> str:
    """Формирует markdown-таблицу для набора нарушений."""

    if not violations:
        return (
            "| path | category | identifier | rule_id | rationale |\n"
            "|---|---|---|---|---|\n"
        )

    rows = [
        "| path | category | identifier | rule_id | rationale |",
        "|---|---|---|---|---|",
    ]
    for violation in violations:
        rows.append(
            "| {path} | {category} | {identifier} | {rule} | {rationale} |".format(
                path=violation.path.as_posix(),
                category=violation.category.name.lower(),
                identifier=violation.identifier,
                rule=violation.rule_id,
                rationale=violation.rationale.replace("|", r"\|"),
            )
        )
    return "\n".join(rows) + "\n"


app = typer.Typer(
    name="bioetl-naming-scan",
    help="Сканирует репозиторий и выводит нарушения правил именования.",
)


@app.command("scan")
def scan(  # noqa: D401 - CLI-команда описана в docstring Typer
    sources: Path = typer.Option(
        None,
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="Каталог с исходниками для проверки. По умолчанию используется src/bioetl.",
    ),
    output: Path | None = typer.Option(
        None,
        help="Путь для записи markdown-таблицы нарушений. Будет создан при необходимости.",
    ),
) -> None:
    """Проводит аудит имен и опционально записывает результат в файл."""

    root = sources if sources is not None else get_project_root() / "src" / "bioetl"
    violations = collect_naming_violations(root)
    table = format_markdown_table(violations)
    typer.echo(table)

    if output is not None:
        target = output.resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target.with_suffix(target.suffix + ".tmp")
        tmp_path.write_text(table, encoding="utf-8")
        tmp_path.replace(target)

    if violations:
        raise typer.Exit(code=1)


def run() -> None:
    """Точка входа для совместимости с runner_factory."""

    load_typer_app("bioetl.tools.naming_violation_scan", "app")



