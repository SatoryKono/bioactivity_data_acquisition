#!/usr/bin/env python3
"""Автоматизация консервативной чистки мёртвого кода на базе отчёта."""
from __future__ import annotations

import argparse
import ast
import csv
import difflib
import json
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence


class Action:
    REMOVE = "remove"
    DEPRECATE = "deprecate"
    COMMENT = "comment"


@dataclass
class Candidate:
    object_name: str
    file: Path
    reason: str
    confidence: str

    @property
    def action(self) -> str:
        confidence_normalized = self.confidence.lower().strip()
        if confidence_normalized == "high":
            return Action.REMOVE
        if confidence_normalized == "medium":
            return Action.DEPRECATE
        return Action.COMMENT

    @property
    def module_import_path(self) -> str:
        relative = self.file
        if relative.as_posix().startswith("src/"):
            relative = relative.relative_to("src")
        module_path = relative.with_suffix("").as_posix().replace("/", ".")
        return module_path


# ---------- чтение отчёта ----------

def load_candidates(path: Path) -> List[Candidate]:
    if not path.exists():
        raise FileNotFoundError(f"Report not found: {path}")
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data = list(reader)
    candidates: List[Candidate] = []
    for row in data:
        candidates.append(
            Candidate(
                object_name=row["object_name"],
                file=Path(row["file"]),
                reason=row.get("reason", ""),
                confidence=row.get("confidence", "low"),
            )
        )
    return candidates


# ---------- генерация плана ----------

def format_table(rows: Sequence[Sequence[str]]) -> str:
    if not rows:
        return "_Нет кандидатов_\n"
    header = rows[0]
    divider = "| " + " | ".join(["---"] * len(header)) + " |"
    lines = ["| " + " | ".join(header) + " |", divider]
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines) + "\n"


def verification_commands(candidate: Candidate) -> str:
    base_cmd = f"rg \"{candidate.object_name}\" src tests"
    yaml_cmd = f"rg -g '*.{{yaml,json}}' \"{candidate.object_name}\" configs"
    if candidate.action == Action.DEPRECATE:
        return "<br>".join([
            base_cmd,
            f"rg \"{candidate.object_name}\" docs",
            yaml_cmd,
        ])
    if candidate.action == Action.REMOVE:
        return "<br>".join([base_cmd, yaml_cmd, "pytest"])
    return base_cmd


def build_plan(candidates: Sequence[Candidate]) -> str:
    sections = []
    for action, title in [
        (Action.REMOVE, "A. Удаление (confidence=high)"),
        (Action.DEPRECATE, "B. Депрекация (confidence=medium)"),
        (Action.COMMENT, "C. Докстринг-комментарий (low/other)"),
    ]:
        scoped = [c for c in candidates if c.action == action]
        rows = [["Object", "File", "Reason", "Verification"]]
        for c in scoped:
            rows.append(
                [c.object_name, c.file.as_posix(), c.reason, verification_commands(c)]
            )
        sections.append(f"## {title}\n\n" + format_table(rows))
    return "\n".join(sections)


# ---------- генерация описания PR ----------

def render_pr_message(candidates: Sequence[Candidate]) -> str:
    counts = {
        Action.REMOVE: len([c for c in candidates if c.action == Action.REMOVE]),
        Action.DEPRECATE: len([c for c in candidates if c.action == Action.DEPRECATE]),
        Action.COMMENT: len([c for c in candidates if c.action == Action.COMMENT]),
    }
    summary_lines = [
        "## Summary",
        f"- remove {counts[Action.REMOVE]} dead-code objects/files (class A)",
        f"- mark {counts[Action.DEPRECATE]} objects as deprecated (class B)",
        f"- annotate {counts[Action.COMMENT]} objects with review docstrings (class C)",
        "",
        "## Testing",
        "- pytest",
        "- smoke run of the primary CLI command",
        "- bioetl CLI help checks (python -m bioetl --help)",
    ]
    return "\n".join(summary_lines)


# ---------- патчи ----------

def write_patch(
    original: str,
    updated: str,
    file_path: Path,
    patch_dir: Path,
    label: str | None = None,
) -> None:
    diff = difflib.unified_diff(
        original.splitlines(),
        updated.splitlines(),
        fromfile=file_path.as_posix(),
        tofile=file_path.as_posix(),
        lineterm="",
    )
    safe_name = file_path.as_posix().replace("/", "__")
    if label:
        safe_label = label.replace("/", "__").replace(" ", "_")
        safe_name = f"{safe_name}__{safe_label}"
    patch_path = patch_dir / f"{safe_name}.patch"
    patch_dir.mkdir(parents=True, exist_ok=True)
    patch_path.write_text("\n".join(diff) + "\n", encoding="utf-8")


def rewrite_file(
    file_path: Path,
    transformer,
    patch_dir: Path,
    apply_changes: bool,
) -> bool:
    if not file_path.exists():
        return False
    original_text = file_path.read_text(encoding="utf-8")
    updated_text = transformer(original_text)
    if original_text == updated_text:
        return False
    write_patch(original_text, updated_text, file_path, patch_dir)
    if apply_changes:
        file_path.write_text(updated_text, encoding="utf-8")
    return True


def find_target_node(tree: ast.Module, candidate: Candidate):
    parts = candidate.object_name.split(".")
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.name == parts[0] and len(parts) == 1:
                return node
            if isinstance(node, ast.ClassDef) and node.name == parts[0] and len(parts) == 2:
                for child in node.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and child.name == parts[1]:
                        return child
    return None


def remove_object(candidate: Candidate, patch_dir: Path, apply_changes: bool) -> bool:
    path = candidate.file
    if not path.exists():
        return False
    source = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False
    node = find_target_node(tree, candidate)
    if node is None:
        return False
    start = node.lineno - 1
    end = (node.end_lineno or node.lineno) - 1
    lines = source.splitlines()
    del lines[start : end + 1]
    updated = "\n".join(lines)
    write_patch(source, updated, path, patch_dir, label=candidate.object_name)
    if apply_changes:
        path.write_text(updated, encoding="utf-8")
    test_dir = patch_dir / "tests"
    test_dir.mkdir(parents=True, exist_ok=True)
    module_path = candidate.module_import_path
    slug = candidate.object_name.replace(".", "_")
    test_path = test_dir / f"test_{slug}_removal.py"
    test_body = textwrap.dedent(
        f"""
        import importlib
        import pytest


        def test_{slug}_removed():
            with pytest.raises((ImportError, AttributeError)):
                module = importlib.import_module("{module_path}")
                getattr(module, "{candidate.object_name.split('.')[-1]}")
        """
    ).strip() + "\n"
    test_path.write_text(test_body, encoding="utf-8")
    return True


def ensure_deprecation_import(lines: List[str]) -> tuple[List[str], int | None]:
    import_stmt = "from bioetl.utils.deprecation import deprecated"
    if any(import_stmt in line for line in lines):
        return lines, None
    insert_at = 0
    if lines and lines[0].startswith("#!/"):
        insert_at = 1
    if insert_at < len(lines) and lines[insert_at].lstrip().startswith("\"\"\""):
        doc_end = insert_at
        while doc_end < len(lines):
            stripped = lines[doc_end].rstrip()
            if stripped.endswith("\"\"\""):
                break
            doc_end += 1
        insert_at = min(doc_end + 1, len(lines))
    while insert_at < len(lines) and lines[insert_at].startswith("from __future__"):
        insert_at += 1
    lines.insert(insert_at, import_stmt)
    return lines, insert_at


def add_deprecated_decorator(candidate: Candidate, patch_dir: Path, apply_changes: bool) -> bool:
    path = candidate.file
    if not path.exists():
        return False
    source = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False
    node = find_target_node(tree, candidate)
    if node is None:
        return False
    lines = source.splitlines()

    # update docstring with warning note before we insert imports/decorators (line numbers stay valid)
    if node.body:
        first_stmt = node.body[0]
        if isinstance(first_stmt, ast.Expr) and isinstance(getattr(first_stmt, "value", None), ast.Constant) and isinstance(first_stmt.value.value, str):
            doc_start = first_stmt.lineno - 1
            doc_end = (first_stmt.end_lineno or first_stmt.lineno) - 1
            indent = " " * first_stmt.col_offset
            docstring = ast.get_docstring(node, clean=False) or ""
            updated_doc = docstring.rstrip() + f"\n\nDeprecated: {candidate.reason}."
            doc_lines = textwrap.indent(f'"""{updated_doc}"""', indent).splitlines()
            lines[doc_start : doc_end + 1] = doc_lines
        else:
            indent = " " * (node.col_offset + 4)
            doc_insert_at = node.body[0].lineno - 1 if node.body else node.lineno
            lines.insert(doc_insert_at, f'{indent}"""Deprecated: {candidate.reason}."""')

    def_index = node.lineno - 1
    lines, import_insert_at = ensure_deprecation_import(lines)
    if import_insert_at is not None and import_insert_at <= def_index:
        def_index += 1

    decorator_line = f"@deprecated(reason=\"{candidate.reason}\")"
    insert_at = def_index
    while insert_at > 0 and lines[insert_at - 1].strip().startswith("@"):
        insert_at -= 1
    lines.insert(insert_at, decorator_line)
    updated = "\n".join(lines)
    write_patch(source, updated, path, patch_dir, label=candidate.object_name)
    if apply_changes:
        path.write_text(updated, encoding="utf-8")
    return True


def add_docstring_comment(candidate: Candidate, patch_dir: Path, apply_changes: bool) -> bool:
    path = candidate.file
    if not path.exists():
        return False
    source = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False
    node = find_target_node(tree, candidate)
    if node is None:
        return False
    lines = source.splitlines()
    note = f"Marked by cleanup_dead_code: {candidate.reason}."
    if node.body:
        first_stmt = node.body[0]
        if isinstance(first_stmt, ast.Expr) and isinstance(getattr(first_stmt, "value", None), ast.Constant) and isinstance(first_stmt.value.value, str):
            doc_start = first_stmt.lineno - 1
            doc_end = (first_stmt.end_lineno or first_stmt.lineno) - 1
            indent = " " * first_stmt.col_offset
            docstring = ast.get_docstring(node, clean=False) or ""
            updated_doc = docstring.rstrip() + f"\n\n{note}"
            doc_lines = textwrap.indent(f'"""{updated_doc}"""', indent).splitlines()
            lines[doc_start : doc_end + 1] = doc_lines
        else:
            indent = " " * (node.col_offset + 4)
            insert_at = node.body[0].lineno - 1 if node.body else node.lineno
            lines.insert(insert_at, f'{indent}"""{note}"""')
    updated = "\n".join(lines)
    write_patch(source, updated, path, patch_dir, label=candidate.object_name)
    if apply_changes:
        path.write_text(updated, encoding="utf-8")
    return True


# ---------- CLI ----------

def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("reports/dead_code_candidates.json"),
        help="Путь к JSON/CSV отчёту кандидатов",
    )
    parser.add_argument(
        "--output-plan",
        type=Path,
        default=Path("reports/cleanup_plan.md"),
        help="Куда положить Markdown план",
    )
    parser.add_argument(
        "--patch-dir",
        type=Path,
        default=Path("reports/generated_patches"),
        help="Директория для патчей и тестовых заготовок",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Модифицировать файлы на месте. По умолчанию только генерируются патчи.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    candidates = load_candidates(args.report)
    args.output_plan.parent.mkdir(parents=True, exist_ok=True)
    args.patch_dir.mkdir(parents=True, exist_ok=True)

    plan_body = build_plan(candidates)
    args.output_plan.write_text(plan_body, encoding="utf-8")

    for candidate in candidates:
        if candidate.action == Action.REMOVE:
            remove_object(candidate, args.patch_dir, apply_changes=args.apply)
        elif candidate.action == Action.DEPRECATE:
            add_deprecated_decorator(candidate, args.patch_dir, apply_changes=args.apply)
        else:
            add_docstring_comment(candidate, args.patch_dir, apply_changes=args.apply)

    pr_message = render_pr_message(candidates)
    print(pr_message)


if __name__ == "__main__":
    main()
