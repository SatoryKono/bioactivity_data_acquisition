"""Utility script for static analysis of pipeline modules.

Collects function metadata (signatures, annotations, side effects, raises)
and detects duplicate or near-duplicate implementations across pipelines.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import io
import json
import os
import tokenize
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Sequence, cast

ROOT = Path(__file__).resolve().parents[1]
PIPELINES_ROOT = ROOT / "src" / "bioetl" / "pipelines"
REPORT_DIR = ROOT / "REPORT"
LINES_DIFF_PATH = REPORT_DIR / "LINES_DIFF.md"
DUPLICATES_CSV_PATH = REPORT_DIR / "DUPLICATES.csv"

LOG_METHODS = {"debug", "info", "warning", "error", "critical", "exception", "log"}
NETWORK_METHODS = {"get", "post", "put", "delete", "request", "fetch", "head", "options"}
EXPECTED_MODULES = ("run.py", "normalize.py", "transform.py")
JACCARD_THRESHOLD = 0.85


@dataclass(slots=True)
class FunctionRecord:
    pipeline: str
    module: str
    qualname: str
    signature: str
    annotations: list[str]
    side_effects: dict[str, list[str]]
    raises: list[str]
    relpath: str
    start_line: int
    end_line: int
    ast_hash: str
    tokens: set[str] = field(default_factory=set)

    @property
    def location(self) -> str:
        return f"{self.relpath}:{self.start_line}-{self.end_line}"

    def as_markdown_row(self) -> str:
        annotation_text = ", ".join(self.annotations) if self.annotations else "—"
        effects_parts = []
        for key in ("logging", "files", "network"):
            items = self.side_effects.get(key)
            if items:
                effects_parts.append(f"{key}: {', '.join(sorted(set(items)))}")
        effects_text = "; ".join(effects_parts) if effects_parts else "—"
        raises_text = ", ".join(self.raises) if self.raises else "—"
        return (
            f"| `{self.qualname}` | `{self.signature}` | {annotation_text} | "
            f"{effects_text} | {raises_text} | `{self.location}` | `{self.ast_hash[:10]}` |"
        )


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def iter_pipeline_files() -> Iterator[Path]:
    for python_file in PIPELINES_ROOT.rglob("*.py"):
        if python_file.name == "__init__.py":
            continue
        yield python_file


def pipeline_key(path: Path) -> tuple[str, str]:
    relative = path.relative_to(PIPELINES_ROOT)
    if len(relative.parts) == 1:
        pipeline = relative.stem
        module = relative.name
    else:
        pipeline = "/".join(relative.parts[:-1])
        module = relative.name
    return pipeline, module


class FunctionCollector(ast.NodeVisitor):
    def __init__(self, source: str, relpath: str, pipeline: str, module: str) -> None:
        self.source = source
        self.relpath = relpath
        self.pipeline = pipeline
        self.module = module
        self.records: list[FunctionRecord] = []
        self._class_stack: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._class_stack.append(node.name)
        self.generic_visit(node)
        self._class_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._register_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._register_function(node)

    def _register_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        qualname = ".".join([*self._class_stack, node.name]) if self._class_stack else node.name
        signature = format_signature(node)
        annotations = collect_annotations(node)
        side_effects = detect_side_effects(node)
        raises = collect_raises(node)
        ast_hash, tokens = fingerprint_function(node, self.source)
        record = FunctionRecord(
            pipeline=self.pipeline,
            module=self.module,
            qualname=qualname,
            signature=signature,
            annotations=annotations,
            side_effects=side_effects,
            raises=raises,
            relpath=self.relpath,
            start_line=node.lineno,
            end_line=getattr(node, "end_lineno", node.lineno),
            ast_hash=ast_hash,
            tokens=tokens,
        )
        self.records.append(record)


def format_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args = node.args
    parts: list[str] = []

    def render_arg(arg: ast.arg, default: ast.AST | None = None) -> str:
        annotation = ast.unparse(arg.annotation) if arg.annotation else None
        arg_text = arg.arg
        if annotation:
            arg_text += f": {annotation}"
        if default is not None:
            arg_text += f" = {ast.unparse(default)}"
        return arg_text

    positional = list(args.args)
    defaults = list(args.defaults)
    default_offset = len(positional) - len(defaults)
    for index, arg in enumerate(positional):
        default = defaults[index - default_offset] if index >= default_offset else None
        parts.append(render_arg(arg, default))

    if args.vararg:
        suffix = ast.unparse(args.vararg.annotation) if args.vararg.annotation else None
        text = f"*{args.vararg.arg}"
        if suffix:
            text += f": {suffix}"
        parts.append(text)

    if args.kwonlyargs:
        if not args.vararg:
            parts.append("*")
        for kw_arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=False):
            parts.append(render_arg(kw_arg, default))

    if args.kwarg:
        suffix = ast.unparse(args.kwarg.annotation) if args.kwarg.annotation else None
        text = f"**{args.kwarg.arg}"
        if suffix:
            text += f": {suffix}"
        parts.append(text)

    return_annotation = ast.unparse(node.returns) if node.returns else None
    signature = f"{node.name}({', '.join(parts)})"
    if return_annotation:
        signature += f" -> {return_annotation}"
    return signature


def collect_annotations(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    annotations: list[str] = []
    for arg in (*node.args.posonlyargs, *node.args.args):
        if arg.annotation:
            annotations.append(f"{arg.arg}: {ast.unparse(arg.annotation)}")
    if node.args.vararg and node.args.vararg.annotation:
        annotations.append(f"*{node.args.vararg.arg}: {ast.unparse(node.args.vararg.annotation)}")
    for kw_arg in node.args.kwonlyargs:
        if kw_arg.annotation:
            annotations.append(f"{kw_arg.arg}: {ast.unparse(kw_arg.annotation)}")
    if node.args.kwarg and node.args.kwarg.annotation:
        annotations.append(f"**{node.args.kwarg.arg}: {ast.unparse(node.args.kwarg.annotation)}")
    if node.returns:
        annotations.append(f"return: {ast.unparse(node.returns)}")
    return annotations


def iter_relevant_nodes(node: ast.AST) -> Iterator[ast.AST]:
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        yield child
        yield from iter_relevant_nodes(child)


def detect_side_effects(node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, list[str]]:
    logging_calls: set[str] = set()
    file_calls: set[str] = set()
    network_calls: set[str] = set()

    for child in iter_relevant_nodes(node):
        if isinstance(child, ast.Call):
            func = child.func
            if isinstance(func, ast.Attribute):
                attr = func.attr
                base_name = getattr(func.value, "id", None)
                if attr in LOG_METHODS or (base_name == "logging" and attr):
                    logging_calls.add(f"{ast.unparse(func)}")
                if attr == "open":
                    file_calls.add(f"{ast.unparse(func)}")
                if attr in NETWORK_METHODS or (base_name and "client" in base_name.lower()):
                    network_calls.add(f"{ast.unparse(func)}")
            elif isinstance(func, ast.Name):
                if func.id == "open":
                    file_calls.add("open")
                if func.id.startswith("logging."):
                    logging_calls.add(func.id)
    side_effects: dict[str, list[str]] = {}
    if logging_calls:
        side_effects["logging"] = sorted(logging_calls)
    if file_calls:
        side_effects["files"] = sorted(file_calls)
    if network_calls:
        side_effects["network"] = sorted(network_calls)
    return side_effects


def collect_raises(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    raises: set[str] = set()
    for child in iter_relevant_nodes(node):
        if isinstance(child, ast.Raise):
            if child.exc is None:
                raises.add("raise")
            else:
                raises.add(ast.unparse(child.exc))
    return sorted(raises)


class NormaliseNames(ast.NodeTransformer):
    def visit_Name(self, node: ast.Name) -> ast.AST:
        return ast.copy_location(ast.Name(id="__VAR__", ctx=node.ctx), node)

    def visit_Attribute(self, node: ast.Attribute) -> ast.AST:
        self.generic_visit(node)
        node.attr = "__ATTR__"
        return node

    def visit_arg(self, node: ast.arg) -> ast.AST:
        annotation = node.annotation
        new_node = ast.arg(arg="__ARG__", annotation=annotation, type_comment=None)
        return ast.copy_location(new_node, node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:
        new_node = cast(ast.FunctionDef, self.generic_visit(node))
        new_node.name = "__FUNC__"
        return new_node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        new_node = cast(ast.AsyncFunctionDef, self.generic_visit(node))
        new_node.name = "__FUNC__"
        return new_node

    def visit_Constant(self, node: ast.Constant) -> ast.AST:
        if isinstance(node.value, (int, float, complex, str, bytes)):
            value: object = type(node.value)()
            return ast.copy_location(ast.Constant(value=value), node)
        return node


def fingerprint_function(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    source: str,
) -> tuple[str, set[str]]:
    tree = ast.fix_missing_locations(ast.parse(ast.get_source_segment(source, node) or ""))
    normalised = NormaliseNames().visit(tree)
    ast_hash = hashlib.sha256(
        ast.dump(normalised, include_attributes=False).encode("utf-8")
    ).hexdigest()

    segment = ast.get_source_segment(source, node) or ""
    tokens: set[str] = set()
    if segment:
        reader = io.StringIO(segment).readline
        for token in tokenize.generate_tokens(reader):
            if token.type in {
                tokenize.ENCODING,
                tokenize.NL,
                tokenize.NEWLINE,
                tokenize.INDENT,
                tokenize.DEDENT,
                tokenize.ENDMARKER,
                tokenize.COMMENT,
            }:
                continue
            lexeme = token.string.strip()
            if lexeme:
                tokens.add(lexeme)
    return ast_hash, tokens


def build_inventory(records: Sequence[FunctionRecord]) -> dict[str, dict[str, str]]:
    inventory: dict[str, dict[str, str]] = {}
    for record in records:
        modules = inventory.setdefault(record.pipeline, {})
        modules.setdefault(record.module, "есть в ветке")
    for pipeline, modules in inventory.items():
        for module in EXPECTED_MODULES:
            modules.setdefault(module, "нет в ветке")
    return inventory


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    intersection = len(left & right)
    union = len(left | right)
    if union == 0:
        return 0.0
    return intersection / union


def build_clusters(records: Sequence[FunctionRecord]) -> list[set[int]]:
    components: list[set[int]] = []
    ast_buckets: dict[str, list[int]] = {}
    for index, record in enumerate(records):
        ast_buckets.setdefault(record.ast_hash, []).append(index)
    seen = set()
    for bucket in ast_buckets.values():
        if len(bucket) > 1:
            components.append(set(bucket))
            seen.update(bucket)

    for i, left in enumerate(records):
        for j in range(i + 1, len(records)):
            right = records[j]
            if left.ast_hash == right.ast_hash:
                continue
            score = jaccard_similarity(left.tokens, right.tokens)
            if score >= JACCARD_THRESHOLD:
                merged = None
                for component in components:
                    if i in component or j in component:
                        component.update({i, j})
                        merged = component
                        break
                if merged is None:
                    components.append({i, j})
    normalised_components: list[set[int]] = []
    for component in components:
        if len(component) > 1:
            normalised_components.append(set(sorted(component)))
    return normalised_components


def format_markdown(records: Sequence[FunctionRecord]) -> str:
    inventory = build_inventory(records)
    lines: list[str] = [
        "# Анализ пайплайнов",
        "",
        "## Инвентаризация модулей",
        "",
        "| Пайплайн | run.py | normalize.py | transform.py | Прочие файлы |",
        "| --- | --- | --- | --- | --- |",
    ]
    for pipeline in sorted(inventory):
        modules = inventory[pipeline]
        extra = {module for module in modules if module not in EXPECTED_MODULES}
        lines.append(
            f"| `{pipeline}` | "
            f"{modules.get('run.py', 'нет в ветке')} | "
            f"{modules.get('normalize.py', 'нет в ветке')} | "
            f"{modules.get('transform.py', 'нет в ветке')} | "
            f"{', '.join(sorted(extra)) if extra else '—'} |"
        )
    lines.append("")

    grouped: dict[tuple[str, str], list[FunctionRecord]] = {}
    for record in records:
        key = (record.pipeline, record.module)
        grouped.setdefault(key, []).append(record)

    for (pipeline, module) in sorted(grouped):
        lines.append(f"## `{pipeline}` :: `{module}`")
        lines.append("")
        lines.append(
            "| Имя | Сигнатура | Аннотации | Побочные эффекты | Исключения | Локация | AST |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for record in sorted(grouped[(pipeline, module)], key=lambda item: (item.relpath, item.start_line)):
            lines.append(record.as_markdown_row())
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def write_atomic(path: Path, content: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, path)


def write_csv(path: Path, cluster_rows: list[tuple[str, str, int, str]]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(["path", "hash", "cluster_size", "recommendation"])
        for row in cluster_rows:
            writer.writerow(row)
    os.replace(tmp_path, path)


def main() -> None:
    records: list[FunctionRecord] = []
    for file_path in iter_pipeline_files():
        source = read_text(file_path)
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            raise RuntimeError(f"Не удалось разобрать {file_path}") from exc
        pipeline, module = pipeline_key(file_path)
        collector = FunctionCollector(
            source=source,
            relpath=str(file_path.relative_to(ROOT)),
            pipeline=pipeline,
            module=module,
        )
        collector.visit(tree)
        records.extend(collector.records)

    markdown = format_markdown(records)
    write_atomic(LINES_DIFF_PATH, markdown)

    clusters = build_clusters(records)
    cluster_rows: list[tuple[str, str, int, str]] = []
    for component in clusters:
        members = [records[index] for index in sorted(component)]
        fingerprint = hashlib.sha256(
            "|".join(member.location for member in members).encode("utf-8")
        ).hexdigest()[:16]
        recommendation = "Рассмотреть вынос общей логики в утилиту"
        for member in members:
            cluster_rows.append(
                (
                    member.location,
                    fingerprint,
                    len(component),
                    recommendation,
                )
            )
    cluster_rows.sort(key=lambda row: (row[1], row[0]))
    write_csv(DUPLICATES_CSV_PATH, cluster_rows)

    metadata = {
        "functions_total": len(records),
        "clusters": len({row[1] for row in cluster_rows}),
        "threshold": JACCARD_THRESHOLD,
    }
    pretty = json.dumps(metadata, ensure_ascii=False, indent=2)
    write_atomic(REPORT_DIR / "metadata.json", pretty + "\n")


if __name__ == "__main__":
    main()


