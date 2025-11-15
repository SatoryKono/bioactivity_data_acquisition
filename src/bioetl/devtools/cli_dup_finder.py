"""Detect duplicate and similar code fragments within the BioETL `src/` tree."""

from __future__ import annotations

import ast
import copy
import csv
import hashlib
import html
import io
import keyword
import os
import sys
import tokenize
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Literal, Protocol, TextIO, cast

from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.tools import get_project_root

__all__ = ["main", "run_dup_finder"]

DEFAULT_ROOT = get_project_root()
DEFAULT_OUTPUT_DIR = DEFAULT_ROOT / "artifacts" / "dup_finder"
_VALID_FORMATS = ("md", "csv")


Kind = Literal["func", "class", "method"]
Role = Literal["extract", "transform", "validate", "write", "run", "client", "schema", "util", "log", "cli"]


@dataclass(frozen=True, slots=True)
class ParseError:
    """Parsing error for a source file."""

    path: Path
    message: str


@dataclass(frozen=True, slots=True)
class CodeUnit:
    """Normalized code fragment such as a function, method, or class."""

    symbol: str
    kind: Kind
    role: Role
    path: Path
    rel_path: Path
    start_line: int
    end_line: int
    norm_src: str
    norm_loc: int
    ast_hash: str
    tokens: tuple[str, ...]
    token_multiset: Counter[str]
    snippet: str

    @property
    def reference(self) -> str:
        return f"{self.rel_path.as_posix()}#L{self.start_line}-L{self.end_line}"


@dataclass(frozen=True, slots=True)
class DuplicateCluster:
    """Group of identical code fragments."""

    ast_hash: str
    members: tuple[CodeUnit, ...]


@dataclass(frozen=True, slots=True)
class NearDuplicatePair:
    """Pair of structurally similar code fragments."""

    unit_a: CodeUnit
    unit_b: CodeUnit
    jaccard: float
    lcs_ratio: float
    divergences: str

    @property
    def pair_label(self) -> str:
        return f"{self.unit_a.symbol} ↔ {self.unit_b.symbol}"


class _SupportsReadline(Protocol):
    def readline(self) -> str:
        ...


def strip_docstrings(body: list[ast.stmt]) -> list[ast.stmt]:
    """Remove a leading docstring expression from a statement body."""

    if not body:
        return body
    first = body[0]
    if (
        isinstance(first, ast.Expr)
        and isinstance(first.value, ast.Constant)
        and isinstance(first.value.value, str)
    ):
        return body[1:]
    return body


class _ASTNormalizer(ast.NodeTransformer):
    """Canonicalise AST nodes by stripping noise, sorting inputs, and normalising literals."""

    _LOG_ATTRS = {"debug", "info", "warning", "error", "exception", "critical", "trace", "log"}

    def visit_Module(self, node: ast.Module) -> ast.Module:
        node = cast(ast.Module, self.generic_visit(node))
        node.body = strip_docstrings(node.body)
        node.body = [stmt for stmt in node.body if stmt is not None]
        return node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:
        node = cast(ast.FunctionDef, self.generic_visit(node))
        node.body = strip_docstrings(node.body)
        node.body = [stmt for stmt in node.body if stmt is not None]
        return node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        node = cast(ast.AsyncFunctionDef, self.generic_visit(node))
        node.body = strip_docstrings(node.body)
        node.body = [stmt for stmt in node.body if stmt is not None]
        return node

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.AST:
        node = cast(ast.ClassDef, self.generic_visit(node))
        node.body = strip_docstrings(node.body)
        node.body = [stmt for stmt in node.body if stmt is not None]
        return node

    def visit_Expr(self, node: ast.Expr) -> ast.AST | None:
        target = getattr(node, "value", None)
        if isinstance(target, ast.Call) and self._is_logging_call(target):
            return None
        return self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> ast.AST:
        node = cast(ast.Call, self.generic_visit(node))
        with_args = [kw for kw in node.keywords if kw.arg is not None]
        without_args = [kw for kw in node.keywords if kw.arg is None]
        with_args.sort(key=lambda kw: kw.arg or "")
        node.keywords = [*with_args, *without_args]
        return node

    def visit_Dict(self, node: ast.Dict) -> ast.AST:
        node = cast(ast.Dict, self.generic_visit(node))
        sortable: list[tuple[str, ast.expr | None, ast.expr]] = []
        fallback: list[tuple[ast.expr | None, ast.expr]] = []
        for key_expr, value_expr in zip(node.keys, node.values, strict=False):
            if isinstance(key_expr, ast.Constant) and isinstance(key_expr.value, str):
                sortable.append((key_expr.value, key_expr, value_expr))
            else:
                fallback.append((key_expr, value_expr))
        sortable.sort(key=lambda item: item[0])
        new_keys: list[ast.expr | None] = [item[1] for item in sortable] + [item[0] for item in fallback]
        new_values: list[ast.expr] = [item[2] for item in sortable] + [item[1] for item in fallback]
        node.keys = new_keys
        node.values = new_values
        return node

    def visit_Constant(self, node: ast.Constant) -> ast.AST:
        value = node.value
        if isinstance(value, str):
            return ast.copy_location(ast.Constant(value="STR"), node)
        if isinstance(value, (int, float, complex)):
            return ast.copy_location(ast.Constant(value="NUM"), node)
        return node

    def _is_logging_call(self, node: ast.Call) -> bool:
        func = node.func
        if isinstance(func, ast.Name) and func.id == "print":
            return True
        if isinstance(func, ast.Attribute) and func.attr in self._LOG_ATTRS:
            return True
        return False


def _compute_canonical_dump(node: ast.AST) -> str:
    return ast.dump(node, annotate_fields=True, include_attributes=False)


def _lcs_ratio(seq_a: Sequence[str], seq_b: Sequence[str]) -> float:
    if not seq_a or not seq_b:
        return 0.0
    len_a = len(seq_a)
    len_b = len(seq_b)
    dp = [[0] * (len_b + 1) for _ in range(len_a + 1)]
    for i in range(len_a):
        for j in range(len_b):
            if seq_a[i] == seq_b[j]:
                dp[i + 1][j + 1] = dp[i][j] + 1
            else:
                dp[i + 1][j + 1] = dp[i + 1][j] if dp[i + 1][j] >= dp[i][j + 1] else dp[i][j + 1]
    lcs_length = dp[len_a][len_b]
    denominator = max(len_a, len_b)
    return lcs_length / denominator if denominator else 0.0


def _jaccard(tokens_a: Iterable[str], tokens_b: Iterable[str]) -> float:
    set_a = set(tokens_a)
    set_b = set(tokens_b)
    if not set_a and not set_b:
        return 1.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union else 0.0


def _escape_html(text: str) -> str:
    return html.escape(text, quote=False)


def _make_snippet(lines: Sequence[str], start_line: int, end_line: int, max_lines: int = 5) -> str:
    lower = max(0, start_line - 1)
    upper = min(len(lines), start_line - 1 + max_lines, end_line)
    clamped = lines[lower:upper]
    return "".join(clamped).rstrip()


def _tokenize_norm_source(norm_src: str) -> tuple[str, ...]:
    reader: _SupportsReadline = io.StringIO(norm_src)
    tokens: list[str] = []
    for tok in tokenize.generate_tokens(reader.readline):
        tok_type = tok.type
        tok_string = tok.string
        if tok_type in {tokenize.NEWLINE, tokenize.NL, tokenize.INDENT, tokenize.DEDENT}:
            continue
        if tok_type == tokenize.NAME:
            if tok_string in {"NUM", "STR", "NAME"}:
                tokens.append(tok_string)
            elif keyword.iskeyword(tok_string):
                tokens.append(tok_string)
            else:
                tokens.append("NAME")
        elif tok_type == tokenize.NUMBER:
            tokens.append("NUM")
        elif tok_type == tokenize.STRING:
            tokens.append("STR")
        elif tok_type == tokenize.OP:
            tokens.append(tok_string)
    return tuple(tokens)


def _normalise_node(node: ast.AST) -> tuple[str, str, tuple[str, ...], Counter[str]]:
    node_copy = copy.deepcopy(node)
    module = ast.Module(body=[cast(ast.stmt, node_copy)], type_ignores=[])
    normalizer = _ASTNormalizer()
    normalised_module = cast(ast.Module, normalizer.visit(module))
    ast.fix_missing_locations(normalised_module)
    norm_src = ast.unparse(normalised_module).strip()
    canonical = _compute_canonical_dump(normalised_module)
    ast_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    tokens = _tokenize_norm_source(norm_src)
    token_multiset = Counter(tokens)
    return norm_src, ast_hash, tokens, token_multiset


class _CodeUnitVisitor(ast.NodeVisitor):
    """Collect functional units from the AST."""

    def __init__(self, *, file_path: Path, rel_path: Path, source_lines: Sequence[str]) -> None:
        self._file_path = file_path
        self._rel_path = rel_path
        self._source_lines = source_lines
        self.units: list[CodeUnit] = []
        self._class_stack: list[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._register_function(node)
        return None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._register_function(node)
        return None

    def visit_ClassDef(self, node: ast.ClassDef) -> Any:
        symbol = node.name
        unit = self._make_code_unit(symbol=symbol, kind="class", node=node)
        self.units.append(unit)
        self._class_stack.append(symbol)
        self.generic_visit(node)
        self._class_stack.pop()
        return None

    def _register_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        kind: Kind = "method" if self._class_stack else "func"
        symbol = ".".join([*self._class_stack, node.name]) if self._class_stack else node.name
        unit = self._make_code_unit(symbol=symbol, kind=kind, node=node)
        self.units.append(unit)

    def _make_code_unit(self, *, symbol: str, kind: Kind, node: ast.AST) -> CodeUnit:
        norm_src, ast_hash, tokens, token_multiset = _normalise_node(node)
        norm_loc = sum(1 for line in norm_src.splitlines() if line.strip())
        start_line = getattr(node, "lineno", 0)
        end_line = getattr(node, "end_lineno", None) or start_line
        snippet = _make_snippet(self._source_lines, start_line, end_line)
        role = _classify_role(self._rel_path, symbol)
        return CodeUnit(
            symbol=symbol,
            kind=kind,
            role=role,
            path=self._file_path,
            rel_path=self._rel_path,
            start_line=start_line,
            end_line=end_line,
            norm_src=norm_src,
            norm_loc=norm_loc,
            ast_hash=ast_hash,
            tokens=tokens,
            token_multiset=token_multiset,
            snippet=snippet,
        )


def _classify_role(rel_path: Path, symbol: str) -> Role:
    parts = {part for part in rel_path.parts}
    filename = rel_path.name
    lowered_symbol = symbol.lower()
    if "clients" in parts:
        return "client"
    if "config" in parts or "schemas" in parts:
        return "schema"
    if "cli" in parts:
        return "cli"
    if "logger" in parts or lowered_symbol.startswith("log_"):
        return "log"
    if "pipelines" in parts:
        if filename == "run.py" or lowered_symbol.startswith(("run", "execute")):
            return "run"
        if any(key in parts for key in {"extract", "loader"}):
            return "extract"
        if lowered_symbol.startswith("extract"):
            return "extract"
        if any(keyword_piece in parts for keyword_piece in {"transform", "transformers"}):
            return "transform"
        if lowered_symbol.startswith("transform"):
            return "transform"
        if any(keyword_piece in parts for keyword_piece in {"validate", "validator"}):
            return "validate"
        if lowered_symbol.startswith("validate"):
            return "validate"
        if lowered_symbol.startswith("write"):
            return "write"
    return "util"


def _collect_python_files(root: Path) -> tuple[list[Path], list[str]]:
    src_dir = root / "src"
    if not src_dir.exists():
        return [], [f"Directory {src_dir} is not accessible"]
    python_files: list[Path] = []
    warnings: list[str] = []
    for dirpath, dirnames, filenames in os.walk(src_dir):
        current = Path(dirpath)
        try:
            dirnames[:] = sorted(dirnames)
            for filename in sorted(filenames):
                if filename.endswith(".py"):
                    python_files.append(current / filename)
        except PermissionError:
            warnings.append(f"Directory {current} is not accessible")
            continue
    python_files.sort()
    return python_files, warnings


def _parse_code_units(file_path: Path, project_root: Path) -> tuple[list[CodeUnit], list[ParseError]]:
    try:
        source = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        return [], [ParseError(path=file_path, message=str(exc))]
    source_lines = source.splitlines(keepends=True)
    rel_path = file_path.relative_to(project_root)
    try:
        module = ast.parse(source, filename=str(file_path))
    except SyntaxError as exc:
        message = f"{exc.msg} (line {exc.lineno})"
        return [], [ParseError(path=file_path, message=message)]
    visitor = _CodeUnitVisitor(file_path=file_path, rel_path=rel_path, source_lines=source_lines)
    visitor.visit(module)
    return visitor.units, []


def _build_clusters(units: Sequence[CodeUnit]) -> list[DuplicateCluster]:
    buckets: dict[str, list[CodeUnit]] = {}
    for unit in units:
        buckets.setdefault(unit.ast_hash, []).append(unit)
    clusters: list[DuplicateCluster] = []
    for ast_hash, members in buckets.items():
        if len(members) > 1:
            ordered = tuple(sorted(members, key=lambda item: (item.rel_path.as_posix(), item.start_line)))
            clusters.append(DuplicateCluster(ast_hash=ast_hash, members=ordered))
    clusters.sort(key=lambda cluster: (len(cluster.members), cluster.ast_hash), reverse=True)
    return clusters


def _build_near_duplicates(units: Sequence[CodeUnit]) -> list[NearDuplicatePair]:
    candidates = [
        unit
        for unit in units
        if "tests" not in unit.rel_path.parts
    ]
    pairs: list[NearDuplicatePair] = []
    for left, right in combinations(candidates, 2):
        if left.ast_hash == right.ast_hash:
            continue
        jaccard = _jaccard(left.tokens, right.tokens)
        if jaccard < 0.85:
            continue
        lcs = _lcs_ratio(left.tokens, right.tokens)
        if lcs < 0.9:
            continue
        divergences = []
        if left.norm_loc != right.norm_loc:
            divergences.append(f"norm_loc:{left.norm_loc}->{right.norm_loc}")
        counter_a = left.token_multiset
        counter_b = right.token_multiset
        sym_diff = sum((counter_a - counter_b).values()) + sum((counter_b - counter_a).values())
        if sym_diff:
            divergences.append(f"token_delta:{sym_diff}")
        divergence_str = ", ".join(sorted(divergences)) if divergences else "equal"
        pairs.append(
            NearDuplicatePair(
                unit_a=left,
                unit_b=right,
                jaccard=jaccard,
                lcs_ratio=lcs,
                divergences=divergence_str,
            )
        )
    pairs.sort(key=lambda item: (-item.jaccard, -item.lcs_ratio, item.unit_a.reference, item.unit_b.reference))
    return pairs


def _format_reference_cell(unit: CodeUnit) -> str:
    ref = unit.reference
    snippet = _escape_html(unit.snippet or "")
    if snippet:
        return f"[{ref}]<br><pre><code>{snippet}</code></pre>"
    return f"[{ref}]"


def _format_pair_reference(pair: NearDuplicatePair) -> str:
    ref_a = pair.unit_a.reference
    ref_b = pair.unit_b.reference
    snippet_a = _escape_html(pair.unit_a.snippet or "")
    snippet_b = _escape_html(pair.unit_b.snippet or "")
    return (
        f"[{ref_a}]<br><pre><code>{snippet_a}</code></pre>"
        f"<br>"
        f"[{ref_b}]<br><pre><code>{snippet_b}</code></pre>"
    )


_CSV_FIELDNAMES = ("symbol", "path", "lines", "kind", "role", "norm_loc", "ast_hash")


def _generate_csv_rows(units: Sequence[CodeUnit]) -> list[dict[str, str | int]]:
    rows: list[dict[str, str | int]] = []
    for unit in units:
        rows.append(
            {
                "symbol": unit.symbol,
                "path": unit.rel_path.as_posix(),
                "lines": f"L{unit.start_line}-L{unit.end_line}",
                "kind": unit.kind,
                "role": unit.role,
                "norm_loc": unit.norm_loc,
                "ast_hash": unit.ast_hash,
            }
        )
    return rows


def _render_csv(units: Sequence[CodeUnit]) -> str:
    rows = _generate_csv_rows(units)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=_CSV_FIELDNAMES)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _write_csv(path: Path, units: Sequence[CodeUnit]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        handle.write(_render_csv(units))
    os.replace(tmp, path)


def _render_markdown(
    units: Sequence[CodeUnit],
    clusters: Sequence[DuplicateCluster],
    pairs: Sequence[NearDuplicatePair],
    tests_present: bool,
) -> str:
    buffer = io.StringIO()
    buffer.write("# Shared code map\n\n")
    if not tests_present:
        buffer.write("> ARCHIVE_TESTS: missing in branch\n\n")
    buffer.write("| Symbol | Kind | Role | Lines | Norm LOC | AST Hash | Reference |\n")
    buffer.write("| --- | --- | --- | --- | --- | --- | --- |\n")
    for unit in units:
        reference_cell = _format_reference_cell(unit)
        buffer.write(
            f"| `{unit.symbol}` | {unit.kind} | {unit.role} | "
            f"L{unit.start_line}-L{unit.end_line} | {unit.norm_loc} | `{unit.ast_hash}` | {reference_cell} |\n"
        )
    buffer.write("\n## Duplicate clusters\n\n")
    if not clusters:
        buffer.write("No duplicates found.\n\n")
    else:
        for cluster in clusters:
            buffer.write(f"### AST Hash `{cluster.ast_hash}` — {len(cluster.members)} entries\n\n")
            for cluster_member in cluster.members:
                buffer.write(
                    f"- `{cluster_member.symbol}` ({cluster_member.kind}, {cluster_member.role}) — "
                    f"[{cluster_member.reference}]\n"
                )
            buffer.write("\n")
    buffer.write("## Near-duplicates\n\n")
    if not pairs:
        buffer.write("No similar fragments found.\n")
    else:
        buffer.write("| Member | Path | Similarity Jaccard | Similarity LCS | Divergences | References |\n")
        buffer.write("| --- | --- | --- | --- | --- | --- |\n")
        for pair in pairs:
            pair_label = pair.pair_label
            path_cell = f"{pair.unit_a.rel_path.as_posix()} ↔ {pair.unit_b.rel_path.as_posix()}"
            reference_cell = _format_pair_reference(pair)
            buffer.write(
                f"| `{pair_label}` | {path_cell} | {pair.jaccard:.3f} | {pair.lcs_ratio:.3f} | "
                f"{pair.divergences} | {reference_cell} |\n"
            )
    return buffer.getvalue()


def _write_markdown(
    path: Path,
    units: Sequence[CodeUnit],
    clusters: Sequence[DuplicateCluster],
    pairs: Sequence[NearDuplicatePair],
    tests_present: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(_render_markdown(units, clusters, pairs, tests_present))
    os.replace(tmp, path)


def _write_errors(path: Path, errors: Sequence[ParseError]) -> None:
    if not errors:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["path", "error"])
        writer.writeheader()
        for error in errors:
            writer.writerow({"path": error.path.as_posix(), "error": error.message})
    os.replace(tmp, path)


def _write_warnings(path: Path, warnings: Sequence[str]) -> None:
    if not warnings:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    content = "\n".join(warnings) + "\n"
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def _write_stream_block(stream: TextIO, content: str) -> None:
    stream.write(content)
    if not content.endswith("\n"):
        stream.write("\n")


def _render_to_stdout(
    formats: Sequence[str],
    units: Sequence[CodeUnit],
    clusters: Sequence[DuplicateCluster],
    pairs: Sequence[NearDuplicatePair],
    tests_present: bool,
    *,
    stream: TextIO | None = None,
) -> None:
    handle = stream or sys.stdout
    if "csv" in formats:
        csv_content = _render_csv(units)
        _write_stream_block(handle, "# dup_map.csv")
        _write_stream_block(handle, csv_content.rstrip())
    if "md" in formats:
        markdown_content = _render_markdown(units, clusters, pairs, tests_present)
        _write_stream_block(handle, "# dup_map.md")
        _write_stream_block(handle, markdown_content.rstrip())


def run_dup_finder(root: Path, out_dir: Path | None, formats: Sequence[str]) -> None:
    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)
    UnifiedLogger.bind(
        component="dup_finder",
        pipeline="tooling",
        stage="analysis",
        dataset="source",
        run_id="dup-finder",
    )
    try:
        python_files, directory_warnings = _collect_python_files(root)
        log.info(LogEvents.SCAN_START, files=len(python_files))
        code_units: list[CodeUnit] = []
        parse_errors: list[ParseError] = []
        for path in python_files:
            units, errors = _parse_code_units(path, root)
            if units:
                code_units.extend(units)
            if errors:
                parse_errors.extend(errors)
        code_units.sort(key=lambda item: (item.rel_path.as_posix(), item.start_line))
        log.info(LogEvents.SCAN_COMPLETE, units=len(code_units), errors=len(parse_errors))
        clusters = _build_clusters(code_units)
        near_duplicates = _build_near_duplicates(code_units)
        tests_present = (root / "tests").exists()
        if not out_dir or str(out_dir) == "-":
            _render_to_stdout(formats, code_units, clusters, near_duplicates, tests_present)
            if parse_errors:
                log.warning(LogEvents.PARSE_ERRORS, count=len(parse_errors))
            if directory_warnings:
                log.warning(LogEvents.DIRECTORY_WARNINGS, count=len(directory_warnings))
            return
        out_dir.mkdir(parents=True, exist_ok=True)
        if "csv" in formats:
            _write_csv(out_dir / "dup_map.csv", code_units)
        if "md" in formats:
            _write_markdown(out_dir / "dup_map.md", code_units, clusters, near_duplicates, tests_present)
        if parse_errors:
            _write_errors(out_dir / "errors.csv", parse_errors)
        if directory_warnings:
            _write_warnings(out_dir / "warnings.log", directory_warnings)
        log.info(LogEvents.ARTIFACTS_WRITTEN,
            csv="csv" in formats,
            markdown="md" in formats,
            errors=len(parse_errors),
            warnings=len(directory_warnings),
            output=str(out_dir),
        )
    finally:
        UnifiedLogger.reset()


def _parse_formats(option: str) -> tuple[str, ...]:
    normalized = tuple(
        dict.fromkeys(
            item.strip().lower()
            for item in option.split(",")
            if item.strip()
        )
    )
    if not normalized:
        raise ValueError("At least one output format must be specified (md,csv).")
    invalid = [item for item in normalized if item not in _VALID_FORMATS]
    if invalid:
        raise ValueError(f"Invalid formats: {', '.join(sorted(set(invalid)))}")
    return normalized


def main(
    *,
    root: Path | None = None,
    out: Path | None = None,
    fmt: str = "md,csv",
) -> None:
    """Validate parameters and execute the duplicate finder workflow."""

    formats = _parse_formats(fmt)
    resolved_root = root or DEFAULT_ROOT
    resolved_out = out if out is not None else DEFAULT_OUTPUT_DIR
    run_dup_finder(resolved_root, resolved_out, formats)


if __name__ == "__main__":
    main()

