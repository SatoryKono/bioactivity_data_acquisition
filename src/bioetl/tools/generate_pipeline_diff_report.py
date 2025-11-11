from __future__ import annotations

import ast
import difflib
import hashlib
import io
import itertools
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence, cast

PIPELINE_ENTITIES: Sequence[str] = ("activity", "assay", "document", "target", "testitem")
MODULE_PRIORITY: Sequence[str] = ("run.py", "transform.py", "normalize.py")


def _read_source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _collapse_blank_lines(source: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", source)


def _normalize_quotes(source: str) -> str:
    try:
        import tokenize
    except ImportError as exc:
        raise RuntimeError("tokenize module is required") from exc

    tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    updated_tokens: list[tuple[int, str]] = []
    for tok_type, tok_string, *_ in tokens:
        if tok_type == tokenize.STRING:
            try:
                value = ast.literal_eval(tok_string)
            except Exception:
                updated_tokens.append((tok_type, tok_string))
                continue
            if isinstance(value, str):
                updated_tokens.append((tok_type, json.dumps(value)))
                continue
        if tok_type == tokenize.INDENT:
            updated_tokens.append((tok_type, tok_string.replace("\t", "    ")))
        else:
            updated_tokens.append((tok_type, tok_string))
    return cast(str, tokenize.untokenize(updated_tokens))


def _format_import(node: ast.stmt) -> str:
    return ast.unparse(node)


def _sort_import_aliases(node: ast.stmt) -> None:
    if isinstance(node, ast.Import):
        node.names.sort(key=lambda alias: alias.name)
    elif isinstance(node, ast.ImportFrom):
        node.names.sort(key=lambda alias: alias.name)


def _normalize_module_source(module: ast.Module) -> str:
    body = list(module.body)
    docstring_node: ast.stmt | None = None
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        docstring_node = body[0]
        body = body[1:]
    import_nodes: list[ast.stmt] = []
    other_nodes: list[ast.stmt] = []
    for stmt in body:
        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            _sort_import_aliases(stmt)
            import_nodes.append(stmt)
        else:
            other_nodes.append(stmt)
    import_nodes.sort(key=_format_import)
    new_body: list[ast.stmt] = []
    if docstring_node is not None:
        new_body.append(docstring_node)
    new_body.extend(import_nodes)
    new_body.extend(other_nodes)
    module.body = new_body
    ast.fix_missing_locations(module)
    source = ast.unparse(module)
    source = _normalize_quotes(source)
    source = _collapse_blank_lines(source)
    lines = [line.rstrip() for line in source.splitlines()]
    return "\n".join(lines) + "\n"


def _hash_ast(node: ast.AST) -> str:
    dump = ast.dump(node, include_attributes=False)
    digest = hashlib.blake2b(dump.encode("utf-8"), digest_size=16)
    return digest.hexdigest()


def _token_set(source: str) -> set[str]:
    import tokenize

    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)
        return {
            tok.string
            for tok in tokens
            if tok.type not in (tokenize.NEWLINE, tokenize.NL, tokenize.INDENT, tokenize.DEDENT)
        }
    except tokenize.TokenError:
        return set(re.findall(r"[A-Za-z0-9_]+", source))


def _get_call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _get_call_name(node.value)
        if base is None:
            return None
        return f"{base}.{node.attr}"
    return None


def _detect_side_effects(node: ast.AST) -> dict[str, list[str]]:
    logging_calls: set[str] = set()
    io_calls: set[str] = set()
    for subnode in ast.walk(node):
        if isinstance(subnode, ast.Call):
            name = _get_call_name(subnode.func)
            if not name:
                continue
            lowered = name.lower()
            if "log" in lowered:
                logging_calls.add(name)
            if any(marker in lowered for marker in ("write", "open", "read", "dump", "save", "load", "request")):
                io_calls.add(name)
    return {
        "logging": sorted(logging_calls),
        "io": sorted(io_calls),
    }


def _collect_exceptions(node: ast.AST) -> list[str]:
    exceptions: set[str] = set()
    for subnode in ast.walk(node):
        if isinstance(subnode, ast.Raise) and subnode.exc is not None:
            try:
                exceptions.add(ast.unparse(subnode.exc))
            except Exception:
                exceptions.add(type(subnode.exc).__name__)
    return sorted(exceptions)


def _signature_for_function(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args = node.args
    parts: list[str] = []
    if args.posonlyargs:
        for arg in args.posonlyargs:
            parts.append(_format_arg(arg))
        parts.append("/")
    for arg in args.args:
        parts.append(_format_arg(arg))
    if args.vararg is not None:
        parts.append(f"*{_format_arg(args.vararg, omit_annotation=True)}")
    if args.kwonlyargs:
        if args.vararg is None:
            parts.append("*")
        for kw, default in zip(args.kwonlyargs, args.kw_defaults, strict=False):
            parts.append(_format_arg(kw, default))
    if args.kwarg is not None:
        parts.append(f"**{_format_arg(args.kwarg, omit_annotation=True)}")
    return ", ".join(parts)


def _format_arg(arg: ast.arg, default: ast.expr | None = None, omit_annotation: bool = False) -> str:
    name = arg.arg
    annotation_part = ""
    if not omit_annotation and arg.annotation is not None:
        annotation_part = f": {ast.unparse(arg.annotation)}"
    default_part = ""
    if default is not None:
        default_part = f" = {ast.unparse(default)}"
    return f"{name}{annotation_part}{default_part}"


def _return_annotation(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    if node.returns is None:
        return None
    return ast.unparse(node.returns)


def _normalized_block(node: ast.AST) -> str:
    if not isinstance(node, ast.stmt):
        raise TypeError("Expected ast.stmt node for normalization")
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    source = ast.unparse(module)
    source = _normalize_quotes(source)
    source = _collapse_blank_lines(source)
    lines = [line.rstrip() for line in source.splitlines() if line.strip()]
    return "\n".join(lines) + "\n"


@dataclass(slots=True)
class DefinitionInfo:
    qualname: str
    kind: str
    signature: str | None
    returns: str | None
    start_line: int
    end_line: int
    side_effects: dict[str, list[str]]
    exceptions: list[str]
    normalized_source: str
    ast_hash: str
    token_set: set[str]


@dataclass(slots=True)
class ModuleAnalysis:
    entity: str
    path: Path
    normalized_source: str
    definitions: dict[str, DefinitionInfo] = field(default_factory=dict)
    module_hash: str = ""
    module_tokens: set[str] = field(default_factory=set)
    module_level_blocks: list[DefinitionInfo] = field(default_factory=list)


def _collect_definitions(module: ast.Module) -> dict[str, DefinitionInfo]:
    definitions: dict[str, DefinitionInfo] = {}

    def visit_body(body: Iterable[ast.stmt], prefix: str) -> None:
        for stmt in body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                qualname = f"{prefix}{stmt.name}"
                signature = _signature_for_function(stmt)
                returns = _return_annotation(stmt)
                normalized = _normalized_block(stmt)
                ast_hash = _hash_ast(stmt)
                tokens = _token_set(normalized)
                definitions[qualname] = DefinitionInfo(
                    qualname=qualname,
                    kind="async function" if isinstance(stmt, ast.AsyncFunctionDef) else "function",
                    signature=signature,
                    returns=returns,
                    start_line=stmt.lineno,
                    end_line=stmt.end_lineno or stmt.lineno,
                    side_effects=_detect_side_effects(stmt),
                    exceptions=_collect_exceptions(stmt),
                    normalized_source=normalized,
                    ast_hash=ast_hash,
                    token_set=tokens,
                )
            elif isinstance(stmt, ast.ClassDef):
                qualname = f"{prefix}{stmt.name}"
                normalized = _normalized_block(stmt)
                ast_hash = _hash_ast(stmt)
                tokens = _token_set(normalized)
                definitions[qualname] = DefinitionInfo(
                    qualname=qualname,
                    kind="class",
                    signature=None,
                    returns=None,
                    start_line=stmt.lineno,
                    end_line=stmt.end_lineno or stmt.lineno,
                    side_effects=_detect_side_effects(stmt),
                    exceptions=_collect_exceptions(stmt),
                    normalized_source=normalized,
                    ast_hash=ast_hash,
                    token_set=tokens,
                )
                visit_body(stmt.body, f"{qualname}.")

    visit_body(module.body, "")
    return definitions


def _collect_module_level_blocks(module: ast.Module) -> list[DefinitionInfo]:
    blocks: list[DefinitionInfo] = []
    for index, stmt in enumerate(module.body):
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        normalized = _normalized_block(stmt)
        if not normalized.strip():
            continue
        ast_hash = _hash_ast(stmt)
        tokens = _token_set(normalized)
        qualname = f"__module_block_{index}"
        blocks.append(
            DefinitionInfo(
                qualname=qualname,
                kind="module_block",
                signature=None,
                returns=None,
                start_line=stmt.lineno,
                end_line=stmt.end_lineno or stmt.lineno,
                side_effects=_detect_side_effects(stmt),
                exceptions=_collect_exceptions(stmt),
                normalized_source=normalized,
                ast_hash=ast_hash,
                token_set=tokens,
            )
        )
    return blocks


def analyze_module(entity: str, path: Path) -> ModuleAnalysis:
    source = _read_source(path)
    module = ast.parse(source, filename=str(path))
    normalized_source = _normalize_module_source(module)
    definitions = _collect_definitions(module)
    module_blocks = _collect_module_level_blocks(module)
    module_hash = _hash_ast(module)
    module_tokens = _token_set(normalized_source)
    return ModuleAnalysis(
        entity=entity,
        path=path,
        normalized_source=normalized_source,
        definitions=definitions,
        module_hash=module_hash,
        module_tokens=module_tokens,
        module_level_blocks=module_blocks,
    )


@dataclass(slots=True)
class PipelineAnalysis:
    entity: str
    modules: dict[str, ModuleAnalysis] = field(default_factory=dict)
    pipeline_hash: str = ""
    pipeline_tokens: set[str] = field(default_factory=set)


def analyze_pipeline(root: Path, entity: str) -> PipelineAnalysis:
    pipeline_dir = root / entity
    modules: dict[str, ModuleAnalysis] = {}
    ast_dumps: list[str] = []
    token_union: set[str] = set()
    for module_name in MODULE_PRIORITY:
        module_path = pipeline_dir / module_name
        if not module_path.exists():
            continue
        analysis = analyze_module(entity, module_path)
        modules[module_name] = analysis
        ast_dumps.append(ast.dump(ast.parse(analysis.normalized_source), include_attributes=False))
        token_union.update(analysis.module_tokens)
    pipeline_hash = hashlib.blake2b("".join(ast_dumps).encode("utf-8"), digest_size=16).hexdigest()
    return PipelineAnalysis(entity=entity, modules=modules, pipeline_hash=pipeline_hash, pipeline_tokens=token_union)


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    union = len(a | b)
    if union == 0:
        return 0.0
    return len(a & b) / union


@dataclass(slots=True)
class DiffEntry:
    entity_a: str
    entity_b: str
    module_name: str
    definition: str
    path_a: Path | None
    path_b: Path | None
    range_a: tuple[int, int] | None
    range_b: tuple[int, int] | None
    diff_text: str


def _diff_blocks(
    entity_a: str,
    entity_b: str,
    module_name: str,
    info_a: DefinitionInfo | None,
    info_b: DefinitionInfo | None,
    path_a: Path | None,
    path_b: Path | None,
) -> list[DiffEntry]:
    if info_a is None and info_b is None:
        return []
    lines_a = info_a.normalized_source.splitlines() if info_a else []
    lines_b = info_b.normalized_source.splitlines() if info_b else []
    diff_lines = [
        line.rstrip("\n")
        for line in difflib.unified_diff(
            lines_a,
            lines_b,
            fromfile=f"{entity_a}:{module_name}",
            tofile=f"{entity_b}:{module_name}",
            n=5,
        )
    ]
    if not diff_lines:
        return []
    header = diff_lines[:2]
    hunks: list[list[str]] = []
    current: list[str] = []
    for line in diff_lines[2:]:
        if line.startswith("@@"):
            if current:
                hunks.append(header + current)
                current = []
            current = [line]
        else:
            if not current:
                current = [line]
            else:
                current.append(line)
    if current:
        hunks.append(header + current)
    range_a = None
    range_b = None
    if info_a is not None:
        range_a = (info_a.start_line, info_a.end_line)
    if info_b is not None:
        range_b = (info_b.start_line, info_b.end_line)
    entries: list[DiffEntry] = []
    for index, hunk in enumerate(hunks, start=1):
        diff_text = "\n".join(hunk)
        base_name = ""
        if info_a is not None:
            base_name = info_a.qualname
        elif info_b is not None:
            base_name = info_b.qualname
        else:
            base_name = module_name
        entries.append(
            DiffEntry(
                entity_a=entity_a,
                entity_b=entity_b,
                module_name=module_name,
                definition=f"{base_name}#{index}",
                path_a=path_a,
                path_b=path_b,
                range_a=range_a,
                range_b=range_b,
                diff_text=diff_text,
            )
        )
    return entries


def compare_pipelines(pipeline_a: PipelineAnalysis, pipeline_b: PipelineAnalysis) -> dict[str, list[DiffEntry]]:
    diff_entries: dict[str, list[DiffEntry]] = {}
    module_names = sorted(set(pipeline_a.modules) | set(pipeline_b.modules), key=lambda name: MODULE_PRIORITY.index(name) if name in MODULE_PRIORITY else len(MODULE_PRIORITY))
    for module_name in module_names:
        module_a = pipeline_a.modules.get(module_name)
        module_b = pipeline_b.modules.get(module_name)
        entries: list[DiffEntry] = []
        definitions: set[str] = set()
        if module_a is not None:
            definitions.update(module_a.definitions.keys())
            definitions.update(info.qualname for info in module_a.module_level_blocks)
        if module_b is not None:
            definitions.update(module_b.definitions.keys())
            definitions.update(info.qualname for info in module_b.module_level_blocks)
        for qualname in sorted(definitions):
            info_a = module_a.definitions.get(qualname) if module_a else None
            info_b = module_b.definitions.get(qualname) if module_b else None
            if info_a is None and module_a is not None:
                info_a = next((block for block in module_a.module_level_blocks if block.qualname == qualname), None)
            if info_b is None and module_b is not None:
                info_b = next((block for block in module_b.module_level_blocks if block.qualname == qualname), None)
            diff_list = _diff_blocks(
                pipeline_a.entity,
                pipeline_b.entity,
                module_name,
                info_a,
                info_b,
                module_a.path if module_a else None,
                module_b.path if module_b else None,
            )
            entries.extend(diff_list)
        if entries:
            diff_entries[module_name] = entries
    return diff_entries


def build_ast_table(pipeline_a: PipelineAnalysis, pipeline_b: PipelineAnalysis, module_name: str) -> list[list[str]]:
    module_a = pipeline_a.modules.get(module_name)
    module_b = pipeline_b.modules.get(module_name)
    header = ["Definition", f"{pipeline_a.entity} signature", f"{pipeline_b.entity} signature", "Side effects", "Exceptions", "Status"]
    rows: list[list[str]] = [header]
    qualnames: set[str] = set()
    if module_a:
        qualnames.update(module_a.definitions.keys())
        qualnames.update(info.qualname for info in module_a.module_level_blocks)
    if module_b:
        qualnames.update(module_b.definitions.keys())
        qualnames.update(info.qualname for info in module_b.module_level_blocks)
    for qualname in sorted(qualnames):
        info_a = module_a.definitions.get(qualname) if module_a else None
        info_b = module_b.definitions.get(qualname) if module_b else None
        if info_a is None and module_a:
            info_a = next((block for block in module_a.module_level_blocks if block.qualname == qualname), None)
        if info_b is None and module_b:
            info_b = next((block for block in module_b.module_level_blocks if block.qualname == qualname), None)
        signature_a = info_a.signature if info_a else "—"
        signature_b = info_b.signature if info_b else "—"
        side_effects_a = info_a.side_effects if info_a else {}
        side_effects_b = info_b.side_effects if info_b else {}
        side_effects_repr = "<br>".join(
            [
                _format_side_effects_block(pipeline_a.entity, side_effects_a),
                _format_side_effects_block(pipeline_b.entity, side_effects_b),
            ]
        )
        exceptions_repr = "<br>".join(
            [
                _format_exceptions_block(pipeline_a.entity, info_a.exceptions if info_a else []),
                _format_exceptions_block(pipeline_b.entity, info_b.exceptions if info_b else []),
            ]
        )
        if info_a and info_b:
            status = "identical" if info_a.ast_hash == info_b.ast_hash else "differs"
        elif info_a and not info_b:
            status = f"only in {pipeline_a.entity}"
        elif info_b and not info_a:
            status = f"only in {pipeline_b.entity}"
        else:
            status = "—"
        rows.append([qualname, signature_a or "—", signature_b or "—", side_effects_repr, exceptions_repr, status])
    return rows


def _format_table(rows: list[list[str]]) -> str:
    if not rows:
        return ""
    sanitized_rows = [[cell.replace("\n", "<br>") for cell in row] for row in rows]
    widths = [max(len(row[i]) for row in sanitized_rows) for i in range(len(sanitized_rows[0]))]
    lines: list[str] = []
    header = sanitized_rows[0]
    header_line = " | ".join(cell.ljust(width) for cell, width in zip(header, widths, strict=False))
    separator = "-|-".join("-" * width for width in widths)
    lines.append(header_line)
    lines.append(separator)

    for row in sanitized_rows[1:]:
        lines.append(" | ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=False)))
    return "\n".join(lines)


def _cluster_definitions(pipeline_a: PipelineAnalysis, pipeline_b: PipelineAnalysis, module_name: str) -> list[str]:
    module_a = pipeline_a.modules.get(module_name)
    module_b = pipeline_b.modules.get(module_name)
    if module_a is None or module_b is None:
        return []
    clusters: list[str] = []
    for qualname, info_a in module_a.definitions.items():
        info_b = module_b.definitions.get(qualname)
        if info_b is None:
            continue
        if info_a.ast_hash == info_b.ast_hash:
            clusters.append(f"{qualname}: exact AST match")
            continue
        similarity = _jaccard(info_a.token_set, info_b.token_set)
        if similarity >= 0.8:
            clusters.append(f"{qualname}: Jaccard={similarity:.2f}")
    return clusters


def _format_side_effects_block(entity: str, side_effects: dict[str, list[str]]) -> str:
    if not side_effects:
        return f"{entity}: ∅"
    parts: list[str] = []
    for key in sorted(side_effects.keys()):
        values = ", ".join(side_effects[key]) if side_effects[key] else "∅"
        parts.append(f"{key}={values}")
    return f"{entity}: " + "; ".join(parts)


def _format_exceptions_block(entity: str, exceptions: Sequence[str]) -> str:
    if not exceptions:
        return f"{entity}: ∅"
    return f"{entity}: {', '.join(exceptions)}"


def _ensure_report_dir(report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)


def _write_atomic(path: Path, content: str) -> None:
    _ensure_report_dir(path)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, path)


def generate_report(root: Path, output_path: Path) -> None:
    repo_root = root.parents[3]
    analyses = {entity: analyze_pipeline(root, entity) for entity in PIPELINE_ENTITIES}
    lines: list[str] = []
    lines.append("# Comparative report for ChEMBL pipelines\n")
    for entity_a, entity_b in itertools.combinations(PIPELINE_ENTITIES, 2):
        pipeline_a = analyses[entity_a]
        pipeline_b = analyses[entity_b]
        lines.append(f"## Pair: {entity_a} ↔ {entity_b}\n")
        lines.append(f"- AST hash: {pipeline_a.pipeline_hash} ↔ {pipeline_b.pipeline_hash}\n")
        jaccard = _jaccard(pipeline_a.pipeline_tokens, pipeline_b.pipeline_tokens)
        lines.append(f"- Jaccard over tokens: {jaccard:.3f}\n")
        diff_entries = compare_pipelines(pipeline_a, pipeline_b)
        total_entries = sum(len(items) for items in diff_entries.values())
        if total_entries == 0:
            lines.append("No differences detected.\n")
            continue
        hotspot_total = 0
        truncation_flag = total_entries > 20
        for module_name, entries in diff_entries.items():
            remaining_budget = max(0, 20 - hotspot_total)
            if remaining_budget == 0:
                break
            lines.append(f"### Module {module_name}\n")
            module_a = pipeline_a.modules.get(module_name)
            module_b = pipeline_b.modules.get(module_name)
            status_a = "absent" if module_a is None else "present"
            status_b = "absent" if module_b is None else "present"
            lines.append(f"- File status: {pipeline_a.entity} — {status_a}, {pipeline_b.entity} — {status_b}")
            hash_a = module_a.module_hash if module_a is not None else "absent"
            hash_b = module_b.module_hash if module_b is not None else "absent"
            lines.append(f"- AST hash: {hash_a} ↔ {hash_b}")
            tokens_a = module_a.module_tokens if module_a is not None else set()
            tokens_b = module_b.module_tokens if module_b is not None else set()
            lines.append(f"- Jaccard over tokens: {_jaccard(tokens_a, tokens_b):.3f}\n")
            table = _format_table(build_ast_table(pipeline_a, pipeline_b, module_name))
            if table:
                lines.append(table + "\n")
            clusters = _cluster_definitions(pipeline_a, pipeline_b, module_name)
            if clusters:
                lines.append("Similar definition clusters:\n")
                for item in clusters:
                    lines.append(f"- {item}")
                lines.append("")
            capped_entries = entries[:remaining_budget]
            hotspot_total += len(capped_entries)
            for idx, entry in enumerate(capped_entries, 1):
                def format_ref(path: Path | None, span: tuple[int, int] | None) -> str:
                    if path is None or span is None:
                        return "absent"
                    try:
                        rel_path = path.relative_to(repo_root)
                    except ValueError:
                        rel_path = path
                    rel_text = rel_path.as_posix()
                    return f"{rel_text}:{span[0]}-{span[1]}"

                ref_a = format_ref(entry.path_a, entry.range_a)
                ref_b = format_ref(entry.path_b, entry.range_b)
                lines.append(f"#### Hotspot {idx}\n")
                lines.append(f"- Definition: {entry.definition}")
                lines.append(f"- {pipeline_a.entity}: {ref_a}")
                lines.append(f"- {pipeline_b.entity}: {ref_b}\n")
                lines.append("```diff")
                lines.append(entry.diff_text)
                lines.append("```\n")
            if len(entries) > len(capped_entries):
                lines.append(f"_Only the first {len(capped_entries)} hotspots of {len(entries)} are shown for module {module_name}._\n")
        if truncation_flag:
            lines.append(f"_First {hotspot_total} hotspots of {total_entries} are shown for pair {entity_a} ↔ {entity_b}._\n")
        if hotspot_total < 10:
            lines.append(f"_Warning: only {hotspot_total} key differences identified (<10)._")
        lines.append("---\n")
    report_content = "\n".join(lines)
    _write_atomic(output_path, report_content)


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]
    pipeline_root = project_root / "src" / "bioetl" / "pipelines" / "chembl"
    output_path = project_root / "REPORT" / "LINES_DIFF.md"
    generate_report(pipeline_root, output_path)


if __name__ == "__main__":
    main()

