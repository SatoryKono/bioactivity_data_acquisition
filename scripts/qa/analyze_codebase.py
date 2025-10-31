"""Utilities for generating quality-analysis artifacts.

This module provides a CLI that inspects the repository source tree and
produces a collection of reproducible reports. The reports currently checked
into ``reports/`` are preserved as historical artifacts – see ``reports/README.md``
for details:

* ``artifacts/module_map.json`` – mapping of python modules to their files
  and import dependencies.
* ``artifacts/import_graph.mmd`` – a Mermaid graph visualising intra-project
  imports.
* ``reports/token_clones.csv`` – clone candidates detected via a token
  normalisation strategy.
* ``reports/ast_clones.csv`` – clone candidates identified by structural AST
  equivalence.
* ``reports/semantic_clones.csv`` – clone candidates detected after a light
  semantic normalisation pass.
* ``reports/config_duplicates.csv`` – duplicated configuration payloads across
  ``configs/``.

The outputs are fully deterministic to support reproducibility.
"""

from __future__ import annotations

import argparse
import ast
import copy
import csv
import hashlib
import io
import json
import keyword
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from collections.abc import Iterator, Sequence

try:  # pragma: no cover - optional dependency
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    yaml = None  # type: ignore

try:  # pragma: no cover - runtime convenience when executed via ``python scripts/...``
    from bioetl.config.paths import get_configs_root
except ModuleNotFoundError:  # pragma: no cover - fallback for repo-local execution
    sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))
    from bioetl.config.paths import get_configs_root


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModuleInfo:
    """Python module metadata."""

    name: str
    path: Path
    imports: tuple[str, ...]

    def to_json(self, repo_root: Path) -> dict[str, object]:
        return {
            "module": self.name,
            "path": str(self.path.relative_to(repo_root)),
            "imports": list(self.imports),
        }


@dataclass(frozen=True)
class CodeObject:
    """A Python callable or class extracted from the source tree."""

    module: str
    file_path: Path
    qualname: str
    kind: str
    lineno: int
    end_lineno: int
    source: str

    def relative_path(self, repo_root: Path) -> str:
        return str(self.file_path.relative_to(repo_root))


def iter_python_files(base_dirs: Sequence[Path]) -> Iterator[Path]:
    for base_dir in base_dirs:
        if not base_dir.exists():
            continue
        for path in sorted(base_dir.rglob("*.py")):
            if path.is_file():
                yield path


def module_name_from_path(path: Path, base_dir: Path) -> str | None:
    try:
        relative = path.relative_to(base_dir)
    except ValueError:
        return None
    parts = list(relative.parts)
    if not parts:
        return None
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = Path(parts[-1]).stem
    if base_dir.name != "src":
        parts = [base_dir.name] + parts
    module = ".".join(part for part in parts if part)
    return module or None


def parse_imports(tree: ast.AST, module_name: str) -> tuple[str, ...]:
    imports: set[str] = set()
    module_parts = module_name.split(".") if module_name else []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            base_module = node.module or ""
            if node.level:
                prefix = module_parts[:-node.level] if node.level <= len(module_parts) else []
                if base_module:
                    base = ".".join(prefix + [base_module])
                else:
                    base = ".".join(prefix)
            else:
                base = base_module
            if base:
                imports.add(base)
            for alias in node.names:
                if alias.name == "*":
                    continue
                if base:
                    imports.add(f"{base}.{alias.name}")
                else:
                    imports.add(alias.name)
    return tuple(sorted(imports))


def gather_code_objects(
    module: str,
    path: Path,
    tree: ast.AST,
    source_lines: Sequence[str],
) -> list[CodeObject]:
    objects: list[CodeObject] = []

    def build_source(node: ast.AST) -> str:
        start = getattr(node, "lineno", None)
        end = getattr(node, "end_lineno", None)
        if start is None or end is None:
            return ""
        segment = source_lines[start - 1 : end]
        return "\n".join(segment)

    def visit(node: ast.AST, parents: list[str], in_class: bool = False) -> None:
        for child in ast.iter_child_nodes(node):
            child_in_class = in_class or isinstance(child, ast.ClassDef)
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = child.name
                qualname_parts = [module] if module else []
                qualname_parts.extend(parents)
                qualname_parts.append(name)
                qualname = ".".join(part for part in qualname_parts if part)
                kind = (
                    "class"
                    if isinstance(child, ast.ClassDef)
                    else "async_method" if child_in_class and isinstance(child, ast.AsyncFunctionDef)
                    else "method" if child_in_class and isinstance(child, ast.FunctionDef)
                    else "async_function" if isinstance(child, ast.AsyncFunctionDef)
                    else "function"
                )
                source = build_source(child)
                lineno = getattr(child, "lineno", 0)
                end_lineno = getattr(child, "end_lineno", lineno)
                objects.append(
                    CodeObject(
                        module=module,
                        file_path=path,
                        qualname=qualname,
                        kind=kind,
                        lineno=lineno,
                        end_lineno=end_lineno,
                        source=source,
                    )
                )
                next_parents = parents + [name]
                visit(child, next_parents, in_class=isinstance(child, ast.ClassDef))
            else:
                visit(child, parents, in_class=child_in_class)

    visit(tree, [])
    return objects


def token_signature(source: str) -> str | None:
    if not source.strip():
        return None
    tokens: list[str] = []
    try:
        for tok in tokenize_source(source):
            tokens.append(tok)
    except SyntaxError:
        return None
    if not tokens:
        return None
    joined = "|".join(tokens)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def tokenize_source(source: str) -> Iterator[str]:
    import tokenize

    stream = io.StringIO(source)
    for tok in tokenize.generate_tokens(stream.readline):
        tok_type = tok.type
        tok_string = tok.string
        if tok_type in {tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER}:
            continue
        if tok_type == tokenize.NAME and not keyword.iskeyword(tok_string):
            tok_repr = "NAME"
        elif tok_type == tokenize.NUMBER:
            tok_repr = "NUMBER"
        elif tok_type == tokenize.STRING:
            tok_repr = "STRING"
        else:
            tok_repr = tok_string
        yield f"{tok_type}:{tok_repr}"


def ast_signature(node: ast.AST) -> str:
    return ast.dump(node, include_attributes=False)


class SemanticNormaliser(ast.NodeTransformer):
    """AST transformer that abstracts identifiers and constants."""

    def visit_Name(self, node: ast.Name) -> ast.AST:  # type: ignore[override]
        return ast.copy_location(ast.Name(id="VAR", ctx=type(node.ctx)()), node)

    def visit_Attribute(self, node: ast.Attribute) -> ast.AST:  # type: ignore[override]
        new_node = ast.Attribute(
            value=self.visit(node.value),
            attr="ATTR",
            ctx=type(node.ctx)(),
        )
        return ast.copy_location(new_node, node)

    def visit_arg(self, node: ast.arg) -> ast.AST:  # type: ignore[override]
        new_node = ast.arg(arg="ARG", annotation=self.visit(node.annotation) if node.annotation else None, type_comment=None)
        return ast.copy_location(new_node, node)

    def visit_Constant(self, node: ast.Constant) -> ast.AST:  # type: ignore[override]
        placeholder = {
            str: "CONST_STR",
            int: "CONST_INT",
            float: "CONST_FLOAT",
            complex: "CONST_COMPLEX",
            bytes: "CONST_BYTES",
        }.get(type(node.value), "CONST")
        new_node = ast.Name(id=placeholder, ctx=ast.Load())
        return ast.copy_location(new_node, node)

    def visit_Str(self, node: ast.Str) -> ast.AST:  # pragma: no cover - legacy nodes
        return self.visit_Constant(ast.Constant(value=node.s))

    def visit_Num(self, node: ast.Num) -> ast.AST:  # pragma: no cover
        return self.visit_Constant(ast.Constant(value=node.n))

    def generic_visit(self, node: ast.AST) -> ast.AST:  # type: ignore[override]
        return super().generic_visit(node)


def semantic_signature(node: ast.AST) -> str:
    normaliser = SemanticNormaliser()
    node_copy = copy.deepcopy(node)
    if hasattr(ast, "fix_missing_locations"):
        node_copy = ast.fix_missing_locations(node_copy)
    transformed = normaliser.visit(node_copy)
    return ast.dump(transformed, include_attributes=False)


@dataclass
class CloneGroup:
    signature: str
    members: list[CodeObject]


def detect_clone_groups(code_objects: Sequence[CodeObject], signature_builder) -> list[CloneGroup]:
    grouped: dict[str, list[CodeObject]] = defaultdict(list)
    for obj in code_objects:
        node = extract_ast_node(obj)
        if node is None:
            continue
        signature = signature_builder(node, obj.source)
        if signature is None:
            continue
        grouped[signature].append(obj)
    result: list[CloneGroup] = []
    for signature, members in grouped.items():
        if len(members) > 1:
            sorted_members = sorted(
                members,
                key=lambda item: (str(item.file_path), item.lineno, item.end_lineno, item.qualname),
            )
            result.append(CloneGroup(signature=signature, members=sorted_members))
    result.sort(key=lambda group: group.signature)
    return result


def extract_ast_node(obj: CodeObject) -> ast.AST | None:
    try:
        tree = ast.parse(obj.source)
    except SyntaxError:
        return None
    if not tree.body:
        return None
    return tree.body[0]


def node_token_signature(node: ast.AST, source: str) -> str | None:
    del node
    return token_signature(source)


def node_ast_signature(node: ast.AST, source: str) -> str:
    del source
    return ast_signature(node)


def node_semantic_signature(node: ast.AST, source: str) -> str:
    del source
    return semantic_signature(node)


def write_clone_report(path: Path, groups: Sequence[CloneGroup], repo_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["clone_id", "signature", "member_index", "file", "qualified_name", "kind", "lineno", "end_lineno"])
        for index, group in enumerate(groups, start=1):
            signature_hash = hashlib.sha256(group.signature.encode("utf-8")).hexdigest()
            for member_idx, member in enumerate(group.members, start=1):
                writer.writerow(
                    [
                        index,
                        signature_hash,
                        member_idx,
                        member.relative_path(repo_root),
                        member.qualname,
                        member.kind,
                        member.lineno,
                        member.end_lineno,
                    ]
                )


def ensure_logger(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="[%(levelname)s] %(message)s")


def build_module_map(repo_root: Path, base_dirs: Sequence[Path]) -> list[ModuleInfo]:
    modules: list[ModuleInfo] = []
    for file_path in iter_python_files(base_dirs):
        base_dir = next((base for base in base_dirs if file_path.is_relative_to(base)), None)
        if base_dir is None:
            continue
        module_name = module_name_from_path(file_path, base_dir)
        if module_name is None:
            continue
        try:
            source = file_path.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (UnicodeDecodeError, SyntaxError) as exc:
            LOGGER.warning("Skipping %s: %s", file_path, exc)
            continue
        imports = parse_imports(tree, module_name)
        modules.append(ModuleInfo(name=module_name, path=file_path, imports=imports))
    modules.sort(key=lambda item: item.name)
    return modules


def gather_all_code_objects(modules: Sequence[ModuleInfo]) -> list[CodeObject]:
    code_objects: list[CodeObject] = []
    for module in modules:
        try:
            source = module.path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            LOGGER.warning("Skipping %s for clone detection: %s", module.path, exc)
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            LOGGER.warning("Skipping %s for clone detection: %s", module.path, exc)
            continue
        objects = gather_code_objects(module.name, module.path, tree, source.splitlines())
        code_objects.extend(objects)
    code_objects.sort(key=lambda item: (str(item.file_path), item.lineno, item.qualname))
    return code_objects


def write_module_map(path: Path, modules: Sequence[ModuleInfo], repo_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {module.name: module.to_json(repo_root) for module in modules}
    with path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, sort_keys=True)
        stream.write("\n")


def build_mermaid_graph(modules: Sequence[ModuleInfo]) -> str:
    import re

    def normalise(identifier: str) -> str:
        return re.sub(r"[^0-9A-Za-z_]", "_", identifier)

    lines: list[str] = ["graph TD"]
    for module in modules:
        node_id = normalise(module.name)
        lines.append(f"    {node_id}[\"{module.name}\"]")
    edges: set[tuple[str, str, str, str]] = set()
    module_lookup = {module.name for module in modules}
    for module in modules:
        source_id = normalise(module.name)
        for imported in module.imports:
            if imported in module_lookup:
                target_id = normalise(imported)
                edges.add((source_id, target_id, module.name, imported))
    for source_id, target_id, source_name, target_name in sorted(edges, key=lambda item: (item[2], item[3])):
        lines.append(f"    {source_id} --> {target_id}")
    return "\n".join(lines) + "\n"


def write_mermaid_graph(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def detect_config_duplicates(config_dir: Path, repo_root: Path) -> list[tuple[str, list[str]]]:
    duplicates: dict[tuple[str, str], list[str]] = defaultdict(list)
    if not config_dir.exists():
        return []
    for path in sorted(config_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".yaml", ".yml", ".toml", ".json"}:
            continue
        relative = str(path.relative_to(repo_root))
        data = load_config(path)
        for key_path, value_repr, value_type in flatten_config(data):
            signature = (value_repr, value_type)
            location = f"{relative}::{key_path}" if key_path else relative
            duplicates[signature].append(location)
    results: list[tuple[str, list[str]]] = []
    for (value_repr, value_type), locations in sorted(duplicates.items(), key=lambda item: (item[0][0], item[0][1])):
        if len(locations) > 1:
            results.append((f"{value_type}:{value_repr}", sorted(locations)))
    return results


def load_config(path: Path):  # type: ignore[override]
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    try:
        if suffix in {".yaml", ".yml"}:
            if yaml is None:
                LOGGER.warning("PyYAML is not available; falling back to line-based parsing for %s", path)
                return simple_kv_parse(text)
            return yaml.safe_load(text) or {}
        if suffix == ".toml":
            if tomllib is None:
                LOGGER.warning("tomllib is not available; falling back to line-based parsing for %s", path)
                return simple_kv_parse(text)
            return tomllib.loads(text)
        if suffix == ".json":
            return json.loads(text)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("Failed to parse %s: %s", path, exc)
        return simple_kv_parse(text)
    return simple_kv_parse(text)


def simple_kv_parse(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
        elif "=" in line:
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def flatten_config(data, prefix: tuple[str, ...] = ()) -> Iterator[tuple[str, str, str]]:
    if isinstance(data, dict):
        for key in sorted(data.keys(), key=str):
            yield from flatten_config(data[key], prefix + (str(key),))
    elif isinstance(data, list):
        for index, item in enumerate(data):
            yield from flatten_config(item, prefix + (str(index),))
    else:
        key_path = ".".join(prefix)
        try:
            value_repr = json.dumps(data, sort_keys=True, ensure_ascii=False)
        except TypeError:
            value_repr = repr(data)
        value_type = type(data).__name__
        yield key_path, value_repr, value_type


def write_config_duplicates(path: Path, duplicates: Sequence[tuple[str, list[str]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["duplicate_id", "occurrence_index", "value_signature", "location"])
        for duplicate_index, (signature, locations) in enumerate(duplicates, start=1):
            for occurrence_index, location in enumerate(locations, start=1):
                writer.writerow([duplicate_index, occurrence_index, signature, location])


def run(repo_root: Path, artifacts_dir: Path, reports_dir: Path) -> None:
    base_dirs = [repo_root / "src", repo_root / "tests"]
    modules = build_module_map(repo_root, base_dirs)
    write_module_map(artifacts_dir / "module_map.json", modules, repo_root)
    mermaid_content = build_mermaid_graph(modules)
    write_mermaid_graph(artifacts_dir / "import_graph.mmd", mermaid_content)

    code_objects = gather_all_code_objects(modules)
    token_groups = detect_clone_groups(code_objects, node_token_signature)
    ast_groups = detect_clone_groups(code_objects, node_ast_signature)
    semantic_groups = detect_clone_groups(code_objects, node_semantic_signature)
    write_clone_report(reports_dir / "token_clones.csv", token_groups, repo_root)
    write_clone_report(reports_dir / "ast_clones.csv", ast_groups, repo_root)
    write_clone_report(reports_dir / "semantic_clones.csv", semantic_groups, repo_root)

    duplicates = detect_config_duplicates(get_configs_root(), repo_root)
    write_config_duplicates(reports_dir / "config_duplicates.csv", duplicates)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate quality analysis artifacts")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2], help="Repository root")
    parser.add_argument("--artifacts-dir", type=Path, default=None, help="Output directory for artifacts")
    parser.add_argument("--reports-dir", type=Path, default=None, help="Output directory for reports")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    ensure_logger(args.verbose)
    repo_root = args.repo_root.resolve()
    artifacts_dir = (args.artifacts_dir or repo_root / "artifacts").resolve()
    reports_dir = (args.reports_dir or repo_root / "reports").resolve()
    LOGGER.info("Running QA analysis from %s", repo_root)
    run(repo_root, artifacts_dir, reports_dir)
    LOGGER.info("Artifacts written to %s and %s", artifacts_dir, reports_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
