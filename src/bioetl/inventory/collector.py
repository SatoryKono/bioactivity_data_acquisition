"""Collect and analyse metadata for source code inventory reporting."""
from __future__ import annotations

import ast
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import yaml

from .config import InventoryConfig
from .models import Cluster, InventoryRecord

_IDENTIFIER_RE = re.compile(r"(?<!^)(?=[A-Z])")


def collect_inventory(config: InventoryConfig) -> list[InventoryRecord]:
    """Collect inventory records for the configured roots."""

    pipeline_names = _discover_pipeline_names(config)
    config_index = _build_config_index(config)
    project_root = config.csv_output.parents[2]

    excluded_paths = {config.csv_output.resolve(), config.cluster_report.resolve()}

    records: list[InventoryRecord] = []
    for file_path in _iter_files(config):
        if file_path.resolve() in excluded_paths:
            continue
        source = _infer_source(file_path, pipeline_names)
        module = _module_name(file_path)
        size_kb = file_path.stat().st_size / 1024
        loc = _count_effective_loc(file_path)
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)

        if file_path.suffix.lower() == ".py":
            (
                top_symbols,
                imports,
                doc_line,
                ngrams,
                import_tokens,
            ) = _extract_python_metadata(file_path, config)
        elif file_path.suffix.lower() in {".yaml", ".yml"}:
            top_symbols = ()
            imports = ()
            doc_line = _extract_text_first_line(file_path, comment_prefixes=("#",))
            ngrams = frozenset()
            import_tokens = frozenset()
        else:
            top_symbols = ()
            imports = ()
            doc_line = _extract_text_first_line(file_path, comment_prefixes=())
            ngrams = frozenset()
            import_tokens = frozenset()

        config_keys = _resolve_config_keys(file_path, source, config_index)

        record = InventoryRecord(
            source=source,
            path=file_path.relative_to(project_root),
            module=module,
            size_kb=size_kb,
            loc=loc,
            mtime=mtime,
            top_symbols=top_symbols,
            imports_top=imports,
            docstring_first_line=doc_line,
            config_keys=config_keys,
            ngrams=ngrams,
            import_tokens=import_tokens,
        )
        records.append(record)

    records.sort(key=lambda record: (record.source, str(record.path)))
    return records


def _discover_pipeline_names(config: InventoryConfig) -> set[str]:
    names: set[str] = set()
    for root in config.root_dirs:
        for file_path in root.rglob("*.py"):
            lowered_parts = {part.lower() for part in file_path.parts}
            if "pipelines" not in lowered_parts:
                continue
            if file_path.stem.startswith("_"):
                continue
            names.add(file_path.stem.lower())
    # Add stems from config files as well for robustness
    for config_dir in config.config_dirs:
        for file_path in config_dir.glob("*.y*ml"):
            names.add(file_path.stem.lower())
    return names


def _build_config_index(config: InventoryConfig) -> dict[str, tuple[str, ...]]:
    index: dict[str, tuple[str, ...]] = {}
    for config_dir in config.config_dirs:
        if not config_dir.exists():
            continue
        for file_path in config_dir.glob("*.y*ml"):
            data = _load_yaml(file_path)
            keys = sorted(_flatten_keys(data))
            index[file_path.stem.lower()] = tuple(keys)
    return index


def _iter_files(config: InventoryConfig) -> Iterator[Path]:
    for root in config.root_dirs:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in config.extension_set:
                continue
            if "__pycache__" in path.parts:
                continue
            yield path


def _infer_source(path: Path, pipeline_names: set[str]) -> str:
    parts = path.parts
    lowered_parts = [part.lower() for part in parts]
    for idx, part in enumerate(lowered_parts):
        if part == "pipelines" and idx + 1 < len(parts):
            return Path(parts[idx + 1]).stem.lower()
    stem_lower = path.stem.lower()
    for name in sorted(pipeline_names):
        if name in lowered_parts or name in stem_lower:
            return name
    if "requirements" in lowered_parts:
        return "documentation"
    if "tests" in lowered_parts:
        return "tests"
    if "configs" in lowered_parts:
        return "config"
    return lowered_parts[-2] if len(lowered_parts) > 1 else lowered_parts[-1]


def _module_name(path: Path) -> str:
    if path.suffix.lower() != ".py":
        return ""

    cwd = Path.cwd()
    try:
        rel = path.relative_to(cwd / "src")
        return ".".join(rel.with_suffix("").parts)
    except ValueError:
        pass

    try:
        rel = path.relative_to(cwd / "tests")
        return "tests." + ".".join(rel.with_suffix("").parts)
    except ValueError:
        pass

    return ".".join(path.with_suffix("").parts)


def _count_effective_loc(path: Path) -> int:
    comment_prefixes = {".py": "#", ".yaml": "#", ".yml": "#"}
    prefix = comment_prefixes.get(path.suffix.lower())
    count = 0
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if prefix and stripped.startswith(prefix):
            continue
        count += 1
    return count


def _extract_text_first_line(path: Path, comment_prefixes: Sequence[str]) -> str:
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if any(stripped.startswith(prefix) for prefix in comment_prefixes):
            continue
        return stripped.lstrip("# ").strip()
    return ""


def _extract_python_metadata(
    path: Path, config: InventoryConfig
) -> tuple[tuple[str, ...], tuple[str, ...], str, frozenset[str], frozenset[str]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    docstring = ast.get_docstring(tree) or ""
    doc_line = docstring.strip().splitlines()[0] if docstring else ""

    exports = _extract_all_directive(tree)
    if exports is None:
        exports = _infer_exports(tree)

    imports = sorted(_normalise_imports(tree))
    import_tokens = frozenset(imports)

    ngrams = _extract_signature_ngrams(tree, config.cluster)

    return tuple(exports), tuple(imports), doc_line, ngrams, import_tokens


def _extract_all_directive(tree: ast.AST) -> tuple[str, ...] | None:
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        values = []
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                values.append(elt.value)
                        return tuple(values)
    return None


def _infer_exports(tree: ast.AST) -> tuple[str, ...]:
    exports: list[str] = []
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            name = getattr(node, "name", "")
            if name and not name.startswith("_"):
                exports.append(name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    exports.append(target.id)
    return tuple(sorted(dict.fromkeys(exports)))


def _normalise_imports(tree: ast.AST) -> set[str]:
    imports: set[str] = set()
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                module = "." * node.level + module
            if module:
                imports.add(module)
            else:
                for alias in node.names:
                    imports.add(alias.name.split(".")[0])
    return imports


def _extract_signature_ngrams(tree: ast.Module, cluster_config) -> frozenset[str]:
    tokens: list[str] = []

    def register_from_name(name: str) -> None:
        if not name:
            return
        fragments = _split_identifier(name)
        tokens.extend(fragments)

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            register_from_name(node.name)
            for arg in node.args.args + node.args.kwonlyargs:
                register_from_name(arg.arg)
            if node.args.vararg:
                register_from_name(node.args.vararg.arg)
            if node.args.kwarg:
                register_from_name(node.args.kwarg.arg)
        elif isinstance(node, ast.ClassDef):
            register_from_name(node.name)
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    register_from_name(child.name)

    keywords = {keyword.lower() for keyword in cluster_config.focus_keywords}
    if keywords:
        tokens = [token for token in tokens if any(keyword in token for keyword in keywords)]

    ngrams: set[str] = set()
    for size in cluster_config.ngram_sizes:
        if size <= 0 or len(tokens) < size:
            continue
        for idx in range(len(tokens) - size + 1):
            window = tokens[idx : idx + size]
            ngrams.add(" ".join(window))

    return frozenset(ngrams)


def _split_identifier(name: str) -> list[str]:
    name = name.replace("-", "_")
    parts = name.split("_")
    tokens: list[str] = []
    for part in parts:
        snake = _IDENTIFIER_RE.sub(" ", part).lower()
        for fragment in snake.split():
            if fragment:
                tokens.append(fragment)
    return tokens


def _resolve_config_keys(
    path: Path, source: str, config_index: dict[str, tuple[str, ...]]
) -> tuple[str, ...]:
    keys: list[str] = []
    lower_source = source.lower()
    if path.suffix.lower() in {".yaml", ".yml"}:
        data = _load_yaml(path)
        keys = sorted(_flatten_keys(data))
    elif lower_source in config_index:
        keys = list(config_index[lower_source])
    return tuple(keys)


def _flatten_keys(data, prefix: str = "") -> set[str]:  # type: ignore[override]
    keys: set[str] = set()
    if isinstance(data, dict):
        for key, value in data.items():
            key_str = str(key)
            next_prefix = f"{prefix}.{key_str}" if prefix else key_str
            keys.add(next_prefix)
            keys.update(_flatten_keys(value, next_prefix))
    elif isinstance(data, list):
        for item in data:
            keys.update(_flatten_keys(item, prefix))
    return keys


def _load_yaml(path: Path):
    class Loader(yaml.SafeLoader):
        pass

    def _construct_include(loader: yaml.SafeLoader, node: yaml.Node):
        relative_path = loader.construct_scalar(node)
        target = (path.parent / relative_path).resolve()
        return _load_yaml(target)

    Loader.add_constructor("!include", _construct_include)
    with path.open(encoding="utf-8") as handle:
        return yaml.load(handle, Loader=Loader)


def analyse_clusters(records: Sequence[InventoryRecord], config: InventoryConfig) -> list[Cluster]:
    """Perform clustering based on signature n-grams and import overlap."""

    python_records = [record for record in records if record.is_python]
    if len(python_records) < 2:
        return []

    adjacency: dict[InventoryRecord, set[InventoryRecord]] = defaultdict(set)
    cluster_conf = config.cluster

    for left, right in combinations(python_records, 2):
        jaccard = _jaccard(left.ngrams, right.ngrams)
        import_overlap = _jaccard(left.import_tokens, right.import_tokens)
        share_ngrams = len(left.ngrams & right.ngrams)
        share_imports = len(left.import_tokens & right.import_tokens)

        if share_ngrams < cluster_conf.min_shared_ngrams and share_imports < cluster_conf.min_shared_imports:
            continue
        if jaccard < cluster_conf.min_jaccard and import_overlap < cluster_conf.min_import_overlap:
            continue
        adjacency[left].add(right)
        adjacency[right].add(left)

    visited: set[InventoryRecord] = set()
    clusters: list[Cluster] = []
    for record in python_records:
        if record in visited:
            continue
        component = _explore_component(record, adjacency, visited)
        if len(component) < 2:
            continue
        clusters.append(_build_cluster_summary(component))

    clusters.sort(key=lambda cluster: (-len(cluster.members), cluster.members[0].path))
    return clusters


def _explore_component(
    start: InventoryRecord,
    adjacency: dict[InventoryRecord, set[InventoryRecord]],
    visited: set[InventoryRecord],
) -> list[InventoryRecord]:
    stack = [start]
    component: list[InventoryRecord] = []
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        component.append(node)
        for neighbour in adjacency.get(node, set()):
            if neighbour not in visited:
                stack.append(neighbour)
    component.sort(key=lambda record: str(record.path))
    return component


def _build_cluster_summary(component: Sequence[InventoryRecord]) -> Cluster:
    if not component:
        raise ValueError("Component must contain at least one record")

    ngram_sets = [record.ngrams for record in component if record.ngrams]
    import_sets = [record.import_tokens for record in component if record.import_tokens]

    common_ngrams: tuple[str, ...] = tuple(sorted(_intersection_all(ngram_sets)))
    common_imports: tuple[str, ...] = tuple(sorted(_intersection_all(import_sets)))

    jaccard_scores: list[float] = []
    import_scores: list[float] = []
    for left, right in combinations(component, 2):
        jaccard_scores.append(_jaccard(left.ngrams, right.ngrams))
        import_scores.append(_jaccard(left.import_tokens, right.import_tokens))

    average_jaccard = sum(jaccard_scores) / len(jaccard_scores) if jaccard_scores else 0.0
    average_import_overlap = (
        sum(import_scores) / len(import_scores) if import_scores else 0.0
    )

    responsibility = _summarise_responsibility(component)
    divergence_points = _identify_divergence_points(component)

    return Cluster(
        members=tuple(component),
        common_ngrams=common_ngrams,
        common_imports=common_imports,
        average_jaccard=average_jaccard,
        average_import_overlap=average_import_overlap,
        responsibility=responsibility,
        divergence_points=divergence_points,
    )


def _summarise_responsibility(component: Sequence[InventoryRecord]) -> str:
    counts = Counter(record.source for record in component)
    if not counts:
        return "unknown"

    total = sum(counts.values())
    primary, primary_count = counts.most_common(1)[0]
    top_sources = counts.most_common(5)
    fragments = [f"{source}={count}" for source, count in top_sources]
    if len(counts) > len(top_sources):
        remaining = sum(count for _, count in counts.items()) - sum(count for _, count in top_sources)
        fragments.append(f"others={remaining}")
    distribution = ", ".join(fragments)
    return f"primary={primary} ({primary_count}/{total}); top sources: {distribution}"


def _identify_divergence_points(component: Sequence[InventoryRecord]) -> tuple[str, ...]:
    divergence: set[str] = set()
    python_members = [record for record in component if record.is_python]

    if python_members:
        symbol_sets = {record.top_symbols for record in python_members}
        if len(symbol_sets) > 1:
            divergence.add("public_api")

        import_sets = {record.import_tokens for record in python_members}
        if len(import_sets) > 1:
            divergence.add("dependencies")

    config_sets = {record.config_keys for record in component if record.config_keys}
    if len(config_sets) > 1:
        divergence.add("config_keys")

    extension_set = {record.file_extension for record in component}
    if len(extension_set) > 1:
        divergence.add("data_formats")

    path_tokens = [record.path.as_posix().lower() for record in component]
    if any("schema" in token for token in path_tokens):
        divergence.add("schemas")
    elif any("schema" in token for record in python_members for token in record.import_tokens):
        divergence.add("schemas")

    io_keywords = ("output", "writer", "log", "logger")
    if any(any(keyword in token for keyword in io_keywords) for token in path_tokens):
        divergence.add("io_logging")

    return tuple(sorted(divergence))


def _jaccard(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set and not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _intersection_all(sets: Sequence[Iterable[str]]) -> set[str]:
    iterator = iter(sets)
    try:
        result = set(next(iterator))
    except StopIteration:
        return set()
    for other in iterator:
        result &= set(other)
    return result
