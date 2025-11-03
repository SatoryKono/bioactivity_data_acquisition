"""Enforce the architectural dependency matrix for project modules."""

from __future__ import annotations

import argparse
import ast
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from tools.qa.analyze_codebase import module_name_from_path, parse_imports


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

BASE_DIRECTORIES: tuple[Path, ...] = (
    SRC_ROOT / "bioetl",
    SRC_ROOT / "scripts",
)


Layer = str


# Explicit dependency permissions; same-layer and external imports are allowed.
ALLOWED_DEPENDENCIES: dict[Layer, set[Layer]] = {
    "adapters": {"core", "normalizers", "sources"},
    "cli": {"config", "core", "pipelines", "scripts", "sources", "utils"},
    "clients": {"core", "normalizers", "pipelines", "sources", "utils"},
    "config": {"utils"},
    "configs": {"config"},
    "core": {"config", "pandera", "schemas", "utils"},
    "inventory": {"config"},
    "normalizers": {"core"},
    "pandera": set(),
    "pipelines": {
        "clients",
        "config",
        "core",
        "normalizers",
        "pandera",
        "schemas",
        "sources",
        "transform",
        "utils",
    },
    "schemas": {"core", "pandera", "sources"},
    "scripts": {"cli", "config", "core", "inventory", "pipelines", "utils"},
    "sources": {
        "adapters",
        "clients",
        "config",
        "core",
        "normalizers",
        "pandera",
        "pipelines",
        "schemas",
        "transform",
        "utils",
    },
    "transform": {"core", "normalizers", "sources"},
    "utils": {"config", "core", "normalizers", "schemas"},
}


def iter_python_files(base_dir: Path) -> Iterable[Path]:
    """Yield Python files under ``base_dir`` (recursively)."""

    if not base_dir.exists():
        return
    for path in sorted(base_dir.rglob("*.py")):
        if path.is_file():
            yield path


def layer_for_path(path: Path) -> Layer:
    """Return the architectural layer for ``path``."""

    parts = path.relative_to(PROJECT_ROOT).parts
    if not parts:
        return "external"
    if parts[0] == "tests":
        return "tests"
    if len(parts) < 2 or parts[0] != "src":
        return "external"

    top_level = parts[1]
    if top_level == "scripts":
        return "scripts"
    if top_level == "library":
        return "library"
    if top_level != "bioetl":
        return "external"

    if len(parts) == 2:
        return "bioetl"
    second = parts[2]
    if second.endswith(".py"):
        if second in {"pandera_pandas.py", "pandera_typing.py"}:
            return "pandera"
        return "bioetl"
    if second in {
        "adapters",
        "cli",
        "clients",
        "config",
        "configs",
        "core",
        "inventory",
        "normalizers",
        "pipelines",
        "schemas",
        "sources",
        "transform",
        "utils",
    }:
        return second
    return "bioetl"


def resolve_known_module(module: str, known_modules: dict[str, Path]) -> str | None:
    """Resolve ``module`` to the nearest known project module."""

    candidate = module
    while candidate:
        if candidate in known_modules:
            return candidate
        if "." not in candidate:
            break
        candidate = candidate.rsplit(".", 1)[0]
    return None


def build_module_graph() -> tuple[dict[str, Path], dict[str, set[str]]]:
    """Return mapping of module to path and imports."""

    module_paths: dict[str, Path] = {}
    module_imports: dict[str, set[str]] = defaultdict(set)

    for base_dir in BASE_DIRECTORIES:
        for path in iter_python_files(base_dir):
            module = module_name_from_path(path, base_dir)
            if not module:
                continue
            module_paths[module] = path
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            imports = parse_imports(tree, module)
            module_imports[module].update(imports)

    return module_paths, module_imports


def check_dependency_matrix() -> int:
    module_paths, module_imports = build_module_graph()
    violations: list[str] = []

    layer_cache = {module: layer_for_path(path) for module, path in module_paths.items()}

    for module, imports in module_imports.items():
        src_layer = layer_cache.get(module, "external")
        if src_layer not in ALLOWED_DEPENDENCIES and src_layer not in {"external", "bioetl"}:
            # Layers without explicit policy default to allowing only same-layer imports.
            allowed = set()
        else:
            allowed = ALLOWED_DEPENDENCIES.get(src_layer, set())

        for imported in imports:
            resolved = resolve_known_module(imported, module_paths)
            if not resolved:
                continue  # External dependency.
            tgt_layer = layer_cache.get(resolved, "external")
            if tgt_layer in {"external", src_layer}:
                continue
            if tgt_layer == "bioetl":
                continue
            if tgt_layer not in allowed:
                violations.append(f"{module} ({src_layer}) -> {resolved} ({tgt_layer})")

    if violations:
        details = "\n  - ".join(sorted(violations))
        print(
            f"Disallowed layer dependencies detected:\n  - {details}",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    return check_dependency_matrix()


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
