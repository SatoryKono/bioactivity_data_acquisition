from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
DOMAIN_ROOT = REPO_ROOT / "src" / "bioetl" / "domain"
FORBIDDEN_PREFIXES = ("bioetl.infrastructure", "bioetl.clients")


@dataclass
class Violation:
    file: Path
    lineno: int
    target: str


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if path.name == "__init__.py" and path.is_file():
            yield path
        elif path.is_file():
            yield path


def module_name_from_path(path: Path, root: Path) -> str:
    relative = path.relative_to(root.parent)
    parts = list(relative.with_suffix("").parts)
    return ".".join(parts)


def resolve_absolute_module(
    module: str | None, level: int, current_module: str
) -> str | None:
    if level == 0:
        return module

    segments = current_module.split(".")
    if level > len(segments):
        return module

    base_parts = segments[: len(segments) - level]
    if module:
        base_parts.extend(module.split("."))
    return ".".join(part for part in base_parts if part)


def check_file(path: Path) -> list[Violation]:
    current_module = module_name_from_path(path, REPO_ROOT / "src")
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    violations: list[Violation] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                target = alias.name
                if target.startswith(FORBIDDEN_PREFIXES):
                    violations.append(
                        Violation(file=path, lineno=node.lineno, target=target)
                    )
        elif isinstance(node, ast.ImportFrom):
            target_module = resolve_absolute_module(
                node.module, node.level, current_module
            )
            if target_module and target_module.startswith(FORBIDDEN_PREFIXES):
                violations.append(
                    Violation(
                        file=path,
                        lineno=node.lineno,
                        target=target_module,
                    )
                )
    return violations


def main() -> int:
    if not DOMAIN_ROOT.exists():
        print("Domain layer not found; skipping import lint.")
        return 0

    all_violations: list[Violation] = []
    for path in iter_python_files(DOMAIN_ROOT):
        all_violations.extend(check_file(path))

    if all_violations:
        print("Forbidden cross-layer imports detected:\n")
        for violation in sorted(
            all_violations, key=lambda item: (item.file, item.lineno, item.target)
        ):
            rel_path = violation.file.relative_to(REPO_ROOT)
            print(f"{rel_path}:{violation.lineno}: {violation.target}")
        return 1

    print("Import lint passed: domain layer is clean.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
