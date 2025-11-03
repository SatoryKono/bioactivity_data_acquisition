"""Static checks that verify required documentation artefacts."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class DocumentationRequirement:
    """Represents a documentation file and the markers it must contain."""

    path: Path
    markers: Sequence[str]


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

REQUIREMENTS: Sequence[DocumentationRequirement] = (
    DocumentationRequirement(
        path=REPOSITORY_ROOT / "docs" / "pipelines" / "PIPELINES.metrics.md",
        markers=(
            "# Pipeline Metrics Report",
            "## Code Footprint",
            "## Test Execution Time",
        ),
    ),
)


REF_LINK_PATTERN = re.compile(r"\[ref:\s*repo:(?P<path>[^@\]]+)@test_refactoring_32\]")


def _collect_errors(requirement: DocumentationRequirement) -> list[str]:
    """Validate a single documentation requirement and return human-friendly errors."""

    errors: list[str] = []
    path = requirement.path
    if not path.exists():
        errors.append(f"missing required documentation file: {path.relative_to(REPOSITORY_ROOT)}")
        return errors

    content = path.read_text(encoding="utf-8").strip()
    if not content:
        errors.append(
            f"documentation file is empty: {path.relative_to(REPOSITORY_ROOT)}",
        )
        return errors

    for marker in requirement.markers:
        if marker not in content:
            errors.append(
                "missing marker '{marker}' in {path}".format(
                    marker=marker,
                    path=path.relative_to(REPOSITORY_ROOT),
                ),
            )
    return errors


def _validate_doc_requirements(
    requirements: Iterable[DocumentationRequirement],
) -> list[str]:
    """Validate all requirements and return a flat list of error messages."""

    errors: list[str] = []
    for requirement in requirements:
        errors.extend(_collect_errors(requirement))
    return errors


def _collect_refactoring_link_errors() -> list[str]:
    """Validate that refactoring documents reference existing repository paths."""

    errors: list[str] = []
    refactoring_dir = REPOSITORY_ROOT / "docs" / "architecture" / "refactoring"
    if not refactoring_dir.exists():
        return errors

    for document_path in sorted(refactoring_dir.glob("*.md")):
        content = document_path.read_text(encoding="utf-8")
        for match in REF_LINK_PATTERN.finditer(content):
            referenced_path = match.group("path").strip()
            if not referenced_path:
                errors.append(
                    "empty repository reference in {path}".format(
                        path=document_path.relative_to(REPOSITORY_ROOT),
                    )
                )
                continue

            if any(token in referenced_path for token in ("*", "<", ">")):
                # Wildcards and template placeholders describe patterns rather than paths.
                continue

            candidate = Path(referenced_path)
            if candidate.is_absolute():
                errors.append(
                    "absolute repository reference '{ref}' in {path}".format(
                        ref=referenced_path,
                        path=document_path.relative_to(REPOSITORY_ROOT),
                    )
                )
                continue

            resolved = (REPOSITORY_ROOT / candidate).resolve()
            try:
                resolved.relative_to(REPOSITORY_ROOT)
            except ValueError:
                errors.append(
                    "repository reference '{ref}' escapes repository root in {path}".format(
                        ref=referenced_path,
                        path=document_path.relative_to(REPOSITORY_ROOT),
                    )
                )
                continue

            if not resolved.exists():
                errors.append(
                    "missing repository path '{ref}' referenced in {path}".format(
                        ref=referenced_path,
                        path=document_path.relative_to(REPOSITORY_ROOT),
                    )
                )

    return errors


def _validate() -> list[str]:
    """Run all documentation-related checks and return human-friendly errors."""

    errors: list[str] = []
    errors.extend(_validate_doc_requirements(REQUIREMENTS))
    errors.extend(_collect_refactoring_link_errors())
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for CLI invocation."""

    parser = argparse.ArgumentParser(
        description=(
            "Verify that mandatory documentation artefacts are present and include key sections."
        )
    )
    parser.parse_args(argv)

    errors = _validate()
    if errors:
        for message in errors:
            print(f"[ERROR] {message}")
        return 1

    print("All documentation checks passed.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
