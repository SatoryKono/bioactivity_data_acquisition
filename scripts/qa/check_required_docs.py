"""Static checks that verify required documentation artefacts."""

from __future__ import annotations

import argparse
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
        path=REPOSITORY_ROOT / "docs" / "requirements" / "PIPELINES.metrics.md",
        markers=(
            "# Pipeline Metrics Baseline",
            "## Baseline KPIs",
            "## Runtime Snapshot",
        ),
    ),
)


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


def _validate(requirements: Iterable[DocumentationRequirement]) -> list[str]:
    """Validate all requirements and return a flat list of error messages."""

    errors: list[str] = []
    for requirement in requirements:
        errors.extend(_collect_errors(requirement))
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for CLI invocation."""

    parser = argparse.ArgumentParser(
        description=(
            "Verify that mandatory documentation artefacts are present and include key sections."
        )
    )
    parser.parse_args(argv)

    errors = _validate(REQUIREMENTS)
    if errors:
        for message in errors:
            print(f"[ERROR] {message}")
        return 1

    print("All required documentation files are present.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
