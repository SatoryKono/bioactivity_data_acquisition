"""Data models for the pipeline inventory collector and cluster analyser."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(slots=True, frozen=True)
class InventoryRecord:
    """Snapshot of a single file included in the pipeline inventory."""

    source: str
    path: Path
    module: str
    size_kb: float
    loc: int
    mtime: datetime
    top_symbols: tuple[str, ...]
    imports_top: tuple[str, ...]
    docstring_first_line: str
    config_keys: tuple[str, ...]
    ngrams: frozenset[str]
    import_tokens: frozenset[str]
    file_extension: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "file_extension", self.path.suffix.lower())

    @property
    def is_python(self) -> bool:
        return self.file_extension == ".py"

    def to_csv_row(self) -> list[str]:
        return [
            self.source,
            str(self.path).replace("\\", "/"),
            self.module,
            f"{self.size_kb:.2f}",
            str(self.loc),
            self.mtime.isoformat(timespec="seconds"),
            ";".join(self.top_symbols),
            ";".join(self.imports_top),
            self.docstring_first_line,
            ";".join(self.config_keys),
        ]


@dataclass(slots=True)
class Cluster:
    """Group of related modules discovered by the clustering stage."""

    members: tuple[InventoryRecord, ...]
    common_ngrams: tuple[str, ...]
    common_imports: tuple[str, ...]
    average_jaccard: float
    average_import_overlap: float
    responsibility: str
    divergence_points: tuple[str, ...]

    def iter_paths(self) -> Iterable[Path]:
        for record in self.members:
            yield record.path

    def summary_lines(self) -> Sequence[str]:
        """Return human friendly description lines for documentation output."""
        header = f"Cluster size: {len(self.members)}"
        members = [f"- {record.path} (source: {record.source})" for record in self.members]
        ngrams = "common n-grams: " + (", ".join(self.common_ngrams) if self.common_ngrams else "—")
        imports = "common imports: " + (", ".join(self.common_imports) if self.common_imports else "—")
        scores = (
            f"avg jaccard={self.average_jaccard:.2f}; "
            f"avg import overlap={self.average_import_overlap:.2f}"
        )
        divergence = (
            "divergence points: "
            + (", ".join(self.divergence_points) if self.divergence_points else "—")
        )
        responsibility = f"responsibility: {self.responsibility}"
        return (header, responsibility, divergence, *members, ngrams, imports, scores)
