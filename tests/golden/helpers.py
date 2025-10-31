"""Utilities for deterministic golden snapshot handling and comparison."""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any, Iterable

import yaml

from bioetl.core.output_writer import OutputArtifacts


__all__ = [
    "calculate_file_hash",
    "compare_files_bitwise",
    "compare_meta_yaml",
    "normalize_meta_yaml_for_comparison",
    "update_golden_file",
    "verify_bit_identical_outputs",
    "snapshot_artifacts",
]


def calculate_file_hash(path: Path, *, algorithm: str = "sha256") -> str:
    """Return the hexadecimal digest of ``path`` contents using ``algorithm``."""

    hasher = hashlib.new(algorithm)
    hasher.update(path.read_bytes())
    return hasher.hexdigest()


def compare_files_bitwise(path1: Path, path2: Path) -> tuple[bool, str | None]:
    """Compare two files byte-wise returning a success flag and diagnostic message."""

    left = path1.read_bytes()
    right = path2.read_bytes()

    if left == right:
        return True, None

    min_length = min(len(left), len(right))
    for index in range(min_length):
        if left[index] != right[index]:
            return False, (
                "Files differ at byte offset "
                f"{index}: 0x{left[index]:02x} != 0x{right[index]:02x}"
            )

    if len(left) != len(right):
        return False, f"File lengths differ: {len(left)} != {len(right)}"

    return False, "Files differ but no byte-level mismatch was detected"


def _normalise_metadata(mapping: dict[str, Any]) -> dict[str, Any]:
    normalised = dict(mapping)
    normalised.pop("extraction_timestamp", None)
    return normalised


def normalize_meta_yaml_for_comparison(path: Path) -> dict[str, Any]:
    """Return ``meta.yaml`` content without volatile timestamp fields."""

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):  # pragma: no cover - defensive guard
        raise TypeError(f"Unexpected metadata payload type: {type(payload)!r}")

    return _normalise_metadata(payload)


def compare_meta_yaml(path1: Path, path2: Path) -> tuple[bool, str | None]:
    """Compare metadata YAML files ignoring volatile timestamp fields."""

    meta1 = normalize_meta_yaml_for_comparison(path1)
    meta2 = normalize_meta_yaml_for_comparison(path2)

    if meta1 == meta2:
        return True, None

    missing_from_second = meta1.keys() - meta2.keys()
    missing_from_first = meta2.keys() - meta1.keys()
    differing_values = {
        key: (meta1[key], meta2[key])
        for key in meta1.keys() & meta2.keys()
        if meta1[key] != meta2[key]
    }

    diagnostics: list[str] = []
    if missing_from_second:
        diagnostics.append(f"Missing keys in second metadata: {sorted(missing_from_second)}")
    if missing_from_first:
        diagnostics.append(f"Missing keys in first metadata: {sorted(missing_from_first)}")
    if differing_values:
        diagnostics.append(f"Differing values: {differing_values}")

    message = "; ".join(diagnostics) if diagnostics else "Metadata mismatch"
    return False, message


def _iter_additional_paths(values: dict[str, Path | dict[str, Path]]) -> Iterable[tuple[str, Path]]:
    for name, value in values.items():
        if isinstance(value, dict):
            for fmt, path in value.items():
                yield f"{name}:{fmt}", path
        else:
            yield name, value


def snapshot_artifacts(artifacts: OutputArtifacts, target_dir: Path) -> OutputArtifacts:
    """Copy ``artifacts`` into ``target_dir`` and return a detached snapshot."""

    target_dir.mkdir(parents=True, exist_ok=True)

    def _copy(path: Path | None) -> Path | None:
        if path is None:
            return None
        destination = target_dir / path.name
        shutil.copy2(path, destination)
        return destination

    copied_additional: dict[str, Path | dict[str, Path]] = {}
    for name, value in artifacts.additional_datasets.items():
        if isinstance(value, dict):
            copied_additional[name] = {
                fmt: _copy(path) for fmt, path in value.items() if path is not None
            }
        else:
            copied = _copy(value)
            if copied is not None:
                copied_additional[name] = copied

    return OutputArtifacts(
        dataset=_copy(artifacts.dataset),
        quality_report=_copy(artifacts.quality_report),
        run_directory=target_dir,
        additional_datasets=copied_additional,
        correlation_report=_copy(artifacts.correlation_report),
        metadata=_copy(artifacts.metadata),
        manifest=_copy(artifacts.manifest),
        qc_summary=_copy(artifacts.qc_summary),
        qc_missing_mappings=_copy(artifacts.qc_missing_mappings),
        qc_enrichment_metrics=_copy(artifacts.qc_enrichment_metrics),
        qc_summary_statistics=_copy(artifacts.qc_summary_statistics),
        qc_dataset_metrics=_copy(artifacts.qc_dataset_metrics),
        debug_dataset=_copy(artifacts.debug_dataset),
        metadata_model=artifacts.metadata_model,
    )


def verify_bit_identical_outputs(
    artifacts1: OutputArtifacts,
    artifacts2: OutputArtifacts,
    *,
    ignore_meta_time: bool = True,
) -> tuple[bool, list[str]]:
    """Validate that two :class:`OutputArtifacts` instances are bit-identical."""

    errors: list[str] = []

    identical, diagnostic = compare_files_bitwise(artifacts1.dataset, artifacts2.dataset)
    if not identical:
        errors.append(f"Dataset mismatch: {diagnostic}")

    identical, diagnostic = compare_files_bitwise(
        artifacts1.quality_report, artifacts2.quality_report
    )
    if not identical:
        errors.append(f"Quality report mismatch: {diagnostic}")

    additional1 = sorted(_iter_additional_paths(artifacts1.additional_datasets))
    additional2 = sorted(_iter_additional_paths(artifacts2.additional_datasets))
    if [name for name, _ in additional1] != [name for name, _ in additional2]:
        errors.append("Additional dataset keys differ")
    else:
        for (name, path1), (_, path2) in zip(additional1, additional2):
            identical, diagnostic = compare_files_bitwise(path1, path2)
            if not identical:
                errors.append(f"Additional dataset '{name}' mismatch: {diagnostic}")

    if artifacts1.correlation_report and artifacts2.correlation_report:
        identical, diagnostic = compare_files_bitwise(
            artifacts1.correlation_report, artifacts2.correlation_report
        )
        if not identical:
            errors.append(f"Correlation report mismatch: {diagnostic}")
    elif bool(artifacts1.correlation_report) != bool(artifacts2.correlation_report):
        errors.append("Correlation report presence differs")

    for attribute in ("qc_summary_statistics", "qc_dataset_metrics", "qc_summary"):
        path1 = getattr(artifacts1, attribute)
        path2 = getattr(artifacts2, attribute)
        if path1 and path2:
            identical, diagnostic = compare_files_bitwise(path1, path2)
            if not identical:
                errors.append(f"{attribute} mismatch: {diagnostic}")
        elif bool(path1) != bool(path2):
            errors.append(f"{attribute} presence differs")

    if artifacts1.metadata and artifacts2.metadata:
        if ignore_meta_time:
            identical, diagnostic = compare_meta_yaml(artifacts1.metadata, artifacts2.metadata)
        else:
            identical, diagnostic = compare_files_bitwise(artifacts1.metadata, artifacts2.metadata)
        if not identical:
            errors.append(f"Metadata mismatch: {diagnostic}")
    elif bool(artifacts1.metadata) != bool(artifacts2.metadata):
        errors.append("Metadata presence differs")

    if artifacts1.manifest and artifacts2.manifest:
        identical, diagnostic = compare_files_bitwise(artifacts1.manifest, artifacts2.manifest)
        if not identical:
            errors.append(f"Manifest mismatch: {diagnostic}")
    elif bool(artifacts1.manifest) != bool(artifacts2.manifest):
        errors.append("Manifest presence differs")

    if artifacts1.debug_dataset and artifacts2.debug_dataset:
        identical, diagnostic = compare_files_bitwise(
            artifacts1.debug_dataset, artifacts2.debug_dataset
        )
        if not identical:
            errors.append(f"Debug dataset mismatch: {diagnostic}")
    elif bool(artifacts1.debug_dataset) != bool(artifacts2.debug_dataset):
        errors.append("Debug dataset presence differs")

    return not errors, errors


def update_golden_file(actual_path: Path, golden_path: Path, *, force: bool = False) -> bool:
    """Copy ``actual_path`` to ``golden_path`` if content differs."""

    if golden_path.exists() and not force:
        identical, _ = compare_files_bitwise(actual_path, golden_path)
        if identical:
            return False

    golden_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(actual_path, golden_path)

    hash_path = golden_path.with_suffix(golden_path.suffix + ".sha256")
    digest = calculate_file_hash(golden_path)
    hash_path.write_text(f"{digest}  {golden_path.name}\n", encoding="utf-8")
    return True
